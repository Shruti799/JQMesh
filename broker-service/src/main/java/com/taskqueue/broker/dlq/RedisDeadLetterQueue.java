
package com.taskqueue.broker.dlq;

import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.model.TaskStatus;
import com.taskqueue.broker.storage.redis.RedisKeys;
import com.taskqueue.broker.storage.redis.LuaScriptProvider;
import com.taskqueue.broker.storage.ScoreCalculator;

import lombok.RequiredArgsConstructor;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class RedisDeadLetterQueue implements DeadLetterQueue{

    private final RedisTemplate<String, String> stringRedisTemplate;
    private final RedisTemplate<String, Task> taskRedisTemplate;
    private final LuaScriptProvider luaScriptProvider;

    @Override
    public boolean moveToDeadLetter(Task task){

        Long result = stringRedisTemplate.execute(
                luaScriptProvider.getDeadLetterTaskScript(),
                List.of(
                        RedisKeys.processingQueue(task.getQueueName()),
                        RedisKeys.deadLetterQueue(task.getQueueName())
                ),
                task.getTaskId().toString(),
                String.valueOf(Instant.now().toEpochMilli())
        );

        if(result == null || result != 1L){
            return false;
        }

        task.setStatus(TaskStatus.DEAD_LETTER);
        task.setWorkerId(null);
        task.setLeaseId(null);
        task.setLeasedUntil(null);
        task.setUpdatedAt(Instant.now());

        taskRedisTemplate.opsForValue().set(
            RedisKeys.task(task.getTaskId()), task
        );

        return true;
    }

    @Override
    public Optional<Task> findById(UUID taskId, String queueName){

        Double score = stringRedisTemplate.opsForZSet().score(
                RedisKeys.deadLetterQueue(queueName),
                taskId.toString()
        );

        if(score == null){
            return Optional.empty();
        }

        Task task = taskRedisTemplate.opsForValue().get(RedisKeys.task(taskId));

        return Optional.ofNullable(task);
    }

    @Override
    public List<Task> findAll(String queueName){

        var taskIds = stringRedisTemplate.opsForZSet().range(
            RedisKeys.deadLetterQueue(queueName), 0, -1
        );

        List<Task> tasks = new ArrayList<>();

        if(taskIds == null || taskIds.isEmpty()){
            return tasks;
        }

        for(String taskId : taskIds){

            Task task = taskRedisTemplate.opsForValue().get(
                    RedisKeys.task(UUID.fromString(taskId))
            );

            if(task != null){
                tasks.add(task);
            }
        }

        return tasks;
    }

    @Override
    public boolean delete(UUID taskId, String queueName){

        Long removed = stringRedisTemplate.execute(
                luaScriptProvider.getRemoveDeadLetterTaskScript(),
                List.of(RedisKeys.deadLetterQueue(queueName)),
                taskId.toString()
        );

        if(removed == null || removed != 1L){
            return false;
        }

        taskRedisTemplate.delete(RedisKeys.task(taskId));

        return true;
    }

    @Override
    public boolean redrive(UUID taskId, String queueName){

        Optional<Task> optionalTask = findById(taskId, queueName);

        if(optionalTask.isEmpty()){
            return false;
        }

        Task task = optionalTask.get();

        if(task.getStatus() != TaskStatus.DEAD_LETTER){
            return false;
        }

        task.setRetryCount(0);
        task.setFailureReason(null);
        task.setNextRetryAt(null);
        task.setWorkerId(null);
        task.setLeaseId(null);
        task.setLeasedUntil(null);
        task.setStatus(TaskStatus.QUEUED);
        task.setUpdatedAt(Instant.now());

        double priorityScore = ScoreCalculator.calculate(task);

        Long result = stringRedisTemplate.execute(
                luaScriptProvider.getRedriveTaskScript(),
                List.of(
                    RedisKeys.deadLetterQueue(queueName),
                    RedisKeys.queue(queueName)
                ),
                taskId.toString(),
                String.valueOf(priorityScore)
        );

        if(result == null || result != 1L){
            return false;
        }

        task.setComputedScore(priorityScore);

        taskRedisTemplate.opsForValue().set(RedisKeys.task(taskId), task);

        return true;
    }
}
