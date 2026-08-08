package com.taskqueue.broker.queue;

import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.model.TaskStatus;
import com.taskqueue.broker.storage.ScoreCalculator;
import com.taskqueue.broker.storage.redis.RedisKeys;
import com.taskqueue.broker.storage.redis.LuaScriptProvider;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.List;
import java.util.UUID;

@Component
public class RedisPriorityQueue implements TaskQueue{
    private final RedisTemplate<String, String> stringRedisTemplate;

    private final RedisTemplate<String, Task> taskRedisTemplate;

    private final LuaScriptProvider luaScriptProvider;

    public RedisPriorityQueue(RedisTemplate<String, String> stringRedisTemplate, RedisTemplate<String, Task> taskRedisTemplate, LuaScriptProvider luaScriptProvider){
        this.stringRedisTemplate = stringRedisTemplate;
        this.taskRedisTemplate = taskRedisTemplate;
        this.luaScriptProvider = luaScriptProvider;
    }

    private String getQueueKey(String queueName){
        return RedisKeys.queue(queueName);
    }

    private String getTaskKey(UUID taskId){
        return RedisKeys.task(taskId);
    }

    private void saveTask(Task task){
        taskRedisTemplate.opsForValue().set(getTaskKey(task.getTaskId()), task);
    }

    private Optional<Task> getTask(UUID taskId){
        Task task = taskRedisTemplate.opsForValue().get(getTaskKey(taskId));
        return Optional.ofNullable(task);
    }

    private Optional<Task> getTask(String taskId){
        return getTask(UUID.fromString(taskId));
    }

    private void deleteTask(UUID taskId){
        taskRedisTemplate.delete(getTaskKey(taskId));
    }

    @Override
    public void enqueue(Task task){

        // Calculating the score used for Redis Sorted Set ordering
        double score = ScoreCalculator.calculate(task);
        task.setComputedScore(score);
    
        // Initializing task state
        task.setStatus(TaskStatus.QUEUED);
        task.setUpdatedAt(Instant.now());
    
        // Persisting the complete task
        saveTask(task);
    
        // Adding only the taskId to the Redis Sorted Set
        stringRedisTemplate.opsForZSet().add(getQueueKey(task.getQueueName()), task.getTaskId().toString(),score);
    }

    @Override
    public Optional<Task> claimNext(String queueName, String workerId, Duration leaseDuration){
    
        Instant now = Instant.now();
        Instant leaseExpiry = now.plus(leaseDuration);
    
        String taskId = stringRedisTemplate.execute(luaScriptProvider.getClaimTaskScript(),
            List.of(getQueueKey(queueName), RedisKeys.processingQueue(queueName)),
            String.valueOf(leaseExpiry.toEpochMilli())
        );
    
        if (taskId == null){
            return Optional.empty();
        }
    
        Optional<Task> optionalTask = getTask(taskId);
    
        if(optionalTask.isEmpty()){
           return Optional.empty();
        }

        Task task = optionalTask.get();
    
        if(task.getStartedAt() == null){
            task.setStartedAt(now);
        }
    
        task.setStatus(TaskStatus.IN_PROGRESS);
    
        task.setWorkerId(workerId);
    
        task.setLeaseId(UUID.randomUUID());
    
        task.setLeasedUntil(leaseExpiry);
    
        task.setDeliveryCount(task.getDeliveryCount() + 1);

        task.setUpdatedAt(now);

        saveTask(task);

        return Optional.of(task);
    }

    @Override
    public boolean cancel(UUID taskId){
    
        Optional<Task> optionalTask = getTask(taskId);
    
        if(optionalTask.isEmpty()){
            return false;
        }
    
        Task task = optionalTask.get();
    
        if(task.getStatus() == TaskStatus.IN_PROGRESS){
            return false;
        }
    
        task.setStatus(TaskStatus.CANCELLED);
        task.setUpdatedAt(Instant.now());
    
        saveTask(task);
    
        stringRedisTemplate.opsForZSet().remove(
            getQueueKey(task.getQueueName()),
            taskId.toString()
        );
    
        return true;
    }

    @Override
    public long size(String queueName){
    
        Long size = stringRedisTemplate
            .opsForZSet()
            .zCard(getQueueKey(queueName));
    
        return size == null ? 0 : size;
    }

    @Override
    public boolean isEmpty(String queueName){
        return size(queueName) == 0;
    }

    @Override
    public void requeue(Task task){
    
        task.setStatus(TaskStatus.QUEUED);
    
        task.setWorkerId(null);
    
        task.setLeaseId(null);
    
        task.setLeasedUntil(null);
    
        task.setUpdatedAt(Instant.now());
    
        double score = ScoreCalculator.calculate(task);
    
        task.setComputedScore(score);
    
        saveTask(task);

        // Remove from processing queue
        stringRedisTemplate.opsForZSet().remove(
            RedisKeys.processingQueue(task.getQueueName()),
            task.getTaskId().toString()
        );

        // Add to the main queue with updated score
        stringRedisTemplate.opsForZSet().add(
            getQueueKey(task.getQueueName()),
            task.getTaskId().toString(), score
        );
    }
}
