package com.taskqueue.broker.queue;

import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.model.TaskStatus;
import com.taskqueue.broker.storage.ScoreCalculator;
import com.taskqueue.broker.storage.redis.RedisKeys;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Component
public class RedisPriorityQueue implements TaskQueue{
    private final RedisTemplate<String, String> stringRedisTemplate;

    private final RedisTemplate<String, Task> taskRedisTemplate;

    public RedisPriorityQueue(RedisTemplate<String, String> stringRedisTemplate, RedisTemplate<String, Task> taskRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
        this.taskRedisTemplate = taskRedisTemplate;
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

    private void deleteTask(UUID taskId){
        taskRedisTemplate.delete(getTaskKey(taskId));
    }
}
