package com.taskqueue.broker.scheduler;

import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.retry.RetryService;
import com.taskqueue.broker.storage.redis.RedisKeys;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.taskqueue.broker.queue.TaskQueue;

import com.taskqueue.broker.model.TaskStatus;
import com.taskqueue.broker.queue.TaskQueue;

import java.time.Instant;
import java.util.Collections;
import java.util.Set;

@Component
public class RetryScheduler{

    private final RedisTemplate<String, String> stringRedisTemplate;
    private final RedisTemplate<String, Task> taskRedisTemplate;
    private final RetryService retryService;
    private final TaskQueue taskQueue;

    public RetryScheduler(RedisTemplate<String, String> stringRedisTemplate, RedisTemplate<String, Task> taskRedisTemplate, RetryService retryService, TaskQueue taskQueue){
        this.stringRedisTemplate = stringRedisTemplate;
        this.taskRedisTemplate = taskRedisTemplate;
        this.retryService = retryService;
        this.taskQueue = taskQueue;
    }

    @Scheduled(fixedDelay = 5000)
    public void processExpiredTasks(){

        Set<String> queueNames = stringRedisTemplate.opsForSet().members(RedisKeys.queueRegistry());

        if(queueNames == null || queueNames.isEmpty()){
            return;
        }

        long now = Instant.now().toEpochMilli();

        for(String queueName : queueNames){

            String processingQueueKey = RedisKeys.processingQueue(queueName);

            Set<String> expiredTaskIds = stringRedisTemplate.opsForZSet().rangeByScore(processingQueueKey,0,now);

            if(expiredTaskIds == null || expiredTaskIds.isEmpty()){
                continue;
            }

            for(String taskId : expiredTaskIds){
    
                Task task = taskRedisTemplate.opsForValue().get("task:" + taskId);

                if(task == null){
                    continue;
                }
                retryService.retry(task);

                taskRedisTemplate.opsForValue().set("task:" + taskId, task);

                stringRedisTemplate.opsForZSet().remove(processingQueueKey, taskId);
            }
        }
        
        processRetryPendingTasks(
            Instant.now().toEpochMilli()
        );
    }

    private void processRetryPendingTasks(long now){

        Set<String> retryTaskIds = stringRedisTemplate.opsForZSet().rangeByScore(RedisKeys.retryPendingQueue(),0,now);
    
        if(retryTaskIds == null || retryTaskIds.isEmpty()){
            return;
        }
    
        for(String taskId : retryTaskIds){
    
            Task task = taskRedisTemplate.opsForValue().get("task:" + taskId);
    
            if(task == null){
                // Task no longer exists, so removing stale entry
                stringRedisTemplate.opsForZSet().remove(RedisKeys.retryPendingQueue(),taskId);
    
                continue;
            }
    
            if(task.getStatus() != TaskStatus.RETRY_PENDING){

                // Task is no longer waiting for retry
                stringRedisTemplate.opsForZSet().remove(RedisKeys.retryPendingQueue(),taskId);
                continue;
            }
    
            // Moving the task back to the ready queue
            taskQueue.requeue(task);
    
            // Removing it from the retry-pending index
            stringRedisTemplate.opsForZSet().remove(RedisKeys.retryPendingQueue(),taskId);
        }
    }
}

