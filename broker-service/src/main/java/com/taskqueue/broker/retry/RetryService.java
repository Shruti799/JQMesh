
package com.taskqueue.broker.retry;

import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.model.TaskStatus;
import com.taskqueue.broker.queue.TaskQueue;
import com.taskqueue.broker.storage.redis.LuaScriptProvider;
import com.taskqueue.broker.storage.redis.RedisKeys;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Service
public class RetryService{

    private final RetryPolicy retryPolicy;
    private final RedisTemplate<String, String> stringRedisTemplate;
    private final RedisTemplate<String, Task> taskRedisTemplate;
    private final LuaScriptProvider luaScriptProvider;

    public RetryService(
            RetryPolicy retryPolicy,
            RedisTemplate<String, String> stringRedisTemplate,
            RedisTemplate<String, Task> taskRedisTemplate,
            LuaScriptProvider luaScriptProvider){

        this.retryPolicy = retryPolicy;
        this.stringRedisTemplate = stringRedisTemplate;
        this.taskRedisTemplate = taskRedisTemplate;
        this.luaScriptProvider = luaScriptProvider;
    }

    public boolean retry(Task task){

        String taskKey = RedisKeys.task(task.getTaskId());

        // Handling exhausted retries.
        if(task.getRetryCount() >= task.getMaxRetries()){
            task.setStatus(TaskStatus.DEAD_LETTER);
            task.setUpdatedAt(Instant.now());

            taskRedisTemplate.opsForValue().set(taskKey, task);

            // DLQ movement will be implemented separately.
            return false;
        }

        // Calculate retry delay using the next retry count.
        task.setRetryCount(task.getRetryCount() + 1);
        
        Duration delay = retryPolicy.computeDelay(task);
        Instant nextRetryAt = Instant.now().plus(delay);
        
        // Atomically move the task from processing to retry-pending.
        Long moved = stringRedisTemplate.execute(
            luaScriptProvider.getRetryTaskScript(),
            List.of(
                RedisKeys.processingQueue(task.getQueueName()),
                RedisKeys.retryPendingQueue()
            ),
            task.getTaskId().toString(),
            String.valueOf(nextRetryAt.toEpochMilli())
        );
        
        if (moved == null || moved != 1L){
            // Restore retry count because the retry operation failed.
            task.setRetryCount(task.getRetryCount() - 1);
            return false;
        }
        
        // Update task metadata only after successful queue movement.
        task.setNextRetryAt(nextRetryAt);
        task.setStatus(TaskStatus.RETRY_PENDING);
        task.setWorkerId(null);
        task.setLeaseId(null);
        task.setLeasedUntil(null);
        task.setUpdatedAt(Instant.now());
        
        // Persist updated task metadata.
        taskRedisTemplate.opsForValue().set(taskKey, task);
        
        return true;
    }
}
