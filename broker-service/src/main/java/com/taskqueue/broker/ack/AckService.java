package com.taskqueue.broker.ack;

import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.model.TaskStatus;
import com.taskqueue.broker.storage.redis.LuaScriptProvider;
import com.taskqueue.broker.storage.redis.RedisKeys;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Collections;
import java.util.UUID;

@Service
public class AckService{

    private final RedisTemplate<String, Task> taskRedisTemplate;
    private final RedisTemplate<String, String> stringRedisTemplate;
    private final LuaScriptProvider luaScriptProvider;


    public AckService(RedisTemplate<String, Task> taskRedisTemplate, RedisTemplate<String, String> stringRedisTemplate, LuaScriptProvider luaScriptProvider){

        this.taskRedisTemplate = taskRedisTemplate;
        this.stringRedisTemplate = stringRedisTemplate;
        this.luaScriptProvider = luaScriptProvider;
    }

    public boolean acknowledge(UUID taskId, UUID leaseId){

        // Loading the task from Redis
        Task task = taskRedisTemplate.opsForValue().get("task:" + taskId);

        if(task == null){
            return false;
        }

        // Task must currently be in progress
        if(task.getStatus() != TaskStatus.IN_PROGRESS){
            return false;
        }

        // Lease ID must match the current lease
        if(task.getLeaseId() == null || !task.getLeaseId().equals(leaseId)){
            return false;
        }

        String processingQueueKey = RedisKeys.processingQueue(task.getQueueName());

        // Executing ACK Lua script
        DefaultRedisScript<Long> ackScript = luaScriptProvider.getAckTaskScript();

        Long result = stringRedisTemplate.execute(ackScript, Collections.singletonList(processingQueueKey),taskId.toString());

        // Lua script failed to remove the task
        if(result == null || result != 1L){
           return false;
        }

        Instant now = Instant.now();

        // Marking task as completed
        task.setStatus(TaskStatus.COMPLETED);
        task.setCompletedAt(now);
        task.setUpdatedAt(now);

        // Clearing lease information
        task.setWorkerId(null);
        task.setLeaseId(null);
        task.setLeasedUntil(null);

        // Persisting updated task
        taskRedisTemplate.opsForValue().set("task:" + taskId, task);

        return true;
    }
}
