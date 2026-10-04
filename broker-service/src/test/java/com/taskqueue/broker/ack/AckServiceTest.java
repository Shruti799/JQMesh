
package com.taskqueue.broker.ack;

import com.taskqueue.broker.config.RedisConfig;
import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.model.TaskStatus;
import com.taskqueue.broker.storage.redis.LuaScriptProvider;
import com.taskqueue.broker.storage.redis.RedisKeys;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;


import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringJUnitConfig(AckServiceTest.TestConfig.class)


class AckServiceTest{

    @Autowired
    private AckService ackService;

    @Autowired
    private RedisTemplate<String, Task> taskRedisTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @BeforeEach
    void cleanRedis(){
        taskRedisTemplate
                .getConnectionFactory()
                .getConnection()
                .serverCommands()
                .flushDb();
    }

    @Test
    void acknowledge_shouldCompleteTask(){

        UUID taskId = UUID.randomUUID();
        UUID leaseId = UUID.randomUUID();

        Task task = createInProgressTask(taskId, leaseId);

        String taskKey = "task:" + taskId;

        String processingQueueKey = RedisKeys.processingQueue(task.getQueueName());

        // Storing task
        taskRedisTemplate.opsForValue().set(taskKey, task);

        // Adding task to processing queue
        stringRedisTemplate.opsForZSet().add(
                processingQueueKey,
                taskId.toString(),
                Instant.now().plusSeconds(30).toEpochMilli()
        );

        // Act
        boolean result = ackService.acknowledge(taskId, leaseId);

        // Assert
        assertTrue(result);

        Task updatedTask = taskRedisTemplate.opsForValue().get(taskKey);

        assertNotNull(updatedTask);

        assertEquals(TaskStatus.COMPLETED, updatedTask.getStatus());

        assertNotNull(updatedTask.getCompletedAt());

        assertNull(updatedTask.getWorkerId());
        assertNull(updatedTask.getLeaseId());
        assertNull(updatedTask.getLeasedUntil());

        Double processingScore =
                stringRedisTemplate.opsForZSet().score(
                        processingQueueKey,
                        taskId.toString()
                );

        assertNull(processingScore);
    }

    @Test
    void acknowledge_shouldRejectWrongLeaseId() {

        UUID taskId = UUID.randomUUID();

        UUID actualLeaseId = UUID.randomUUID();
        UUID wrongLeaseId = UUID.randomUUID();

        Task task = createInProgressTask(taskId, actualLeaseId);

        taskRedisTemplate.opsForValue().set("task:" + taskId, task);

        // Act
        boolean result = ackService.acknowledge(taskId, wrongLeaseId);

        // Assert
        assertFalse(result);

        Task unchangedTask =
                taskRedisTemplate.opsForValue().get(
                        "task:" + taskId
                );

        assertNotNull(unchangedTask);

        assertEquals(TaskStatus.IN_PROGRESS, unchangedTask.getStatus());
    }

    @Test
    void acknowledge_shouldRejectNonInProgressTask() {

        UUID taskId = UUID.randomUUID();
        UUID leaseId = UUID.randomUUID();

        Task task = createInProgressTask(taskId, leaseId);

        task.setStatus(TaskStatus.COMPLETED);

        taskRedisTemplate.opsForValue().set(
                "task:" + taskId,
                task
        );

        // Act
        boolean result = ackService.acknowledge(taskId, leaseId);

        // Assert
        assertFalse(result);
    }

    @Test
    void acknowledge_shouldRejectMissingTask() {

        UUID taskId = UUID.randomUUID();
        UUID leaseId = UUID.randomUUID();

        // Act
        boolean result = ackService.acknowledge(taskId, leaseId);

        // Assert
        assertFalse(result);
    }

    private Task createInProgressTask(UUID taskId, UUID leaseId){

        Task task = new Task();

        task.setTaskId(taskId);
        task.setQueueName("test-queue");
        task.setStatus(TaskStatus.IN_PROGRESS);

        task.setWorkerId("worker-1");
        task.setLeaseId(leaseId);

        task.setCreatedAt(Instant.now());
        task.setStartedAt(Instant.now());

        task.setLeasedUntil(Instant.now().plus(Duration.ofSeconds(30)));

        return task;
    }

    @TestConfiguration
    @Import({
        RedisAutoConfiguration.class,
        RedisConfig.class,
        AckService.class,
        LuaScriptProvider.class
    })
    static class TestConfig {
    }
}

