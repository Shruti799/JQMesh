
package com.taskqueue.broker.dlq;

import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.model.TaskPriority;
import com.taskqueue.broker.model.TaskStatus;
import com.taskqueue.broker.model.TaskType;
import com.taskqueue.broker.config.RedisConfig;
import com.taskqueue.broker.storage.redis.RedisKeys;
import com.taskqueue.broker.storage.redis.LuaScriptProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringJUnitConfig(DeadLetterQueueTest.TestConfig.class)
class DeadLetterQueueTest{

    @TestConfiguration
    @Import({
            RedisAutoConfiguration.class,
            RedisConfig.class,
            LuaScriptProvider.class
    })
    static class TestConfig{

        @Bean
        RedisDeadLetterQueue redisDeadLetterQueue(
                RedisTemplate<String, String> stringRedisTemplate,
                RedisTemplate<String, Task> taskRedisTemplate,
                LuaScriptProvider luaScriptProvider){

            return new RedisDeadLetterQueue(
                stringRedisTemplate,
                taskRedisTemplate,
                luaScriptProvider
            );
        }
    }

    @Autowired
    private RedisDeadLetterQueue deadLetterQueue;

    @Autowired
    private RedisTemplate<String, String> stringRedisTemplate;

    @Autowired
    private RedisTemplate<String, Task> taskRedisTemplate;

    private Task task;
    private String queueName;

    @BeforeEach
    void setUp(){

        stringRedisTemplate.getConnectionFactory().getConnection().serverCommands().flushDb();

        queueName = "test-queue";

        task = Task.builder()
                .taskId(UUID.randomUUID())
                .queueName(queueName)
                .taskType(TaskType.values()[0])
                .payload("{\"message\":\"test\"}")
                .priority(TaskPriority.values()[0])
                .status(TaskStatus.IN_PROGRESS)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .retryCount(3)
                .maxRetries(3)
                .deliveryCount(4)
                .workerId("worker-1")
                .leaseId(UUID.randomUUID())
                .leasedUntil(Instant.now().plusSeconds(30))
                .build();

        taskRedisTemplate.opsForValue().set(RedisKeys.task(task.getTaskId()), task);
    }

    @Test
    void shouldMoveTaskToDeadLetterQueue(){

        stringRedisTemplate.opsForZSet().add(
            RedisKeys.processingQueue(queueName),
            task.getTaskId().toString(),
            Instant.now().toEpochMilli()
        );

        boolean result = deadLetterQueue.moveToDeadLetter(task);

        assertTrue(result);

        assertEquals(
            0L,
            stringRedisTemplate.opsForZSet().zCard(RedisKeys.processingQueue(queueName))
        );

        assertEquals(
            1L,
            stringRedisTemplate.opsForZSet().zCard(RedisKeys.deadLetterQueue(queueName))
        );

        Task storedTask = taskRedisTemplate.opsForValue().get(RedisKeys.task(task.getTaskId()));

        assertNotNull(storedTask);
        assertEquals(TaskStatus.DEAD_LETTER, storedTask.getStatus());
        assertNull(storedTask.getWorkerId());
        assertNull(storedTask.getLeaseId());
        assertNull(storedTask.getLeasedUntil());
    }

    @Test
    void shouldNotMoveTaskWhenNotInProcessingQueue(){

        boolean result = deadLetterQueue.moveToDeadLetter(task);

        assertFalse(result);

        assertEquals(
            0L,
            stringRedisTemplate.opsForZSet().zCard(RedisKeys.deadLetterQueue(queueName))
        );
    }

    @Test
    void shouldFindDeadLetterTaskById(){

        moveTaskToDlq();

        var result = deadLetterQueue.findById(task.getTaskId(), queueName);

        assertTrue(result.isPresent());
        assertEquals(task.getTaskId(), result.get().getTaskId());
        assertEquals(TaskStatus.DEAD_LETTER, result.get().getStatus());
    }

    @Test
    void shouldReturnEmptyWhenDeadLetterTaskDoesNotExist(){

        var result = deadLetterQueue.findById(UUID.randomUUID(), queueName);

        assertTrue(result.isEmpty());
    }

    @Test
    void shouldFindAllDeadLetterTasks(){

        moveTaskToDlq();

        Task secondTask = createSecondTask();

        taskRedisTemplate.opsForValue().set(RedisKeys.task(secondTask.getTaskId()), secondTask);

        stringRedisTemplate.opsForZSet().add(
            RedisKeys.deadLetterQueue(queueName),
            secondTask.getTaskId().toString(),
            Instant.now().plusSeconds(1).toEpochMilli()
        );

        var result = deadLetterQueue.findAll(queueName);

        assertEquals(2, result.size());
    }

    @Test
    void shouldRedriveDeadLetterTask(){

        moveTaskToDlq();

        boolean result = deadLetterQueue.redrive(task.getTaskId(), queueName);

        assertTrue(result);

        assertEquals(
            0L,
            stringRedisTemplate.opsForZSet().zCard(RedisKeys.deadLetterQueue(queueName))
        );

        assertEquals(
            1L,
            stringRedisTemplate.opsForZSet().zCard(RedisKeys.queue(queueName))
        );

        Task storedTask = taskRedisTemplate.opsForValue().get(
            RedisKeys.task(task.getTaskId())
        );

        assertEquals(TaskStatus.QUEUED, storedTask.getStatus());
        assertEquals(0, storedTask.getRetryCount());
        assertNull(storedTask.getFailureReason());
        assertNull(storedTask.getNextRetryAt());
        assertNotNull(storedTask.getComputedScore());
    }

    @Test
    void shouldNotRedriveTaskNotInDeadLetterQueue(){

        boolean result = deadLetterQueue.redrive(task.getTaskId(), queueName);

        assertFalse(result);
    }

    @Test
    void shouldDeleteDeadLetterTask(){

        moveTaskToDlq();

        boolean result = deadLetterQueue.delete(task.getTaskId(), queueName);

        assertTrue(result);

        assertNull(
                stringRedisTemplate.opsForZSet().score(
                    RedisKeys.deadLetterQueue(queueName),
                    task.getTaskId().toString()
                )
        );

        assertNull(
                taskRedisTemplate.opsForValue().get(
                    RedisKeys.task(task.getTaskId())
                )
        );
    }

    @Test
    void shouldNotDeleteTaskNotInDeadLetterQueue(){

        boolean result = deadLetterQueue.delete(task.getTaskId(), queueName);

        assertFalse(result);

        assertNotNull(
            taskRedisTemplate.opsForValue().get(RedisKeys.task(task.getTaskId()))
        );
    }

    private void moveTaskToDlq(){

        stringRedisTemplate.opsForZSet().add(
                RedisKeys.processingQueue(queueName),
                task.getTaskId().toString(),
                Instant.now().toEpochMilli()
        );

        assertTrue(deadLetterQueue.moveToDeadLetter(task));
    }

    private Task createSecondTask(){

        return Task.builder()
            .taskId(UUID.randomUUID())
            .queueName(queueName)
            .taskType(TaskType.values()[0])
            .payload("{\"message\":\"second task\"}")
            .priority(TaskPriority.values()[0])
            .status(TaskStatus.DEAD_LETTER)
            .createdAt(Instant.now())
            .updatedAt(Instant.now())
            .retryCount(3)
            .maxRetries(3)
            .build();
    }
}
