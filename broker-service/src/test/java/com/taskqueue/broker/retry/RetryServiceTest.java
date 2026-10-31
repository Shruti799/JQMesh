
package com.taskqueue.broker.retry;

import com.taskqueue.broker.model.*;
import com.taskqueue.broker.storage.redis.LuaScriptProvider;
import com.taskqueue.broker.config.RedisConfig;
import com.taskqueue.broker.storage.redis.RedisKeys;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringJUnitConfig(RetryServiceTest.TestConfig.class)
class RetryServiceTest{

    @TestConfiguration
    @Import({
        RedisAutoConfiguration.class,
        RedisConfig.class,
        LuaScriptProvider.class
    })
    static class TestConfig{

        @Bean
        RetryPolicy retryPolicy(){
            return task -> Duration.ofSeconds(30);
        }

        @Bean
        RetryService retryService(
                RetryPolicy retryPolicy,
                RedisTemplate<String, String> stringRedisTemplate,
                RedisTemplate<String, Task> taskRedisTemplate,
                LuaScriptProvider luaScriptProvider){

            return new RetryService(
                retryPolicy,
                stringRedisTemplate,
                taskRedisTemplate,
                luaScriptProvider
            );
        }
    }

    @org.springframework.beans.factory.annotation.Autowired
    private RetryService retryService;

    @org.springframework.beans.factory.annotation.Autowired
    private RedisTemplate<String, String> stringRedisTemplate;

    @org.springframework.beans.factory.annotation.Autowired
    private RedisTemplate<String, Task> taskRedisTemplate;

    private Task task;

    @BeforeEach
    void setUp(){

        stringRedisTemplate.getConnectionFactory()
            .getConnection().serverCommands().flushDb();

        task = Task.builder()
            .taskId(UUID.randomUUID())
            .queueName("test-queue")
            .taskType(TaskType.values()[0])
            .payload("{\"message\":\"test\"}")
            .priority(TaskPriority.values()[0])
            .status(TaskStatus.IN_PROGRESS)
            .createdAt(Instant.now())
            .startedAt(Instant.now())
            .updatedAt(Instant.now())
            .leasedUntil(Instant.now().plusSeconds(60))
            .leaseId(UUID.randomUUID())
            .workerId("worker-1")
            .retryCount(0)
            .deliveryCount(1)
            .maxRetries(3)
            .build();

        taskRedisTemplate.opsForValue().set(
            RedisKeys.task(task.getTaskId()),
            task
        );

        stringRedisTemplate.opsForZSet().add(
            RedisKeys.processingQueue(task.getQueueName()),
            task.getTaskId().toString(),
            Instant.now().minusSeconds(10).toEpochMilli()
        );
    }

    @Test
    void shouldMoveTaskToRetryPendingQueue(){

        boolean result = retryService.retry(task);

        assertTrue(result);

        assertEquals(1, task.getRetryCount());
        assertEquals(TaskStatus.RETRY_PENDING, task.getStatus());

        assertNotNull(task.getNextRetryAt());
        assertTrue(task.getNextRetryAt().isAfter(Instant.now()));

        assertNull(task.getWorkerId());
        assertNull(task.getLeaseId());
        assertNull(task.getLeasedUntil());

        assertEquals(
            0L,
            stringRedisTemplate.opsForZSet().zCard(
                RedisKeys.processingQueue(task.getQueueName())
            )
        );

        assertEquals(
            1L,
            stringRedisTemplate.opsForZSet().zCard(
                RedisKeys.retryPendingQueue()
            )
        );

        Task savedTask = taskRedisTemplate.opsForValue().get(
            RedisKeys.task(task.getTaskId())
        );

        assertNotNull(savedTask);
        assertEquals(TaskStatus.RETRY_PENDING, savedTask.getStatus());
        assertEquals(1, savedTask.getRetryCount());
    }

    @Test
    void shouldNotRetryWhenTaskIsNotInProcessingQueue(){

        stringRedisTemplate.opsForZSet().remove(
            RedisKeys.processingQueue(task.getQueueName()),
            task.getTaskId().toString()
        );

        boolean result = retryService.retry(task);

        assertFalse(result);

        assertEquals(
            0,
            task.getRetryCount()
        );

        assertEquals(
            0L,
            stringRedisTemplate.opsForZSet().zCard(
                RedisKeys.retryPendingQueue()
            )
        );
    }

    @Test
    void shouldMarkTaskDeadLetterWhenRetryLimitIsExhausted(){

        task.setRetryCount(3);

        taskRedisTemplate.opsForValue().set(
            RedisKeys.task(task.getTaskId()),
            task
        );

        boolean result = retryService.retry(task);

        assertFalse(result);
        assertEquals(TaskStatus.DEAD_LETTER, task.getStatus());

        Task savedTask = taskRedisTemplate.opsForValue().get(
            RedisKeys.task(task.getTaskId())
        );

        assertNotNull(savedTask);
        assertEquals(TaskStatus.DEAD_LETTER, savedTask.getStatus());

        // DLQ movement is not implemented yet.
        assertEquals(
            1L,
            stringRedisTemplate.opsForZSet().zCard(
                RedisKeys.processingQueue(task.getQueueName())
            )
        );
    }
}
