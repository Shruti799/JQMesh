package com.taskqueue.broker.cancel;

import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.model.TaskStatus;
import com.taskqueue.broker.storage.redis.LuaScriptProvider;
import com.taskqueue.broker.config.RedisConfig;
import com.taskqueue.broker.storage.redis.RedisKeys;
import com.taskqueue.broker.queue.RedisPriorityQueue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringJUnitConfig(CancelServiceTest.TestConfig.class)
class CancelServiceTest {

    @Autowired
    private RedisPriorityQueue redisPriorityQueue;

    @Autowired
    private RedisTemplate<String, String> stringRedisTemplate;

    @Autowired
    private RedisTemplate<String, Task> taskRedisTemplate;

    @BeforeEach
    void setUp() {
        stringRedisTemplate.getConnectionFactory()
                .getConnection()
                .serverCommands()
                .flushDb();
    }

    private Task createTask(TaskStatus status) {
        Task task = new Task();
        task.setTaskId(UUID.randomUUID());
        task.setQueueName("test-queue");
        task.setStatus(status);
        task.setCreatedAt(Instant.now());
        task.setUpdatedAt(Instant.now());

        taskRedisTemplate.opsForValue().set(
                RedisKeys.task(task.getTaskId()),
                task
        );

        return task;
    }

    @Test
    void cancelQueuedTask() {
        Task task = createTask(TaskStatus.QUEUED);

        stringRedisTemplate.opsForZSet().add(
                RedisKeys.queue(task.getQueueName()),
                task.getTaskId().toString(),
                1.0
        );

        boolean result = redisPriorityQueue.cancel(task.getTaskId());

        assertTrue(result);

        Task updatedTask = taskRedisTemplate.opsForValue()
                .get(RedisKeys.task(task.getTaskId()));

        assertNotNull(updatedTask);
        assertEquals(TaskStatus.CANCELLED, updatedTask.getStatus());

        assertFalse(stringRedisTemplate.opsForZSet().rank(
                RedisKeys.queue(task.getQueueName()),
                task.getTaskId().toString()
        ) != null);
    }

    @Test
    void cancelRetryPendingTask() {
        Task task = createTask(TaskStatus.RETRY_PENDING);

        stringRedisTemplate.opsForZSet().add(
                RedisKeys.retryPendingQueue(),
                task.getTaskId().toString(),
                1.0
        );

        boolean result = redisPriorityQueue.cancel(task.getTaskId());

        assertTrue(result);

        Task updatedTask = taskRedisTemplate.opsForValue()
                .get(RedisKeys.task(task.getTaskId()));

        assertNotNull(updatedTask);
        assertEquals(TaskStatus.CANCELLED, updatedTask.getStatus());

        assertNull(stringRedisTemplate.opsForZSet().rank(
                RedisKeys.retryPendingQueue(),
                task.getTaskId().toString()
        ));
    }

    @Test
    void cancelNonexistentTaskReturnsFalse() {
        boolean result = redisPriorityQueue.cancel(UUID.randomUUID());

        assertFalse(result);
    }

    @Test
    void cannotCancelInProgressTask() {
        Task task = createTask(TaskStatus.IN_PROGRESS);

        boolean result = redisPriorityQueue.cancel(task.getTaskId());

        assertFalse(result);

        Task storedTask = taskRedisTemplate.opsForValue()
                .get(RedisKeys.task(task.getTaskId()));

        assertNotNull(storedTask);
        assertEquals(TaskStatus.IN_PROGRESS, storedTask.getStatus());
    }

    @Test
    void cannotCancelAlreadyCancelledTask() {
        Task task = createTask(TaskStatus.CANCELLED);

        boolean result = redisPriorityQueue.cancel(task.getTaskId());

        assertFalse(result);
    }

    @Test
    void cannotCancelCompletedTask() {
        Task task = createTask(TaskStatus.COMPLETED);

        boolean result = redisPriorityQueue.cancel(task.getTaskId());

        assertFalse(result);
    }

    @Test
    void cannotCancelDeadLetterTask() {
        Task task = createTask(TaskStatus.DEAD_LETTER);

        boolean result = redisPriorityQueue.cancel(task.getTaskId());

        assertFalse(result);
    }

    @TestConfiguration
    @Import({
            RedisAutoConfiguration.class,
            RedisConfig.class,
            LuaScriptProvider.class
    })
    static class TestConfig {

        @Bean
        RedisPriorityQueue redisPriorityQueue(
                RedisTemplate<String, String> stringRedisTemplate,
                RedisTemplate<String, Task> taskRedisTemplate,
                LuaScriptProvider luaScriptProvider) {

            return new RedisPriorityQueue(
                    stringRedisTemplate,
                    taskRedisTemplate,
                    luaScriptProvider
            );
        }
    }
}

