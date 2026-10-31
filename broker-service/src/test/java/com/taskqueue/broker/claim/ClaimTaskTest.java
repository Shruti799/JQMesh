
package com.taskqueue.broker.claim;

import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.model.TaskPriority;
import com.taskqueue.broker.model.TaskStatus;
import com.taskqueue.broker.model.TaskType;
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
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringJUnitConfig(ClaimTaskTest.TestConfig.class)
class ClaimTaskTest {

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

    @Autowired
    private RedisPriorityQueue redisPriorityQueue;

    @Autowired
    private RedisTemplate<String, String> stringRedisTemplate;

    @Autowired
    private RedisTemplate<String, Task> taskRedisTemplate;

    private final String queueName = "claim-test-queue";

    @BeforeEach
    void setUp() {

        stringRedisTemplate.getConnectionFactory()
            .getConnection()
            .serverCommands()
            .flushDb();
    }

    private Task createTask(double score) {

        Task task = Task.builder()
            .taskId(UUID.randomUUID())
            .queueName(queueName)
            .taskType(TaskType.values()[0])
            .payload("{\"message\":\"test\"}")
            .priority(TaskPriority.values()[0])
            .computedScore(score)
            .status(TaskStatus.QUEUED)
            .createdAt(Instant.now())
            .updatedAt(Instant.now())
            .retryCount(0)
            .deliveryCount(0)
            .maxRetries(3)
            .build();

        taskRedisTemplate.opsForValue().set(
            RedisKeys.task(task.getTaskId()),
            task
        );

        stringRedisTemplate.opsForZSet().add(
            RedisKeys.queue(queueName),
            task.getTaskId().toString(),
            score
        );

        return task;
    }

    @Test
    void shouldClaimHighestPriorityTask() {

        Task lowPriorityTask = createTask(10.0);
        Task highPriorityTask = createTask(100.0);

        Optional<Task> result = redisPriorityQueue.claimNext(
            queueName,
            "worker-1",
            Duration.ofSeconds(30)
        );

        assertTrue(result.isPresent());

        assertEquals(
            highPriorityTask.getTaskId(),
            result.get().getTaskId()
        );

        assertEquals(1L,
            stringRedisTemplate.opsForZSet()
                .zCard(RedisKeys.processingQueue(queueName))
        );

        assertEquals(1L,
            stringRedisTemplate.opsForZSet()
                .zCard(RedisKeys.queue(queueName))
        );
    }

    @Test
    void shouldReturnEmptyWhenQueueIsEmpty() {

        Optional<Task> result = redisPriorityQueue.claimNext(
            queueName,
            "worker-1",
            Duration.ofSeconds(30)
        );

        assertTrue(result.isEmpty());
    }

    @Test
    void shouldUpdateTaskStatusAfterClaim() {

        Task task = createTask(50.0);

        Optional<Task> result = redisPriorityQueue.claimNext(
            queueName,
            "worker-1",
            Duration.ofSeconds(30)
        );

        assertTrue(result.isPresent());

        Task claimedTask = result.get();

        assertEquals(TaskStatus.IN_PROGRESS, claimedTask.getStatus());
        assertEquals("worker-1", claimedTask.getWorkerId());

        assertNotNull(claimedTask.getStartedAt());
        assertNotNull(claimedTask.getUpdatedAt());

        assertEquals(
            1,
            claimedTask.getDeliveryCount()
        );

        assertNotNull(claimedTask.getLeaseId());
        assertNotNull(claimedTask.getLeasedUntil());
    }

    @Test
    void shouldAssignLeaseExpiryCorrectly() {

        createTask(50.0);

        Duration leaseDuration = Duration.ofSeconds(60);

        Instant beforeClaim = Instant.now();

        Optional<Task> result = redisPriorityQueue.claimNext(
            queueName,
            "worker-1",
            leaseDuration
        );

        Instant afterClaim = Instant.now();

        assertTrue(result.isPresent());

        Instant leasedUntil = result.get().getLeasedUntil();

        assertFalse(
            leasedUntil.isBefore(beforeClaim.plus(leaseDuration))
        );

        assertFalse(
            leasedUntil.isAfter(afterClaim.plus(leaseDuration))
        );
    }

    @Test
    void shouldNotAllowSameTaskToBeClaimedTwice() {

        createTask(50.0);

        Optional<Task> firstClaim = redisPriorityQueue.claimNext(
            queueName,
            "worker-1",
            Duration.ofSeconds(30)
        );

        Optional<Task> secondClaim = redisPriorityQueue.claimNext(
            queueName,
            "worker-2",
            Duration.ofSeconds(30)
        );

        assertTrue(firstClaim.isPresent());
        assertTrue(secondClaim.isEmpty());

        assertEquals(1L,
            stringRedisTemplate.opsForZSet()
                .zCard(RedisKeys.processingQueue(queueName))
        );
    }

    @Test
    void shouldIncrementDeliveryCountOnEveryClaim() {

        Task task = createTask(50.0);

        Optional<Task> firstClaim = redisPriorityQueue.claimNext(
            queueName,
            "worker-1",
            Duration.ofSeconds(30)
        );

        assertTrue(firstClaim.isPresent());
        assertEquals(1, firstClaim.get().getDeliveryCount());

        // Simulate the task becoming available again.
        redisPriorityQueue.requeue(firstClaim.get());

        Optional<Task> secondClaim = redisPriorityQueue.claimNext(
            queueName,
            "worker-2",
            Duration.ofSeconds(30)
        );

        assertTrue(secondClaim.isPresent());
        assertEquals(2, secondClaim.get().getDeliveryCount());
    }

    @Test
    void shouldRemoveStaleProcessingEntryWhenTaskDoesNotExist() {

        String taskId = UUID.randomUUID().toString();

        stringRedisTemplate.opsForZSet().add(
            RedisKeys.queue(queueName),
            taskId,
            50.0
        );

        Optional<Task> result = redisPriorityQueue.claimNext(
            queueName,
            "worker-1",
            Duration.ofSeconds(30)
        );

        assertTrue(result.isEmpty());

        assertEquals(
            0L,
            stringRedisTemplate.opsForZSet()
                .zCard(RedisKeys.processingQueue(queueName))
        );
    }
}

