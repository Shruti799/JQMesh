package com.taskqueue.broker.idempotency;

import com.taskqueue.broker.config.RedisConfig;
import com.taskqueue.broker.storage.redis.LuaScriptProvider;
import com.taskqueue.broker.storage.redis.RedisKeys;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringJUnitConfig(IdempotencyServiceTest.TestConfig.class)
class IdempotencyServiceTest{

    @TestConfiguration
    @Import({
        RedisAutoConfiguration.class,
        RedisConfig.class,
        LuaScriptProvider.class,
        RedisIdempotencyService.class
    })
    static class TestConfig{
    }

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    private static final String IDEMPOTENCY_KEY = "test-payment-123";

    private UUID taskId;

    @BeforeEach
    void setUp(){
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushDb();

        taskId = UUID.randomUUID();
    }

    @Test
    void shouldRegisterIdempotencyKey(){

        boolean registered = idempotencyService.register(IDEMPOTENCY_KEY, taskId);

        assertTrue(registered);

        assertEquals(
            taskId,
            idempotencyService.getExistingTaskId(IDEMPOTENCY_KEY)
        );
    }

    @Test
    void shouldNotRegisterDuplicateIdempotencyKey(){

        UUID firstTaskId = UUID.randomUUID();
        UUID secondTaskId = UUID.randomUUID();

        assertTrue(idempotencyService.register(IDEMPOTENCY_KEY, firstTaskId));

        assertFalse(idempotencyService.register(IDEMPOTENCY_KEY, secondTaskId));

        assertEquals(
            firstTaskId,
            idempotencyService.getExistingTaskId(IDEMPOTENCY_KEY)
        );
    }

    @Test
    void shouldReturnExistingTaskId(){

        idempotencyService.register(IDEMPOTENCY_KEY, taskId);

        UUID existingTaskId = idempotencyService.getExistingTaskId(IDEMPOTENCY_KEY);

        assertNotNull(existingTaskId);
        assertEquals(taskId, existingTaskId);
    }

    @Test
    void shouldReturnNullWhenIdempotencyKeyDoesNotExist(){

        UUID existingTaskId = idempotencyService.getExistingTaskId(IDEMPOTENCY_KEY);

        assertNull(existingTaskId);
    }

    @Test
    void shouldReleaseIdempotencyKeyWhenTaskIdMatches(){

        idempotencyService.register(IDEMPOTENCY_KEY, taskId);

        boolean released = idempotencyService.release(IDEMPOTENCY_KEY, taskId);

        assertTrue(released);

        assertNull(idempotencyService.getExistingTaskId(IDEMPOTENCY_KEY));
    }

    @Test
    void shouldNotReleaseIdempotencyKeyWhenTaskIdDoesNotMatch(){

        UUID anotherTaskId = UUID.randomUUID();

        idempotencyService.register(IDEMPOTENCY_KEY, taskId);

        boolean released = idempotencyService.release(IDEMPOTENCY_KEY, anotherTaskId);

        assertFalse(released);

        assertEquals(
            taskId,
            idempotencyService.getExistingTaskId(IDEMPOTENCY_KEY)
        );
    }

    @Test
    void shouldReturnFalseWhenReleasingNonExistingKey(){

        boolean released = idempotencyService.release(IDEMPOTENCY_KEY, taskId);

        assertFalse(released);
    }

    @Test
    void shouldSetExpiryOnRegisteredIdempotencyKey(){

        idempotencyService.register(IDEMPOTENCY_KEY, taskId);

        Long expiry = redisTemplate.getExpire(
            RedisKeys.idempotency(IDEMPOTENCY_KEY),
            TimeUnit.SECONDS
        );

        assertNotNull(expiry);
        assertTrue(expiry > 0);
        assertTrue(expiry <= Duration.ofDays(7).toSeconds());
    }
}