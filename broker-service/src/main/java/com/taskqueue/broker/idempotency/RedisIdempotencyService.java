package com.taskqueue.broker.idempotency;

import com.taskqueue.broker.storage.redis.RedisKeys;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RedisIdempotencyService implements IdempotencyService{

    private final RedisTemplate<String, String> redisTemplate;

    private static final Duration IDEMPOTENCY_TTL = Duration.ofDays(7);

    @Override
    public UUID getExistingTaskId(String idempotencyKey){

        String taskId = redisTemplate.opsForValue().get(RedisKeys.idempotency(idempotencyKey));

        return taskId == null ? null : UUID.fromString(taskId);
    }

    @Override
    public boolean register(String idempotencyKey, UUID taskId){

        Boolean registered = redisTemplate.opsForValue().setIfAbsent(
            RedisKeys.idempotency(idempotencyKey),
            taskId.toString(),
            IDEMPOTENCY_TTL
        );

        return Boolean.TRUE.equals(registered);
    }
}