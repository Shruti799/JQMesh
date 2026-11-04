package com.taskqueue.broker.idempotency;

import java.util.UUID;

public interface IdempotencyService{
    
    // Returns the task ID associated with the given idempotency key.
    // Returns null if the key has not been registered.
    
    UUID getExistingTaskId(String idempotencyKey);

    
    // Registers an idempotency key against a task ID.
    // Returns true if registration succeeds, false if the key already exists.
    
    boolean register(String idempotencyKey, UUID taskId);

    boolean release(String idempotencyKey, UUID taskId);
}
