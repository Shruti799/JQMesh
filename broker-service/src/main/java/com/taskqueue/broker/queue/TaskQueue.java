package com.taskqueue.broker.queue;

import com.taskqueue.broker.model.Task;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

public interface TaskQueue {

    void enqueue(Task task);

    Optional<Task> claimNext(String queueName, String workerId, Duration leaseDuration);

    boolean acknowledge(UUID taskId, UUID leaseId);

    boolean fail(UUID taskId, UUID leaseId, String failureReason);

    boolean releaseLease(UUID taskId, UUID leaseId);

    boolean cancel(UUID taskId);

    long size(String queueName);

    boolean isEmpty(String queueName);

    void requeue(Task task);
}
