package com.taskqueue.broker.queue;

import com.taskqueue.broker.model.Task;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

public interface TaskQueue {

    void enqueue(Task task);

    Optional<Task> claimNext(String queueName, String workerId, Duration leaseDuration);

    boolean cancel(UUID taskId);

    long size(String queueName);

    boolean isEmpty(String queueName);

    void requeue(Task task);
}
