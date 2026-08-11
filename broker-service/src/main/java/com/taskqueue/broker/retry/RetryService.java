package com.taskqueue.broker.retry;

import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.model.TaskStatus;
import com.taskqueue.broker.queue.TaskQueue;

import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Service
public class RetryService{

    private final TaskQueue taskQueue;
    private final RetryPolicy retryPolicy;

    public RetryService(TaskQueue taskQueue, RetryPolicy retryPolicy){
        this.taskQueue = taskQueue;
        this.retryPolicy = retryPolicy;
    }

    public boolean retry(Task task){

        // Checking whether the task has exhausted its retries
        if (task.getRetryCount() >= task.getMaxRetries()){

            task.setStatus(TaskStatus.DEAD_LETTER);
            task.setUpdatedAt(Instant.now());

            return false;
        }

        // Incrementing retry count
        task.setRetryCount(task.getRetryCount() + 1);

        // Calculating retry delay
        Duration delay = retryPolicy.computeDelay(task);

        // Setting the time when the task becomes eligible again
        task.setNextRetryAt(Instant.now().plus(delay));

        // Task is waiting for its retry time
        task.setStatus(TaskStatus.RETRY_PENDING);

        task.setWorkerId(null);
        task.setLeaseId(null);
        task.setLeasedUntil(null);

        task.setUpdatedAt(Instant.now());

        return true;
    }
}