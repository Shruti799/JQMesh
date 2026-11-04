package com.taskqueue.broker.service;

import com.taskqueue.broker.idempotency.IdempotencyService;
import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.queue.TaskQueue;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TaskSubmissionService{

    private final IdempotencyService idempotencyService;
    private final TaskQueue taskQueue;

    public UUID submit(Task task, String idempotencyKey){

        // Check whether this request was already processed.
        UUID existingTaskId = idempotencyService.getExistingTaskId(idempotencyKey);

        if(existingTaskId != null){
            return existingTaskId;
        }

        // Reserve the idempotency key before enqueueing.
        boolean registered = idempotencyService.register(idempotencyKey, task.getTaskId());

        if(!registered){
            // Another concurrent request registered the same key.
            return idempotencyService.getExistingTaskId(idempotencyKey);
        }

        // Enqueue only after successful registration.
        taskQueue.enqueue(task);

        return task.getTaskId();
    }
}
