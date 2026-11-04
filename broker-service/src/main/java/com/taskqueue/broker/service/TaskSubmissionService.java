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

        if(task == null){
            throw new IllegalArgumentException("Task cannot be null");
        }

        if(idempotencyKey == null || idempotencyKey.isBlank()){
            throw new IllegalArgumentException("Idempotency key cannot be empty");
        }

        // Check for an existing submission.
        UUID existingTaskId = idempotencyService.getExistingTaskId(idempotencyKey);

        if(existingTaskId != null){
            return existingTaskId;
        }

        // Atomically register the key.
        boolean registered = idempotencyService.register(idempotencyKey, task.getTaskId());

        if(!registered){
            // Another request may have registered this key concurrently.
            UUID concurrentTaskId = idempotencyService.getExistingTaskId(idempotencyKey);

            if(concurrentTaskId != null){
                return concurrentTaskId;
            }

            throw new IllegalStateException("Unable to resolve concurrent task submission");
        }

        try{
            // Enqueue only after successful registration.
            taskQueue.enqueue(task);

            return task.getTaskId();

        }catch(RuntimeException exception){

            // Release only if the mapping still belongs to this task.
            idempotencyService.release(idempotencyKey, task.getTaskId());

            throw exception;
        }
    }
}