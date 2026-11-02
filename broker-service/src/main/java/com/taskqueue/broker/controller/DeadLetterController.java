package com.taskqueue.broker.controller;

import com.taskqueue.broker.dlq.DeadLetterService;
import com.taskqueue.broker.model.Task;

import lombok.RequiredArgsConstructor;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/dlq")
@RequiredArgsConstructor
public class DeadLetterController {

    private final DeadLetterService deadLetterService;

    // Retrieve all dead-letter tasks for a queue.
    @GetMapping("/{queueName}")
    public ResponseEntity<List<Task>> getDeadLetterTasks(@PathVariable String queueName){

        return ResponseEntity.ok(deadLetterService.findAll(queueName));
    }

    // Retrieve a specific dead-letter task.
    @GetMapping("/{queueName}/{taskId}")
    public ResponseEntity<Task> getDeadLetterTask(
            @PathVariable String queueName,
            @PathVariable UUID taskId) {

        return deadLetterService.findById(taskId, queueName)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    // Redrive a dead-letter task back to the ready queue.
    @PostMapping("/{queueName}/{taskId}/redrive")
    public ResponseEntity<String> redriveTask(
            @PathVariable String queueName,
            @PathVariable UUID taskId) {

        boolean redriven = deadLetterService.redrive(taskId, queueName);

        if(!redriven){
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Task could not be redriven.");
        }

        return ResponseEntity.ok("Task successfully redriven.");
    }

    // Permanently delete a dead-letter task.
    @DeleteMapping("/{queueName}/{taskId}")
    public ResponseEntity<String> deleteDeadLetterTask(@PathVariable String queueName, @PathVariable UUID taskId) {

        boolean deleted = deadLetterService.delete(taskId, queueName);

        if(!deleted){
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok("Dead-letter task deleted successfully.");
    }
}
