package com.taskqueue.broker.dlq;

import com.taskqueue.broker.model.Task;

import lombok.RequiredArgsConstructor;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DeadLetterService{

    private final DeadLetterQueue deadLetterQueue;

    public boolean moveToDeadLetter(Task task){
        return deadLetterQueue.moveToDeadLetter(task);
    }

    public Optional<Task> findById(UUID taskId, String queueName){
        return deadLetterQueue.findById(taskId, queueName);
    }

    public List<Task> findAll(String queueName){
        return deadLetterQueue.findAll(queueName);
    }

    public boolean delete(UUID taskId, String queueName){
        return deadLetterQueue.delete(taskId, queueName);
    }

    public boolean redrive(UUID taskId, String queueName){
        return deadLetterQueue.redrive(taskId, queueName);
    }
}
