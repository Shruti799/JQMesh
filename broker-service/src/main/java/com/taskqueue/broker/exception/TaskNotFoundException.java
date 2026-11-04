package com.taskqueue.broker.exception;

import java.util.UUID;

public class TaskNotFoundException extends RuntimeException{

    public TaskNotFoundException(UUID taskId){
        super("Task not found with ID: " + taskId);
    }

    public TaskNotFoundException(String message){
        super(message);
    }
}