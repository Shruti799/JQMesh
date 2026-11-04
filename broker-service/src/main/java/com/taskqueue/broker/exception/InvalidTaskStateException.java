package com.taskqueue.broker.exception;

import java.util.UUID;

public class InvalidTaskStateException extends RuntimeException{

    public InvalidTaskStateException(UUID taskId, String currentState, String operation){
        super("Cannot perform operation '" + operation
             + "' on task " + taskId
             + " because its current state is " + currentState);
    }

    public InvalidTaskStateException(String message){
        super(message);
    }
}
