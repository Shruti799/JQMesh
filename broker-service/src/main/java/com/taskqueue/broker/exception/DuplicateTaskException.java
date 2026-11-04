package com.taskqueue.broker.exception;

public class DuplicateTaskException extends RuntimeException{

    public DuplicateTaskException(String idempotencyKey){
        super("A task already exists for idempotency key: " + idempotencyKey);
    }

    public DuplicateTaskException(String message, Throwable cause){
        super(message, cause);
    }
}
