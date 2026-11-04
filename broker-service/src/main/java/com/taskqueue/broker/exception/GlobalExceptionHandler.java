package com.taskqueue.broker.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;

@RestControllerAdvice
public class GlobalExceptionHandler{

    @ExceptionHandler(TaskNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleTaskNotFound(
            TaskNotFoundException exception){

        return buildResponse(
            HttpStatus.NOT_FOUND,
            exception.getMessage()
        );
    }

    @ExceptionHandler(InvalidTaskStateException.class)
    public ResponseEntity<ErrorResponse> handleInvalidTaskState(
            InvalidTaskStateException exception){

        return buildResponse(
            HttpStatus.CONFLICT,
            exception.getMessage()
        );
    }

    @ExceptionHandler(DuplicateTaskException.class)
    public ResponseEntity<ErrorResponse> handleDuplicateTask(
            DuplicateTaskException exception){

        return buildResponse(
            HttpStatus.CONFLICT,
            exception.getMessage()
        )
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(
            IllegalArgumentException exception){

        return buildResponse(
            HttpStatus.BAD_REQUEST,
            exception.getMessage()
        );
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception exception){

        return buildResponse(
            HttpStatus.INTERNAL_SERVER_ERROR,
            "An unexpected error occurred"
        );
    }

    private ResponseEntity<ErrorResponse> buildResponse(
            HttpStatus status, String message){

        ErrorResponse response = new ErrorResponse(
            Instant.now(),
            status.value(),
            status.getReasonPhrase(),
            message
        );

        return ResponseEntity.status(status).body(response);
    }
}