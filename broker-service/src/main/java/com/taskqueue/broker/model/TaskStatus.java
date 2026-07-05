package com.taskqueue.broker.model;

public enum TaskStatus {
    SCHEDULED,
    QUEUED,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    RETRY_PENDING,
    DEAD_LETTER,
    CANCELLED
}
