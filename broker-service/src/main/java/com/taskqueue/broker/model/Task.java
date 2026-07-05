package com.taskqueue.broker.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor

public class Task implements Serializable{

    private static final long serialVersionUID = 1L;

    private UUID taskId;

    private String queueName;

    private TaskType taskType;

    private String payload;

    private TaskPriority priority;

    private Double computedScore;

    private TaskStatus status;

    private Instant createdAt;

    private Instant startedAt;

    private Instant completedAt;

    private Instant updatedAt;

    private Instant scheduledAt;

    private Instant leasedUntil;

    private Instant nextRetryAt;

    private long leaseVersion;

    private int retryCount;

    private int deliveryCount;

    private int maxRetries;

    private String idempotencyKey;

    private String workerId;

    private String failureReason;

    private Map<String, String> metadata;

}


