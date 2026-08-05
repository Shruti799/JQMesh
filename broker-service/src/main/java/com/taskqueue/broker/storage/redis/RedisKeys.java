package com.taskqueue.broker.storage.redis;

import java.util.UUID;

public class RedisKeys {
    private static final String QUEUE_PREFIX = "queue:";
    private static final String TASK_PREFIX = "task:";
    private static final String LEASE_PREFIX = "lease:";
    private static final String IDEMPOTENCY_PREFIX = "idempotency:";
    private static final String DLQ_PREFIX = "dlq:";
    private static final String WORKER_PREFIX = "worker:";

    private RedisKeys(){
        // Prevent instantiation
    }

    public static String queue(String queueName){
        return QUEUE_PREFIX + queueName;
    }

     public static String task(UUID taskId){
        return TASK_PREFIX + taskId;
    }

    public static String lease(String leaseId){
        return LEASE_PREFIX + leaseId;
    }

    public static String idempotency(String idempotencyKey){
        return IDEMPOTENCY_PREFIX + idempotencyKey;
    }

    public static String deadLetterQueue(String queueName){
        return DLQ_PREFIX + queueName;
    }

    public static String worker(String workerId){
       return WORKER_PREFIX + workerId;
    }

    public static String processingQueue(String queueName) {
       return "processing:" + queueName;
    }

}
