package com.taskqueue.broker.retry;
import com.taskqueue.broker.model.Task;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class ExponentialBackoffPolicy implements RetryPolicy{

    private final Duration baseDelay;
    private final Duration maxDelay;
    private final Duration jitter;

    public ExponentialBackoffPolicy(Duration baseDelay, Duration maxDelay, Duration jitter){
        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;
        this.jitter = jitter;
    }

    @Override
    public Duration computeDelay(Task task){

        int retryCount = task.getRetryCount();

        long exponentialDelaySeconds = baseDelay.toSeconds() * (1L << Math.max(0, retryCount - 1));

        long maxDelaySeconds = maxDelay.toSeconds();

        long cappedDelaySeconds = Math.min(exponentialDelaySeconds, maxDelaySeconds);

        long jitterSeconds = ThreadLocalRandom.current().nextLong(0, jitter.toSeconds() + 1);

        long finalDelaySeconds = Math.min(cappedDelaySeconds + jitterSeconds, maxDelaySeconds);

        return Duration.ofSeconds(finalDelaySeconds);
    }
}