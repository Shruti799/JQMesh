package com.taskqueue.broker.retry;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class RetryConfig{

    @Bean
    public RetryPolicy retryPolicy(){

        return new ExponentialBackoffPolicy(
            Duration.ofSeconds(5),    // Base delay
            Duration.ofMinutes(5),    // Maximum delay
            Duration.ofSeconds(5)     // Maximum jitter
        );
    }
}