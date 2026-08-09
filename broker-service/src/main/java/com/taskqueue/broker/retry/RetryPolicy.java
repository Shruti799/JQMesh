package com.taskqueue.broker.retry;
 
import com.taskqueue.broker.model.Task;
import java.time.Duration;


public interface RetryPolicy{

    Duration computeDelay(Task task);
    
}
