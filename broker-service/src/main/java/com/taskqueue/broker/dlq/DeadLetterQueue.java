package com.taskqueue.broker.dlq;

import com.taskqueue.broker.model.Task;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DeadLetterQueue{

    
    // Moves a task from the processing queue to the dead-letter queue.

    boolean moveToDeadLetter(Task task);


    //Retrieves a task from the dead-letter queue.
    
    Optional<Task> findById(UUID taskId, String queueName);


    // Retrieves dead-letter tasks for a queue.
     
    List<Task> findAll(String queueName);

    // Removes a task from the dead-letter queue permanently.
     
    boolean delete(UUID taskId, String queueName);

    // Moves a task from the dead-letter queue back to the ready queue.
     
    boolean redrive(UUID taskId, String queueName);
}
