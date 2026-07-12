package com.taskqueue.broker.queue;

import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.model.TaskStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryQueue implements TaskQueue{

    private final Map<String, PriorityQueue<Task>> queues;
    private final Map<UUID, Task> taskStore;
    private final Comparator<Task> taskComparator;

    public InMemoryQueue(){

        this.queues = new ConcurrentHashMap<>();
    
        this.taskStore = new ConcurrentHashMap<>();
    
        this.taskComparator = Comparator.comparing(Task::getComputedScore).reversed();
    }

    private PriorityQueue<Task> getOrCreateQueue(String queueName){

        return queues.computeIfAbsent(
            queueName,
            key -> new PriorityQueue<>(taskComparator)
        );
    }

    @Override
    public void enqueue(Task task){
    
        PriorityQueue<Task> queue = getOrCreateQueue(task.getQueueName());
    
        synchronized (queue){
    
            task.setStatus(TaskStatus.QUEUED);
    
            task.setUpdatedAt(Instant.now());
    
            queue.offer(task);
    
            taskStore.put(task.getTaskId(), task);
        }
    }

    @Override
    public Optional<Task> claimNext(String queueName, String workerId, Duration leaseDuration){
    
        PriorityQueue<Task> queue = queues.get(queueName);
    
        if(queue == null){
            return Optional.empty();
        }
    
        synchronized (queue){
    
            Task task = queue.poll();
    
            if(task == null){
                return Optional.empty();
            }
    
            task.setStatus(TaskStatus.IN_PROGRESS);
    
            task.setWorkerId(workerId);
    
            task.setStartedAt(Instant.now());
    
            task.setUpdatedAt(Instant.now());
    
            task.setLeasedUntil(
                Instant.now().plus(leaseDuration)
            );
    
            task.setDeliveryCount(
                task.getDeliveryCount() + 1
            );
    
            return Optional.of(task);
        }
    }

    @Override
    public boolean cancel(UUID taskId){
    
        Task task = taskStore.remove(taskId);
    
        if(task == null){
            return false;
        }
    
        PriorityQueue<Task> queue =
            queues.get(task.getQueueName());
    
        if(queue != null){
    
            synchronized (queue) {
                queue.remove(task);
            }
        }
    
        task.setStatus(TaskStatus.CANCELLED);
    
        task.setUpdatedAt(Instant.now());
    
        return true;
    }

    @Override
    public long size(String queueName){
    
        PriorityQueue<Task> queue =
                queues.get(queueName);
    
        return queue == null ? 0 : queue.size();
    }

    @Override
    public boolean isEmpty(String queueName){
    
        return size(queueName) == 0;
    }

    @Override
    public void requeue(Task task){
    
        task.setStatus(TaskStatus.QUEUED);
    
        task.setWorkerId(null);
    
        task.setLeasedUntil(null);
    
        task.setUpdatedAt(Instant.now());
    
        PriorityQueue<Task> queue =
                getOrCreateQueue(task.getQueueName());
    
        synchronized (queue){
    
            queue.offer(task);
        }
    }
}
