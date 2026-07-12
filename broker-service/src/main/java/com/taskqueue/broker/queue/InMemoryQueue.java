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
}
