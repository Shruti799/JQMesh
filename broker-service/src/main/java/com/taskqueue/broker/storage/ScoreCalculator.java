package com.taskqueue.broker.storage;

import com.taskqueue.broker.model.Task;
import com.taskqueue.broker.model.TaskPriority;

import java.time.Instant;

public final class ScoreCalculator {

    private static final double HIGH_PRIORITY_WEIGHT = 1_000_000;
    private static final double MEDIUM_PRIORITY_WEIGHT = 500_000;
    private static final double LOW_PRIORITY_WEIGHT = 100_000;

    private ScoreCalculator() {
        // Utility class
    }

    public static double calculate(Task task) {

        double priorityWeight = switch (task.getPriority()) {
            case HIGH -> HIGH_PRIORITY_WEIGHT;
            case MEDIUM -> MEDIUM_PRIORITY_WEIGHT;
            case LOW -> LOW_PRIORITY_WEIGHT;
        };

        double agingBonus = Instant.now().getEpochSecond() - task.getCreatedAt().getEpochSecond();

        double retryPenalty = task.getRetryCount() * 1000;

        return priorityWeight + agingBonus - retryPenalty;
    }
    
}
