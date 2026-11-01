-- KEYS[1] = Dead-letter queue (dlq:<queueName>)
-- KEYS[2] = Ready queue (queue:<queueName>)
-- ARGV[1] = Task ID
-- ARGV[2] = Computed priority score

local deadLetterQueue = KEYS[1]
local readyQueue = KEYS[2]
local taskId = ARGV[1]
local priorityScore = tonumber(ARGV[2])

-- Remove the task from the dead-letter queue.
local removed = redis.call('ZREM', deadLetterQueue, taskId)

-- Move it to the ready queue only if removal succeeds.
if removed == 1 then
    redis.call('ZADD', readyQueue, priorityScore, taskId)
    return 1
end

return 0