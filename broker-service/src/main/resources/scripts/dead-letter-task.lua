-- KEYS[1] = Processing queue (processing:<queueName>)
-- KEYS[2] = Dead-letter queue (dlq:<queueName>)
-- ARGV[1] = Task ID
-- ARGV[2] = Dead-letter timestamp (epoch milliseconds)

local processingQueue = KEYS[1]
local deadLetterQueue = KEYS[2]
local taskId = ARGV[1]
local deadLetterTimestamp = tonumber(ARGV[2])

-- Remove the task from the processing queue.
local removed = redis.call('ZREM', processingQueue, taskId)

-- Move it to the dead-letter queue only if it was in processing.
if removed == 1 then
    redis.call('ZADD', deadLetterQueue, deadLetterTimestamp, taskId)
    return 1
end

return 0