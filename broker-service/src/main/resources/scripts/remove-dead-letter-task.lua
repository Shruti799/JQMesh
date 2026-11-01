-- KEYS[1] = Dead-letter queue (dlq:<queueName>)
-- ARGV[1] = Task ID

local deadLetterQueue = KEYS[1]
local taskId = ARGV[1]

-- Remove the task from the dead-letter queue.
local removed = redis.call('ZREM', deadLetterQueue, taskId)

if removed == 1 then
    return 1
end

return 0