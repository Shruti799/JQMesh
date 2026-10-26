
-- KEYS[1] = Main queue key: queue:<queueName>
-- KEYS[2] = Retry-pending queue key: retry:pending
-- ARGV[1] = Task ID

local taskId = ARGV[1]

-- Remove the task from both queues
local mainRemoved = redis.call('ZREM', KEYS[1], taskId)
local retryRemoved = redis.call('ZREM', KEYS[2], taskId)

-- Return the total number of queue entries removed
return mainRemoved + retryRemoved
