-- KEYS[1] = processing queue
-- KEYS[2] = retry-pending queue
--
-- ARGV[1] = task ID
-- ARGV[2] = next retry timestamp (epoch millis)

local processingQueue = KEYS[1]
local retryPendingQueue = KEYS[2]

local taskId = ARGV[1]
local nextRetryAt = tonumber(ARGV[2])


-- Removing task from processing queue
local removed = redis.call(
    'ZREM',
    processingQueue,
    taskId
)

-- Only adding the task to retry-pending
-- if it was actually present in processing queue
if removed == 1 then

    redis.call(
        'ZADD',
        retryPendingQueue,
        nextRetryAt,
        taskId
    )

    return 1
end

return 0