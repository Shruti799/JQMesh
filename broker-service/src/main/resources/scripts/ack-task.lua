-- KEYS[1] = processing queue
-- ARGV[1] = task ID

local processingQueue = KEYS[1]
local taskId = ARGV[1]

local exists = redis.call(
    'ZSCORE',
    processingQueue,
    taskId
)

if not exists then
    return 0
end

local removed = redis.call(
    'ZREM',
    processingQueue,
    taskId
)

return removed