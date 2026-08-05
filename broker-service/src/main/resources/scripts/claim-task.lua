-- KEYS[1] = Ready queue
-- KEYS[2] = Processing queue
-- ARGV[1] = Lease expiry timestamp

local task = redis.call('ZPOPMAX', KEYS[1])

if (#task == 0) then
    return nil
end

local taskId = task[1]

-- Adding to processing queue
-- Score = lease expiry time
redis.call(
    'ZADD',
    KEYS[2],
    ARGV[1],
    taskId
)

return taskId