-- KEYS[1] = Idempotency key
-- ARGV[1] = Task ID

local existingTaskId = redis.call('GET', KEYS[1])

if existingTaskId == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end

return 0