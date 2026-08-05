local queueKey = KEYS[1]

local task = redis.call('ZPOPMAX', queueKey)

if(#task == 0) then
    return nil
end

return task[1]