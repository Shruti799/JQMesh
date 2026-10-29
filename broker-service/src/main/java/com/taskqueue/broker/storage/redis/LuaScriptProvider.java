package com.taskqueue.broker.storage.redis;

import java.nio.LongBuffer;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

@Component
public class LuaScriptProvider{

    private final DefaultRedisScript<String> claimTaskScript;
    private final DefaultRedisScript<Long> retryTaskScript;
    private final DefaultRedisScript<Long> ackTaskScript;
    private final DefaultRedisScript<Long> cancelTaskScript;

    public LuaScriptProvider(){

        claimTaskScript = new DefaultRedisScript<>();

        claimTaskScript.setLocation(
            new ClassPathResource("scripts/claim-task.lua")
        );

        claimTaskScript.setResultType(String.class);

        retryTaskScript = new DefaultRedisScript<>();

        retryTaskScript.setLocation(new ClassPathResource("scripts/retry-task.lua"));

        retryTaskScript.setResultType(Long.class);

        ackTaskScript = new DefaultRedisScript<>();

        ackTaskScript.setLocation(new ClassPathResource("scripts/ack-task.lua"));

        ackTaskScript.setResultType(Long.class);

        cancelTaskScript = new DefaultRedisScript<>();

        cancelTaskScript.setLocation(new ClassPathResource("scripts/cancel-task.lua"));

        cancelTaskScript.setResultType(Long.class);
    }

    public DefaultRedisScript<String> getClaimTaskScript(){
        return claimTaskScript;
    }

    public DefaultRedisScript<Long> getRetryTaskScript(){
        return retryTaskScript;
    }

    public DefaultRedisScript<Long> getAckTaskScript(){
        return ackTaskScript;
    }

    public DefaultRedisScript<Long> getCancelTaskScript() {
        return cancelTaskScript;
    }
}
