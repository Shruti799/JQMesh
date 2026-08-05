package com.taskqueue.broker.storage.redis;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

@Component
public class LuaScriptProvider{

    private final DefaultRedisScript<String> claimTaskScript;

    public LuaScriptProvider(){

        claimTaskScript = new DefaultRedisScript<>();

        claimTaskScript.setLocation(
            new ClassPathResource("scripts/claim-task.lua")
        );

        claimTaskScript.setResultType(String.class);
    }

    public DefaultRedisScript<String> getClaimTaskScript(){
        return claimTaskScript;
    }
}
