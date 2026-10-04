package com.taskqueue.broker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.taskqueue.broker.model.Task;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig{

    @Bean
    public RedisTemplate<String, Task> redisTemplate(RedisConnectionFactory connectionFactory){
    
        RedisTemplate<String, Task> redisTemplate = new RedisTemplate<>();
    
        redisTemplate.setConnectionFactory(connectionFactory);
    
        StringRedisSerializer keySerializer = new StringRedisSerializer();
    
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    
        Jackson2JsonRedisSerializer<Task> valueSerializer =
                new Jackson2JsonRedisSerializer<>(
                        objectMapper,
                        Task.class
                );
    
        redisTemplate.setKeySerializer(keySerializer);
        redisTemplate.setHashKeySerializer(keySerializer);
    
        redisTemplate.setValueSerializer(valueSerializer);
        redisTemplate.setHashValueSerializer(valueSerializer);
    
        redisTemplate.afterPropertiesSet();
    
        return redisTemplate;
   }
}