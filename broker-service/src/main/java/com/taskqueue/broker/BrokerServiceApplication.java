package com.taskqueue.broker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BrokerServiceApplication {
	public static void main(String[] args) {
		SpringApplication.run(BrokerServiceApplication.class, args);
	}

}

