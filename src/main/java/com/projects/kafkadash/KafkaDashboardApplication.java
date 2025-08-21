package com.projects.kafkadash;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaDashboardApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaDashboardApplication.class, args);
    }
}
