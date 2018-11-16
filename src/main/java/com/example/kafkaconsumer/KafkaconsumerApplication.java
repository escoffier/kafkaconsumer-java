package com.example.kafkaconsumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaconsumerApplication {

    @Autowired
    private Listener listener = null;

    public static void main(String[] args) {
        SpringApplication.run(KafkaconsumerApplication.class, args);
    }
}
