package com.example.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;


public class StringMsgListerner implements AcknowledgingMessageListener<Integer, String> {
    @Override
    public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {
        System.out.println("StringMsgListerner recv: " + data.value());
        acknowledgment.acknowledge();

    }
}
