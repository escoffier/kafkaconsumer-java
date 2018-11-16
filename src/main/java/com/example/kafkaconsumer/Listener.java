package com.example.kafkaconsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class Listener {

    @KafkaListener(id = "foo", topics = "testtopic", clientIdPrefix = "testclient", autoStartup = "${listen.auto.start:true}", concurrency = "${listen.concurrency:3}")
    public void listern(String data, Acknowledgment acknowledgment){

        System.out.println("----receive: "+ data);
        acknowledgment.acknowledge();
    }


//    @KafkaListener(id = "batchfoo", topics = "testtopic", clientIdPrefix = "batchclient",containerFactory = "batchFactory")
//    public void listern(List<String> stringList, Acknowledgment acknowledgment){
//        System.out.println("----batch receive: "+ stringList.toString());
//        acknowledgment.acknowledge();
//
//    }

    @KafkaListener(id = "batchessagefoo", topics = "testtopic", clientIdPrefix = "batchclient",containerFactory = "batchFactory")
    public void listern(List<ConsumerRecord<Integer, String>> consumerRecordList, Acknowledgment acknowledgment, Consumer<Integer, String> consumer){

        for (ConsumerRecord<Integer, String> record : consumerRecordList) {
            System.out.println("----batch receive: " + record.value());
            acknowledgment.acknowledge();
        }

    }
}
