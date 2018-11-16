package com.example.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Iterator;

@Component
public class MyRebalanceListener implements ConsumerRebalanceListener {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        Iterator<TopicPartition> it =  partitions.iterator();
        while (it.hasNext()) {
            TopicPartition tp = it.next();
            System.out.println("------onPartitionsRevoked: " + tp.toString());
        }

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        Iterator<TopicPartition> it = partitions.iterator();
        while (it.hasNext()) {
            TopicPartition tp = it.next();
            System.out.println("------onPartitionsAssigned: " + tp.toString());
        }

    }
}
