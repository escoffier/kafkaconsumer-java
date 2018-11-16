package com.example.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.omg.PortableServer.THREAD_POLICY_ID;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JsonListerner implements AcknowledgingMessageListener<Integer, AccessLog> {

    private ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Override
    public void onMessage(ConsumerRecord<Integer, AccessLog> data, Acknowledgment acknowledgment) {

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                System.out.println("onMessage Thread"+ Thread.currentThread().getName()+ "----JsonListerner : " + data.toString());
                acknowledgment.acknowledge();
            }
        });
        //System.out.println("JsonListerner : " + data.toString());
        //acknowledgment.acknowledge();

    }

    public void setCurrency(int currency) {

    }
}
