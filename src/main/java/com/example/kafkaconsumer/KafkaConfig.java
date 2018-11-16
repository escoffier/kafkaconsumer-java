package com.example.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Autowired
    private MyRebalanceListener myRebalanceListener = null;

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
    kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();


        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setConsumerRebalanceListener(myRebalanceListener);
        factory.setConcurrency(3);
        factory.setConsumerFactory(consumerFactory());
        //factory.setRecordFilterStrategy();
        return factory;
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
    batchFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        return factory;
    }

    @Bean
    public ConsumerFactory<Integer, String> consumerFactory() {
        return  new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs(){
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.21.225:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        //disable auto commit offsets
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    @Bean
    public KafkaMessageListenerContainer<Integer, String> getContainer() {
        ContainerProperties containerProperties = new ContainerProperties("testtopic");
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProperties.setMessageListener(new StringMsgListerner());
        containerProperties.setGroupId("stringGroup");

        DefaultKafkaConsumerFactory<Integer, String> cf =
                new DefaultKafkaConsumerFactory<>(consumerConfigs());

        KafkaMessageListenerContainer<Integer, String> kafkaMessageListenerContainer =
                new KafkaMessageListenerContainer<>(cf, containerProperties);

        return kafkaMessageListenerContainer;
    }

    @Bean
    public Map<String, Object> jsonConsumerConfigs(){
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.21.225:9092");
        //props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);


        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, JsonDeserializer.class);

        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());

        //props.put(JsonDeserializer.KEY_DEFAULT_TYPE, "com.example.MyKey");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.example.AccessLog");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example");

        //disable auto commit offsets
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }


    @Bean(name = "jsonContainer")
    public KafkaMessageListenerContainer<Integer, AccessLog> jsonContainer() {
        ContainerProperties containerProperties = new ContainerProperties("jsontopic");
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProperties.setMessageListener(new JsonListerner());
        containerProperties.setGroupId("jsonGroup");


        DefaultKafkaConsumerFactory<Integer, AccessLog> cf =
                new DefaultKafkaConsumerFactory<>(jsonConsumerConfigs(), new IntegerDeserializer(), new JsonDeserializer<>(AccessLog.class, false));

        KafkaMessageListenerContainer<Integer, AccessLog> kafkaMessageListenerContainer =
                new KafkaMessageListenerContainer<>(cf, containerProperties);

        return kafkaMessageListenerContainer;
    }

    @Bean(name = "concurrencyContainer")
    public ConcurrentMessageListenerContainer<Integer, AccessLog> concurrencyContainer() {
        ContainerProperties containerProperties = new ContainerProperties("concurrencytopic");
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProperties.setMessageListener(new JsonListerner());
        containerProperties.setGroupId("concurrencyGroup");


        DefaultKafkaConsumerFactory<Integer, AccessLog> cf =
                new DefaultKafkaConsumerFactory<>(jsonConsumerConfigs(), new IntegerDeserializer(), new JsonDeserializer<>(AccessLog.class, false));

        ConcurrentMessageListenerContainer<Integer, AccessLog> container =
                new ConcurrentMessageListenerContainer<>(cf, containerProperties);
        container.setConcurrency(3);
        return container;
    }
}
