package com.nick.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class KafkaApplication implements ApplicationRunner {

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) {
        // ProducerFactory
        Map<String , Object> producerProps = new HashMap<>();
        producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:3333");
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer());
        producerFactory.setTransactionIdPrefix("txn-");

        // ConsumerFactory
        Map<String , Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:3333");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer());

        // KafkaListenerContainerFactory
        ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        concurrentKafkaListenerContainerFactory.getContainerProperties().setTransactionManager(new KafkaTransactionManager<>(producerFactory));
        concurrentKafkaListenerContainerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory);
        concurrentKafkaListenerContainerFactory.setBatchListener(true);

        // Create container
        SimpleBatchListener listener = new SimpleBatchListener(new KafkaTemplate<>(producerFactory));
        ConcurrentMessageListenerContainer<String, String> container = concurrentKafkaListenerContainerFactory.createContainer("input-topic");
        container.setupMessageListener(listener);

        // Start
        container.start();
    }
}
