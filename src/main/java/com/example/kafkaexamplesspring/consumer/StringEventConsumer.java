package com.example.kafkaexamplesspring.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class StringEventConsumer {
    private final KafkaConsumer<String, String> consumer;

    public StringEventConsumer(String brokerAddr, String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddr);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId); //
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.close()));
    }

    public ConsumerRecords<String, String> subscribeAndPoll(String topic) {
        consumer.subscribe(List.of(topic));
        return consumer.poll(Duration.ofSeconds(10));
    }

}
