package com.example.kafkaexamplesspring.producer;

import com.example.kafkaexamplesspring.serialize.PurchaseEventSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PurchaseEventProducer {
    private final Producer<String, PurchaseEventSerializer> producer;

    public PurchaseEventProducer(String server) {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PurchaseEventSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        producer = new KafkaProducer<>(properties);
    }

    public RecordMetadata sendEventSync(String topic, String key, PurchaseEventSerializer value) throws ExecutionException, InterruptedException, TimeoutException {
        ProducerRecord<String, PurchaseEventSerializer> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> response = producer.send(record);

        return response.get(2, TimeUnit.SECONDS);
    }

}