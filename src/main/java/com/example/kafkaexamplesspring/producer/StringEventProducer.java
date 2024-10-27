package com.example.kafkaexamplesspring.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StringEventProducer {
    private Producer<String, String> producer;

    public StringEventProducer(String server, String lingerS) {
        String serializer = "org.apache.kafka.common.serialization.StringSerializer";
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server); // can be multiple broker addresses
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // can ACK from the leader broker to consider event sent can also be "all"
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Idempotence producers guarantee no duplication for messages
        // The batch is sent after linger time or if Batch size is full
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, lingerS); // how much time to wait before sending a message batch
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32000"); // Batch size in Bytes ( Sum(Message * Message_Size) )
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "500");
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "1000");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3"); // how many times to retry sending an event
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100"); // backoff period between retries
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, "1000"); // The backoff time increases, this is the upper limit
//        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG) com.example.kafkaexamplesspring.partitioner.UserTypePartitioner

        producer = new KafkaProducer<>(properties);
    }

    // TODO think about closing producer connection
    public RecordMetadata sendEventSync(String topic, String key, String value) throws ExecutionException, InterruptedException, TimeoutException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> response = producer.send(record);

        return response.get(2, TimeUnit.SECONDS);
    }

    public void sendEventAsync(String topic, String key, String value, Callback callback) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, callback);
    }
}
