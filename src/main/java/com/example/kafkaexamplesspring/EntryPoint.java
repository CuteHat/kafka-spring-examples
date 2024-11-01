package com.example.kafkaexamplesspring;

import com.example.kafkaexamplesspring.admin.KafkaAdmin;
import com.example.kafkaexamplesspring.consumer.StringEventConsumer;
import com.example.kafkaexamplesspring.producer.PurchaseEventFakerProducerThread;
import com.example.kafkaexamplesspring.producer.PurchaseEventProducer;
import com.example.kafkaexamplesspring.stream.PurchaseEventStreams;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
public class EntryPoint implements CommandLineRunner {
    public static String brokerAddress = "172.20.203.198:9092";
    public static String userTopic = "user-topic";
    public static String priorityPurchaseEventsTopic = "priority-purchase-events";
    public static String lingerSec = "1";

    @Override
    public void run(String... args) throws Exception {
        setUpCluster();
        sendEvents();
        startStream();
    }

    public void sendEvents() throws ExecutionException, InterruptedException, TimeoutException {
        Thread producerThread = new Thread(new PurchaseEventFakerProducerThread(brokerAddress, 5000L, 3));
        producerThread.start();
    }

    public void setUpCluster() throws ExecutionException, InterruptedException {
        KafkaAdmin kafkaAdmin = new KafkaAdmin(brokerAddress);
        kafkaAdmin.removeTopicIfExists(PurchaseEventProducer.TOPIC_NAME);
        kafkaAdmin.createTopic(PurchaseEventProducer.TOPIC_NAME, 1, (short) 1);
        kafkaAdmin.removeTopicIfExists(priorityPurchaseEventsTopic);
        kafkaAdmin.createTopic(priorityPurchaseEventsTopic, 1, (short) 1);
    }

    public void pollEvents() {
        StringEventConsumer eventConsumer = new StringEventConsumer(brokerAddress, "1");
        ConsumerRecords<String, String> consumerRecords = eventConsumer.subscribeAndPoll(userTopic);
        consumerRecords.forEach(record -> log.info("polled the record {}:{}", record.key(), record.value()));
    }

    public void startStream() {
        PurchaseEventStreams purchaseEventStreams = new PurchaseEventStreams(brokerAddress);
//        purchaseEventStreams.createPriorityStream(PurchaseEventProducer.TOPIC_NAME, priorityPurchaseEventsTopic);
        purchaseEventStreams.createProfitCounterStream(PurchaseEventProducer.TOPIC_NAME);
    }
}
