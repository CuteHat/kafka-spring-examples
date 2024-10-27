package com.example.kafkaexamplesspring;

import com.example.kafkaexamplesspring.admin.KafkaAdmin;
import com.example.kafkaexamplesspring.consumer.StringEventConsumer;
import com.example.kafkaexamplesspring.datafaker.PurchaseEventFaker;
import com.example.kafkaexamplesspring.model.PurchaseEvent;
import com.example.kafkaexamplesspring.model.User;
import com.example.kafkaexamplesspring.model.UserType;
import com.example.kafkaexamplesspring.producer.StringEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
public class EntryPoint implements CommandLineRunner {
    public static String brokerAddress = "172.20.203.198:9092";
    public static String userTopic = "user-topic";
    public static String lingerSec = "1";

    @Override
    public void run(String... args) throws Exception {
//        setUpCluster();
//        sendEvents();
//        pollEvents();

        PurchaseEventFaker purchaseEventFaker = new PurchaseEventFaker();
        List<PurchaseEvent> purchaseEvents =
                purchaseEventFaker.generatePurchaseEventsWithInitiatedStatus(200);
        purchaseEvents.forEach(event -> log.info("{}", event));
    }

    public void sendEvents() throws ExecutionException, InterruptedException, TimeoutException {
        StringEventProducer stringEventProducer = new StringEventProducer(brokerAddress, lingerSec);
        User user = new User(1L, "Mr kafka", UserType.REGULAR, new BigDecimal(10));
        stringEventProducer.sendEventSync(userTopic, "test", user.toString());
    }

    public void setUpCluster() throws ExecutionException, InterruptedException {
        KafkaAdmin kafkaAdmin = new KafkaAdmin(brokerAddress);
        kafkaAdmin.removeTopicIfExists(userTopic);
        kafkaAdmin.createTopic(userTopic, 1, (short) 1);
    }

    public void pollEvents() {
        StringEventConsumer eventConsumer = new StringEventConsumer(brokerAddress, "1");
        ConsumerRecords<String, String> consumerRecords = eventConsumer.subscribeAndPoll(userTopic);
        consumerRecords.forEach(record -> log.info("polled the record {}:{}", record.key(), record.value()));
    }
}
