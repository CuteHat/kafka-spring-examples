package com.example.kafkaexamplesspring;

import com.example.kafkaexamplesspring.producer.StringEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EntryPoint implements CommandLineRunner {
    public static String serverAddress = "localhost:9092";
    public static String lingerSec = "1";

    @Override
    public void run(String... args) throws Exception {
        String userTopic = "user-topic";
        StringEventProducer producer = new StringEventProducer(serverAddress, lingerSec);
    }
}
