package com.example.kafkaexamplesspring.serialize;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class PurchaseEventSerializer implements Serializer<com.example.kafkaexamplesspring.model.PurchaseEvent> {
    private final Gson gson = new Gson();

    @Override
    public byte[] serialize(String topic, com.example.kafkaexamplesspring.model.PurchaseEvent data) {
        return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
    }
}
