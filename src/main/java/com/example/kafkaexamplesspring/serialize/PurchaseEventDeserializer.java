package com.example.kafkaexamplesspring.serialize;

import com.example.kafkaexamplesspring.model.PurchaseEvent;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class PurchaseEventDeserializer implements Deserializer<PurchaseEvent> {
    private final Gson gson = new Gson();

    @Override
    public PurchaseEvent deserialize(String topic, byte[] data) {
        return gson.fromJson(new String(data),PurchaseEvent.class);
    }
}
