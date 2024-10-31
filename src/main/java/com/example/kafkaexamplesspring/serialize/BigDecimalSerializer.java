package com.example.kafkaexamplesspring.serialize;

import org.apache.kafka.common.serialization.Serializer;

import java.math.BigDecimal;

public class BigDecimalSerializer implements Serializer<BigDecimal> {
    @Override
    public byte[] serialize(String topic, BigDecimal data) {
        if (data == null) return null;
        return data.toString().getBytes();
    }
}
