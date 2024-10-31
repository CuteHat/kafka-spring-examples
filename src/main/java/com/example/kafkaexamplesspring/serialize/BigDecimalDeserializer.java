package com.example.kafkaexamplesspring.serialize;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.math.BigDecimal;

public class BigDecimalDeserializer implements Deserializer<BigDecimal> {

    @Override
    public BigDecimal deserialize(String topic, byte[] data) {
        if (data == null) return null;
        return new BigDecimal(new String(data));
    }
}
