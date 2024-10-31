package com.example.kafkaexamplesspring.stream;

import com.example.kafkaexamplesspring.model.PurchaseEvent;
import com.example.kafkaexamplesspring.model.UserType;
import com.example.kafkaexamplesspring.serialize.BigDecimalDeserializer;
import com.example.kafkaexamplesspring.serialize.BigDecimalSerializer;
import com.example.kafkaexamplesspring.serialize.PurchaseEventDeserializer;
import com.example.kafkaexamplesspring.serialize.PurchaseEventSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.Properties;

@Slf4j
public class PurchaseEventStreams {
    private final Properties streamDefaultProps;
    private final Serde<BigDecimal> bigDecimalSerde = Serdes.serdeFrom(new BigDecimalSerializer(), new BigDecimalDeserializer());
    private final Serde<PurchaseEvent> purchaseEventSerde = Serdes.serdeFrom(new PurchaseEventSerializer(), new PurchaseEventDeserializer());

    public PurchaseEventStreams(String broker) {
        streamDefaultProps = new Properties();
        streamDefaultProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        streamDefaultProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "purchase-events-processor-app");
        streamDefaultProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamDefaultProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, purchaseEventSerde.getClass());
    }

    public void createPriorityStream(String from, String to) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(from)
                .filter((key, value) -> Objects.equals(key, UserType.PREMIUM.name()))
                .to(to);

        final Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, streamDefaultProps);
        streams.start();
        System.out.println("I do not know what is happening");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public void createProfitCounterStream(String targetTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, PurchaseEvent> stream = builder.stream(
                targetTopic,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(new PurchaseEventSerializer(), new PurchaseEventDeserializer()))
        );

        KTable<String, BigDecimal> ktable = stream.groupByKey()
                .aggregate(
                        () -> new BigDecimal(0),
                        (key, value, aggregate) -> aggregate.add(value.getProduct().getPrice()),
                        Materialized.<String, BigDecimal, KeyValueStore<Bytes, byte[]>>as("profit-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(bigDecimalSerde)
                );

        KafkaStreams kStreams = new KafkaStreams(builder.build(), streamDefaultProps);
        kStreams.setUncaughtExceptionHandler(throwable -> {
            log.error("Stream Error: " + throwable);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        kStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
    }

}
