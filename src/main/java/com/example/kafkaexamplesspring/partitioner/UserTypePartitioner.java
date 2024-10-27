package com.example.kafkaexamplesspring.partitioner;

import com.example.kafkaexamplesspring.model.User;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

// TODO test if the partitioner works
public class UserTypePartitioner implements Partitioner {

    // In many cases Partitioning with value is slow and not a good idea, in this case I could have simply used
    // UserType as the Key and that's all, but just wanted to create a customer Partitioner 🤓
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (value instanceof User u) {
            return u.getType().ordinal();
        } else {
            throw new RuntimeException("Invalid value type");
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
