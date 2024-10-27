package com.example.kafkaexamplesspring.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class KafkaAdmin {
    private Properties properties;
    private Admin admin;

    public KafkaAdmin(String server) throws ExecutionException, InterruptedException {
        properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(AdminClientConfig.RETRIES_CONFIG, "3");
         properties.setProperty(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        properties.setProperty(AdminClientConfig.RETRY_BACKOFF_MAX_MS_CONFIG, "1000");

        admin = Admin.create(properties);
        DescribeClusterResult describeClusterResult = admin.describeCluster();
        KafkaFuture<String> clusterIdFuture = describeClusterResult.clusterId();
        log.info("Cluster ID: {}", clusterIdFuture.get());
        log.info("Authorized OPS: {}", describeClusterResult.authorizedOperations().get());
        log.info("Node: {}", describeClusterResult.controller().get());
    }

    public void createTopic(String topicName, int numberOfPartitions, short replicationFactor) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor);
        CreateTopicsResult result = admin.createTopics(List.of(newTopic));
        result.all().get();
        log.info("Topic: {} created successfully", topicName);
    }

    public boolean topicExists(String topicName) throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult = admin.describeTopics(List.of(topicName));
        KafkaFuture<Map<String, TopicDescription>> mapKafkaFuture = describeTopicsResult.allTopicNames();
        if (mapKafkaFuture == null) return false;

        try {
            mapKafkaFuture.get();
        } catch (Exception exception) {
            Class<? extends Throwable> causedBy = exception.getCause().getClass();
            if (causedBy == UnknownTopicOrPartitionException.class) {
                return false;
            }
            throw exception;
        }
        return true;
    }

    public void removeTopicIfExists(String topicName) throws ExecutionException, InterruptedException {
        if (topicExists(topicName)) {
            DeleteTopicsResult deleteTopicsResult = admin.deleteTopics(List.of(topicName));
            deleteTopicsResult.all().get();
            log.info("Topic: {} marked for deletion succesfully", topicName);
        } else {
            log.info("Topic: {} does not exist, no need to invoke delete", topicName);
        }
    }
}