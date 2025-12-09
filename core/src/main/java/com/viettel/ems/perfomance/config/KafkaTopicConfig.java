package com.viettel.ems.perfomance.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {
    @Value("${spring.kafka.producer.bootstrap-address}")
    private String bootstrapAddress;
    @Value("${spring.kafka.producer.topics}")
    private String topics;
    @Value("${spring.kafka.producer.num-partitions}")
    private int numPartitions;
    @Value("${spring.kafka.producer.replication-factor}")
    private short replicationFactor;
    @Value("${spring.kafka.consumer.ont.topic}")
    private String topicsONT;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic topic5gAccess() {
        return new NewTopic(topics, numPartitions, replicationFactor);
    }

    @Bean
    public NewTopic topicONT() {
        return new NewTopic(topicsONT, numPartitions, replicationFactor);
    }
}
