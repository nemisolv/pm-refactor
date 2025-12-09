package com.viettel.ems.perfomance.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.viettel.ems.perfomance.object.CounterObject;
import com.viettel.ems.perfomance.object.clickhouse.NewFormatCounterObject;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {
    @Value("${spring.kafka.producer.bootstrap-address}")
    private String bootstrapAddress;

    @Value("${spring.kafka.producer.max-request-size}")
    private String maxRequestSize;

    @Bean
    public ProducerFactory<String, List<CounterObject>> producerFactory() {
        return new DefaultKafkaProducerFactory<>(createConfig());
    }

    @Bean
    public KafkaTemplate<String, List<CounterObject>> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ProducerFactory<String, NewFormatCounterObject> producerNewFormatFactory() {
        return new DefaultKafkaProducerFactory<>(createConfig());
    }

    @Bean
    public KafkaTemplate<String, NewFormatCounterObject> kafkaNewFormatTemplate() {
        return new KafkaTemplate<>(producerNewFormatFactory());
    }

    @Bean
    public ProducerFactory<String, JsonNode> producerClickhouseFactory() {
        return new DefaultKafkaProducerFactory<>(createConfig());
    }

    @Bean
    public KafkaTemplate<String, JsonNode> kafkaClickhouseTemplate() {
        return new KafkaTemplate<>(producerClickhouseFactory());
    }

    private Map<String, Object> createConfig() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        configProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return  configProps;
    }


}
