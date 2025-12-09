package com.viettel.ems.perfomance.config;

import com.viettel.ems.perfomance.encryption.ZipDeserializer;
import com.viettel.ems.perfomance.object.CounterInfoObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    @Value("${spring.kafka.consumer.bootstrap-address}")
    private String bootstrapAddress;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.enable-auto-commit-config}")
    private boolean enableAutoCommitConfig;

    @Value("${spring.kafka.consumer.auto_commit-interval-ms-config}")
    private String autoCommitIntervalMSConfig;

    @Value("${spring.kafka.consumer.session-timeout-ms-config}")
    private String sessionTimeoutMSConfig;

    @Value("${spring.kafka.consumer.auto-offset-reset-config}")
    private String autoOffsetResetMSConfig;


    @Value("${spring.kafka.consumer.fetch-max-bytes}")
    private String fetchMaxBytes;

    @Value("${spring.kafka.consumer.max-pool-records}")
    private Integer maxPollRecords;

    @Value("${spring.kafka.consumer.ont.group}")
    private String groupIDONT;

    @Value("${spring.kafka.schema.registry.url}")
    private String schemaRegistryURL;

    @Bean
    @Primary
    public ConsumerFactory<String, CounterInfoObject> consumerFactory() {
        StringDeserializer stringDeserializer = null;
        try {
            stringDeserializer = new StringDeserializer();
            Map<String, Object> props = getProps(groupId, JsonDeserializer.class);
            return new DefaultKafkaConsumerFactory<>(
                    props,
                    new StringDeserializer(),
                    new JsonDeserializer<>(CounterInfoObject.class, false)
            );
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        } finally {
            assert stringDeserializer != null;
            stringDeserializer.close();
        }
    }

    @Bean
    @Primary
    public ConcurrentKafkaListenerContainerFactory<String, CounterInfoObject> kafkaListenerContainerFactory() {
        try {
            ConcurrentKafkaListenerContainerFactory<String, CounterInfoObject> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory());
            return factory;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Bean("consumerFactory1")
    public ConsumerFactory<String, String> consumerFactory1() {
        try {
            Map<String, Object> props = getProps(groupIDONT, ZipDeserializer.class);
            return new DefaultKafkaConsumerFactory<>(props);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Bean("kafkaListenerContainerFactory1")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory1(
            @Qualifier("consumerFactory1") ConsumerFactory<String, String> consumerFactory) {
        try {
            ConcurrentKafkaListenerContainerFactory<String, String> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.setBatchListener(true);
            return factory;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }



    private Map<String, Object> getProps(String groupId, Class<?> valueDeserializer) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommitConfig);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMSConfig);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMSConfig);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetMSConfig);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        return props;
    }
}
