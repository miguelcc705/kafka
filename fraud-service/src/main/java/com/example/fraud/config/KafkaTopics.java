package com.example.fraud.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics que usa fraud-service. Se declaran todos (los que consume, los que produce y su DLT): KafkaAdmin no falla
 * si el topic ya existe, así que el orden de arranque de los servicios no importa.
 */
@Configuration
public class KafkaTopics {

    public static final String TRANSFER_CREATED = "transfer.created";
    public static final String TRANSFER_EVALUATED = "transfer.evaluated";
    public static final String TRANSFER_CREATED_DLT = TRANSFER_CREATED + KafkaErrorHandlingConfig.DLT_SUFFIX;

    private static final int PARTITIONS = 3;
    private static final int REPLICAS = 1;

    @Bean
    public NewTopic transferCreatedTopic() {
        return TopicBuilder.name(TRANSFER_CREATED).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    public NewTopic transferEvaluatedTopic() {
        return TopicBuilder.name(TRANSFER_EVALUATED).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    public NewTopic transferCreatedDltTopic() {
        return TopicBuilder.name(TRANSFER_CREATED_DLT).partitions(PARTITIONS).replicas(REPLICAS).build();
    }
}
