package com.example.fraud.config;

import com.example.fraud.messaging.TransferCreatedEvent;
import com.example.fraud.service.InvalidEventException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.Map;

/**
 * Reintentos y DLT del consumidor. Spring Boot conecta este {@link DefaultErrorHandler} a la fábrica de listeners
 * autoconfigurada.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    public static final String DLT_SUFFIX = ".DLT";

    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_INTERVAL_MS = 1_000;
    private static final double MULTIPLIER = 2.0;

    /**
     * 3 reintentos (1 s, 2 s, 4 s) y luego el registro va a {@code <topic>.DLT}. No se reintentan los errores de
     * deserialización (no recuperables por defecto) ni los eventos con datos obligatorios faltantes.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(ProducerFactory<?, ?> producerFactory) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(MAX_RETRIES);
        backOff.setInitialInterval(INITIAL_INTERVAL_MS);
        backOff.setMultiplier(MULTIPLIER);
        // Spring Kafka usa "-dlt" por defecto; el contrato del proyecto es "<topic>.DLT", en la misma partición.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(deadLetterTemplate(producerFactory),
                (record, ex) -> new TopicPartition(record.topic() + DLT_SUFFIX, record.partition()));
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(InvalidEventException.class);
        return errorHandler;
    }

    /**
     * Si el JSON no se pudo deserializar, el recoverer reenvía los bytes originales (byte[]); si falló el
     * procesamiento, reenvía el evento ya deserializado. Por eso la DLT necesita un serializador para cada caso.
     */
    private static KafkaTemplate<Object, Object> deadLetterTemplate(ProducerFactory<?, ?> producerFactory) {
        Map<Class<?>, Serializer<?>> valueSerializers = Map.of(
                byte[].class, new ByteArraySerializer(),
                // Sin type headers: lo toma de spring.json.add.type.headers en la configuración copiada.
                TransferCreatedEvent.class, new JacksonJsonSerializer<>());
        @SuppressWarnings({"unchecked", "rawtypes"})
        Serializer<Object> keySerializer = (Serializer) new StringSerializer();
        @SuppressWarnings({"unchecked", "rawtypes"})
        Serializer<Object> valueSerializer = (Serializer) new DelegatingByTypeSerializer(valueSerializers);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                producerFactory.getConfigurationProperties(), keySerializer, valueSerializer));
    }
}
