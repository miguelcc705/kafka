package com.example.transfer.unit;

import com.example.transfer.config.CorrelationId;
import com.example.transfer.config.KafkaTopics;
import com.example.transfer.config.OutboxProperties;
import com.example.transfer.domain.OutboxEvent;
import com.example.transfer.messaging.OutboxPublisher;
import com.example.transfer.repository.OutboxEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    private static final Instant NOW = Instant.parse("2026-11-19T17:00:05Z");

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        OutboxProperties properties = new OutboxProperties(Duration.ofSeconds(1), 100, Duration.ofSeconds(5));
        publisher = new OutboxPublisher(outboxEventRepository, kafkaTemplate, JsonMapper.builder().build(),
                properties, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void publishesPendingEventsInOrderAndMarksThemPublished() {
        OutboxEvent first = outboxEvent("ACC-1", "corr-1");
        OutboxEvent second = outboxEvent("ACC-2", "corr-2");
        when(outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(Limit.of(100)))
                .thenReturn(List.of(first, second));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(sent());

        publisher.publishPending();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate, times(2)).send(captor.capture());
        List<ProducerRecord<String, Object>> records = captor.getAllValues();

        ProducerRecord<String, Object> record = records.getFirst();
        assertThat(record.topic()).isEqualTo(KafkaTopics.TRANSFER_CREATED);
        assertThat(record.key()).isEqualTo("ACC-1");
        // Lo que realmente viaja: el payload tal cual, sin reformatear el monto ni escaparlo como texto.
        try (JacksonJsonSerializer<Object> serializer = new JacksonJsonSerializer<>().noTypeInfo()) {
            String wire = new String(serializer.serialize(record.topic(), record.value()), StandardCharsets.UTF_8);
            assertThat(wire).isEqualTo(first.getPayload()).contains("\"amount\":1000.00");
        }
        Header header = record.headers().lastHeader(CorrelationId.KAFKA_HEADER);
        assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo("corr-1");
        assertThat(records.get(1).key()).isEqualTo("ACC-2");

        verify(outboxEventRepository).markPublished(first.getId(), NOW);
        verify(outboxEventRepository).markPublished(second.getId(), NOW);
    }

    @Test
    void stopsTheBatchWhenSendingFailsAndLeavesEventsPending() {
        OutboxEvent first = outboxEvent("ACC-1", "corr-1");
        OutboxEvent second = outboxEvent("ACC-2", "corr-2");
        when(outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(Limit.of(100)))
                .thenReturn(List.of(first, second));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        publisher.publishPending();

        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
        verify(outboxEventRepository, never()).markPublished(any(), any());
    }

    @Test
    void retriesPendingEventOnNextRun() {
        OutboxEvent event = outboxEvent("ACC-1", null);
        when(outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(Limit.of(100)))
                .thenReturn(List.of(event));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")))
                .thenReturn(sent());

        publisher.publishPending();
        publisher.publishPending();

        verify(kafkaTemplate, times(2)).send(argThat((ProducerRecord<String, Object> r) ->
                r.headers().lastHeader(CorrelationId.KAFKA_HEADER) == null));
        verify(outboxEventRepository, times(1)).markPublished(event.getId(), NOW);
    }

    private static OutboxEvent outboxEvent(String fromAccount, String correlationId) {
        UUID id = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();
        String payload = """
                {"eventId":"%s","transferId":"%s","fromAccount":"%s","toAccount":"ACC-9",\
                "amount":1000.00,"currency":"COP","createdAt":"2026-11-19T17:00:00Z"}"""
                .formatted(id, transferId, fromAccount);
        return new OutboxEvent(id, transferId, KafkaTopics.TRANSFER_CREATED, fromAccount, payload, correlationId,
                Instant.parse("2026-11-19T17:00:00Z"));
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<SendResult<String, Object>> sent() {
        return CompletableFuture.completedFuture(mock(SendResult.class));
    }
}
