package com.example.fraud.unit;

import com.example.fraud.config.CorrelationId;
import com.example.fraud.config.KafkaTopics;
import com.example.fraud.domain.Decision;
import com.example.fraud.domain.Evaluation;
import com.example.fraud.messaging.EvaluationPublisher;
import com.example.fraud.messaging.TransferEvaluatedEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EvaluationPublisherTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    void publishesTheDecisionKeyedByTransferIdWithCorrelationId() {
        Evaluation evaluation = evaluation();
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(sent());
        MDC.put(CorrelationId.MDC_KEY, "corr-1");

        new EvaluationPublisher(kafkaTemplate).publish(evaluation);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, Object> record = captor.getValue();
        assertThat(record.topic()).isEqualTo(KafkaTopics.TRANSFER_EVALUATED);
        assertThat(record.key()).isEqualTo(evaluation.getTransferId().toString());
        assertThat(record.value()).isEqualTo(new TransferEvaluatedEvent(evaluation.getId(),
                evaluation.getTransferId(), Decision.REJECTED, "AMOUNT_EXCEEDED", evaluation.getEvaluatedAt()));
        assertThat(new String(record.headers().lastHeader(CorrelationId.KAFKA_HEADER).value(),
                StandardCharsets.UTF_8)).isEqualTo("corr-1");
    }

    @Test
    void failsWhenKafkaDoesNotConfirmSoTheMessageIsRetried() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        assertThatThrownBy(() -> new EvaluationPublisher(kafkaTemplate).publish(evaluation()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No se pudo publicar");
    }

    private static Evaluation evaluation() {
        return new Evaluation(UUID.randomUUID(), UUID.randomUUID(), "ACC-1", new BigDecimal("20000000.00"),
                Decision.REJECTED, "AMOUNT_EXCEEDED", Instant.parse("2026-11-19T17:00:00Z"),
                Instant.parse("2026-11-19T17:00:01Z"));
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<SendResult<String, Object>> sent() {
        return CompletableFuture.completedFuture(mock(SendResult.class));
    }
}
