package com.example.fraud.messaging;

import com.example.fraud.config.CorrelationId;
import com.example.fraud.config.KafkaTopics;
import com.example.fraud.domain.Evaluation;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publica la decisión en transfer.evaluated con clave {@code transferId}.
 * <p>
 * El envío es síncrono: si Kafka no confirma, la excepción llega al listener y el mensaje de transfer.created se
 * reintenta. Como la evaluación ya quedó guardada, el reintento reenvía la misma decisión (no hace falta outbox).
 */
@Component
public class EvaluationPublisher {

    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public EvaluationPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(Evaluation evaluation) {
        TransferEvaluatedEvent event = new TransferEvaluatedEvent(evaluation.getId(), evaluation.getTransferId(),
                evaluation.getDecision(), evaluation.getReason(), evaluation.getEvaluatedAt());
        ProducerRecord<String, Object> record = new ProducerRecord<>(KafkaTopics.TRANSFER_EVALUATED,
                evaluation.getTransferId().toString(), event);
        record.headers().add(CorrelationId.KAFKA_HEADER,
                CorrelationId.currentOrNew().getBytes(StandardCharsets.UTF_8));
        try {
            kafkaTemplate.send(record).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Publicación interrumpida de la transferencia "
                    + evaluation.getTransferId(), ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new IllegalStateException("No se pudo publicar la evaluación de la transferencia "
                    + evaluation.getTransferId(), ex);
        }
    }
}
