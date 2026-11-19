package com.example.fraud.messaging;

import com.example.fraud.config.CorrelationId;
import com.example.fraud.config.KafkaTopics;
import com.example.fraud.domain.Evaluation;
import com.example.fraud.service.EvaluationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Consume transfer.created, evalúa la transferencia y publica la decisión. Los duplicados reenvían la decisión
 * guardada sin volver a evaluar.
 */
@Component
public class TransferCreatedListener {

    private final EvaluationService evaluationService;
    private final EvaluationPublisher evaluationPublisher;

    public TransferCreatedListener(EvaluationService evaluationService, EvaluationPublisher evaluationPublisher) {
        this.evaluationService = evaluationService;
        this.evaluationPublisher = evaluationPublisher;
    }

    @KafkaListener(topics = KafkaTopics.TRANSFER_CREATED)
    public void onTransferCreated(ConsumerRecord<String, TransferCreatedEvent> record) {
        MDC.put(CorrelationId.MDC_KEY, correlationIdOf(record));
        try {
            Evaluation evaluation = evaluationService.evaluate(record.value());
            evaluationPublisher.publish(evaluation);
        } finally {
            MDC.remove(CorrelationId.MDC_KEY);
        }
    }

    /** El del header si es válido; si no, uno nuevo para que los logs y transfer.evaluated lleven el mismo. */
    private static String correlationIdOf(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(CorrelationId.KAFKA_HEADER);
        String value = header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
        return CorrelationId.isValid(value) ? value : CorrelationId.newId();
    }
}
