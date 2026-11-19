package com.example.transfer.messaging;

import com.example.transfer.config.CorrelationId;
import com.example.transfer.config.KafkaTopics;
import com.example.transfer.service.TransferService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Consume las decisiones de fraude y las aplica a la transferencia.
 */
@Component
public class TransferEvaluatedListener {

    private final TransferService transferService;

    public TransferEvaluatedListener(TransferService transferService) {
        this.transferService = transferService;
    }

    @KafkaListener(topics = KafkaTopics.TRANSFER_EVALUATED)
    public void onTransferEvaluated(ConsumerRecord<String, TransferEvaluatedEvent> record) {
        Header header = record.headers().lastHeader(CorrelationId.KAFKA_HEADER);
        if (header != null) {
            MDC.put(CorrelationId.MDC_KEY, new String(header.value(), StandardCharsets.UTF_8));
        }
        try {
            transferService.applyDecision(record.value());
        } finally {
            MDC.remove(CorrelationId.MDC_KEY);
        }
    }
}
