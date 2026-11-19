package com.example.transfer.messaging;

import com.example.transfer.config.CorrelationId;
import com.example.transfer.config.OutboxProperties;
import com.example.transfer.domain.OutboxEvent;
import com.example.transfer.repository.OutboxEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.util.RawValue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Publica en Kafka los eventos pendientes del outbox, en orden de creación.
 * <p>
 * Si un envío falla se corta el lote: el evento sigue pendiente y se reintenta en la siguiente ejecución,
 * sin adelantar eventos posteriores. Si Kafka confirma pero falla el marcado, el evento se reenvía
 * (al menos una vez): los consumidores toleran duplicados.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final JsonMapper jsonMapper;
    private final OutboxProperties properties;
    private final Clock clock;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                           KafkaTemplate<String, Object> kafkaTemplate,
                           JsonMapper jsonMapper,
                           OutboxProperties properties,
                           Clock clock) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${transfer.outbox.poll-interval:1s}")
    public void publishPending() {
        List<OutboxEvent> batch =
                outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(Limit.of(properties.batchSize()));
        for (OutboxEvent event : batch) {
            if (!publish(event)) {
                return;
            }
        }
    }

    private boolean publish(OutboxEvent event) {
        if (event.getCorrelationId() != null) {
            MDC.put(CorrelationId.MDC_KEY, event.getCorrelationId());
        }
        try {
            // El payload ya es JSON: se envía como valor crudo para que el serializador lo escriba tal cual, sin
            // escaparlo como texto ni reinterpretarlo (releerlo como árbol convertiría 150000.00 en el double 150000.0).
            JsonNode payload = jsonMapper.getNodeFactory().rawValueNode(new RawValue(event.getPayload()));
            ProducerRecord<String, Object> record =
                    new ProducerRecord<>(event.getTopic(), event.getMessageKey(), payload);
            if (event.getCorrelationId() != null) {
                record.headers().add(CorrelationId.KAFKA_HEADER,
                        event.getCorrelationId().getBytes(StandardCharsets.UTF_8));
            }
            kafkaTemplate.send(record).get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            outboxEventRepository.markPublished(event.getId(), Instant.now(clock));
            log.debug("Evento {} publicado en {}", event.getId(), event.getTopic());
            return true;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("Publicación del evento {} interrumpida", event.getId());
            return false;
        } catch (Exception ex) {
            log.warn("No se pudo publicar el evento {} en {}; se reintentará: {}",
                    event.getId(), event.getTopic(), ex.getMessage());
            return false;
        } finally {
            MDC.remove(CorrelationId.MDC_KEY);
        }
    }
}
