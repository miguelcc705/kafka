package com.example.transfer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Configuración del publicador del outbox (transfer.outbox.*).
 *
 * @param pollInterval tiempo entre ejecuciones del publicador
 * @param batchSize    máximo de eventos leídos por ejecución
 * @param sendTimeout  espera máxima por la confirmación de Kafka de cada evento
 */
@ConfigurationProperties("transfer.outbox")
public record OutboxProperties(
        @DefaultValue("1s") Duration pollInterval,
        @DefaultValue("100") int batchSize,
        @DefaultValue("10s") Duration sendTimeout) {
}
