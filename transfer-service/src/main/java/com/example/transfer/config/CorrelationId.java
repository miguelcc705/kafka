package com.example.transfer.config;

import org.slf4j.MDC;

import java.util.UUID;

/**
 * Nombres compartidos del correlationId: header HTTP, clave del MDC y header de Kafka.
 */
public final class CorrelationId {

    public static final String HTTP_HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";
    public static final String KAFKA_HEADER = "correlationId";
    public static final int MAX_LENGTH = 100;

    private CorrelationId() {
    }

    /** Devuelve el correlationId del MDC o genera uno nuevo si no hay (por ejemplo, fuera de una petición HTTP). */
    public static String currentOrNew() {
        String current = MDC.get(MDC_KEY);
        return isValid(current) ? current : newId();
    }

    public static String newId() {
        return UUID.randomUUID().toString();
    }

    public static boolean isValid(String value) {
        return value != null && !value.isBlank() && value.length() <= MAX_LENGTH;
    }
}
