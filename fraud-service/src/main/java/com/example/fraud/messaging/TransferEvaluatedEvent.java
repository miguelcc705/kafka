package com.example.fraud.messaging;

import com.example.fraud.domain.Decision;

import java.time.Instant;
import java.util.UUID;

/** Evento publicado en transfer.evaluated con la decisión. {@code reason} es nulo si se aprueba. */
public record TransferEvaluatedEvent(
        UUID eventId,
        UUID transferId,
        Decision decision,
        String reason,
        Instant evaluatedAt) {
}
