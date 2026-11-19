package com.example.transfer.messaging;

import java.time.Instant;
import java.util.UUID;

/** Contrato JSON del topic transfer.evaluated. */
public record TransferEvaluatedEvent(
        UUID eventId,
        UUID transferId,
        Decision decision,
        String reason,
        Instant evaluatedAt) {

    public enum Decision {
        APPROVED,
        REJECTED
    }
}
