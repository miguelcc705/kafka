package com.example.transfer.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Contrato JSON del topic transfer.created. */
public record TransferCreatedEvent(
        UUID eventId,
        UUID transferId,
        String fromAccount,
        String toAccount,
        BigDecimal amount,
        String currency,
        Instant createdAt) {
}
