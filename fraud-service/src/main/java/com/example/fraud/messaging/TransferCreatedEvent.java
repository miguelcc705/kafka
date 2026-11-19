package com.example.fraud.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Evento consumido de transfer.created. El contrato es el JSON; esta clase es la vista propia de fraud-service. */
public record TransferCreatedEvent(
        UUID eventId,
        UUID transferId,
        String fromAccount,
        String toAccount,
        BigDecimal amount,
        String currency,
        Instant createdAt) {
}
