package com.example.fraud.unit;

import com.example.fraud.messaging.TransferCreatedEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Eventos de prueba para las pruebas unitarias. */
final class TestEvents {

    static final Instant CREATED_AT = Instant.parse("2026-11-19T17:00:00Z");

    private TestEvents() {
    }

    static TransferCreatedEvent transfer(String from, String to, String amount) {
        return new TransferCreatedEvent(UUID.randomUUID(), UUID.randomUUID(), from, to, new BigDecimal(amount),
                "COP", CREATED_AT);
    }
}
