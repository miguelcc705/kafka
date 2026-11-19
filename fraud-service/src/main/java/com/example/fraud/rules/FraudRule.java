package com.example.fraud.rules;

import com.example.fraud.messaging.TransferCreatedEvent;

/**
 * Una regla de fraude. Las reglas son beans ordenados con {@code @Order}: Spring las inyecta como lista en ese
 * orden y se guarda el motivo de la primera que aplique.
 */
public interface FraudRule {

    /** Motivo de rechazo que se guarda si la regla aplica (por ejemplo, {@code SAME_ACCOUNT}). */
    String reason();

    /** {@code true} si la transferencia debe rechazarse por esta regla. */
    boolean appliesTo(TransferCreatedEvent event);
}
