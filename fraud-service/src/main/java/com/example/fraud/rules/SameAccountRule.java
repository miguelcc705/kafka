package com.example.fraud.rules;

import com.example.fraud.messaging.TransferCreatedEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/** Regla 1: la cuenta origen es igual a la destino. */
@Component
@Order(1)
public class SameAccountRule implements FraudRule {

    public static final String REASON = "SAME_ACCOUNT";

    @Override
    public String reason() {
        return REASON;
    }

    @Override
    public boolean appliesTo(TransferCreatedEvent event) {
        return event.fromAccount().equals(event.toAccount());
    }
}
