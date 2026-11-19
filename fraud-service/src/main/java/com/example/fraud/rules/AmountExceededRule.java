package com.example.fraud.rules;

import com.example.fraud.messaging.TransferCreatedEvent;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/** Regla 2: el monto es mayor que el umbral configurado (un monto igual al umbral se aprueba). */
@Component
@Order(2)
public class AmountExceededRule implements FraudRule {

    public static final String REASON = "AMOUNT_EXCEEDED";

    private final FraudRulesProperties properties;

    public AmountExceededRule(FraudRulesProperties properties) {
        this.properties = properties;
    }

    @Override
    public String reason() {
        return REASON;
    }

    @Override
    public boolean appliesTo(TransferCreatedEvent event) {
        return event.amount().compareTo(properties.amountThreshold()) > 0;
    }
}
