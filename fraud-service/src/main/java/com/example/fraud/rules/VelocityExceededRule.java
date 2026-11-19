package com.example.fraud.rules;

import com.example.fraud.messaging.TransferCreatedEvent;
import com.example.fraud.repository.EvaluationRepository;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Regla 3: demasiadas transferencias de la misma cuenta en poco tiempo.
 * <p>
 * Cuenta las evaluaciones previas (aprobadas o rechazadas) con {@code transferCreatedAt} en
 * {@code [createdAt - ventana, createdAt)}. La ventana usa la hora de creación del evento y no la de procesamiento,
 * así un retraso del consumidor no cambia el resultado. Con el límite de 3 se rechaza la cuarta.
 */
@Component
@Order(3)
public class VelocityExceededRule implements FraudRule {

    public static final String REASON = "VELOCITY_EXCEEDED";

    private final EvaluationRepository evaluationRepository;
    private final FraudRulesProperties properties;

    public VelocityExceededRule(EvaluationRepository evaluationRepository, FraudRulesProperties properties) {
        this.evaluationRepository = evaluationRepository;
        this.properties = properties;
    }

    @Override
    public String reason() {
        return REASON;
    }

    @Override
    public boolean appliesTo(TransferCreatedEvent event) {
        Instant to = event.createdAt();
        Instant from = to.minus(properties.velocityWindow());
        long previous = evaluationRepository.countInWindow(event.fromAccount(), from, to);
        return previous >= properties.velocityMaxTransfers();
    }
}
