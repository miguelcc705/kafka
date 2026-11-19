package com.example.fraud.service;

import com.example.fraud.domain.Decision;
import com.example.fraud.domain.Evaluation;
import com.example.fraud.messaging.TransferCreatedEvent;
import com.example.fraud.repository.EvaluationRepository;
import com.example.fraud.rules.FraudRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Evalúa transferencias con las reglas de fraude, de forma idempotente por {@code transferId}.
 * <p>
 * No es {@code @Transactional}: cada llamada al repositorio tiene su propia transacción. Así, si dos mensajes del
 * mismo {@code transferId} compiten, la violación de la restricción única no deja una transacción marcada para
 * rollback y se puede leer la evaluación que ganó.
 */
@Service
public class EvaluationService {

    private static final Logger log = LoggerFactory.getLogger(EvaluationService.class);
    private static final int MAX_ACCOUNT_LENGTH = 34;

    private final EvaluationRepository evaluationRepository;
    private final List<FraudRule> rules;
    private final Clock clock;

    /** {@code rules} llega en el orden de {@code @Order}: SAME_ACCOUNT, AMOUNT_EXCEEDED, VELOCITY_EXCEEDED. */
    public EvaluationService(EvaluationRepository evaluationRepository, List<FraudRule> rules, Clock clock) {
        this.evaluationRepository = evaluationRepository;
        this.rules = List.copyOf(rules);
        this.clock = clock;
    }

    /** Devuelve la evaluación de la transferencia: la ya guardada si es un duplicado, o una nueva. */
    public Evaluation evaluate(TransferCreatedEvent event) {
        validate(event);
        Optional<Evaluation> existing = evaluationRepository.findByTransferId(event.transferId());
        if (existing.isPresent()) {
            log.info("La transferencia {} ya fue evaluada ({}); se reenvía la decisión guardada",
                    event.transferId(), existing.get().getDecision());
            return existing.get();
        }

        // Las reglas se evalúan en orden y se detiene en la primera que aplica.
        String reason = rules.stream()
                .filter(rule -> rule.appliesTo(event))
                .map(FraudRule::reason)
                .findFirst()
                .orElse(null);
        Decision decision = reason == null ? Decision.APPROVED : Decision.REJECTED;
        Evaluation evaluation = new Evaluation(UUID.randomUUID(), event.transferId(), event.fromAccount(),
                event.amount(), decision, reason, event.createdAt().truncatedTo(ChronoUnit.MICROS), now());

        try {
            Evaluation saved = evaluationRepository.saveAndFlush(evaluation);
            log.info("Transferencia {} evaluada: {}, motivo {}", event.transferId(), decision, reason);
            return saved;
        } catch (DataIntegrityViolationException ex) {
            // Otro consumidor guardó la misma transferencia entre la lectura y el insert: gana la evaluación guardada.
            return evaluationRepository.findByTransferId(event.transferId())
                    .map(winner -> {
                        log.info("Evaluación concurrente de la transferencia {}; se usa la ya guardada",
                                event.transferId());
                        return winner;
                    })
                    .orElseThrow(() -> ex);
        }
    }

    public Evaluation getByTransferId(UUID transferId) {
        return evaluationRepository.findByTransferId(transferId)
                .orElseThrow(() -> new EvaluationNotFoundException(transferId));
    }

    private static void validate(TransferCreatedEvent event) {
        if (event == null) {
            throw new InvalidEventException("Evento vacío");
        }
        if (event.transferId() == null) {
            throw new InvalidEventException("Falta transferId");
        }
        if (isBlank(event.fromAccount()) || event.fromAccount().length() > MAX_ACCOUNT_LENGTH) {
            throw new InvalidEventException("fromAccount vacío o de más de " + MAX_ACCOUNT_LENGTH + " caracteres");
        }
        if (isBlank(event.toAccount())) {
            throw new InvalidEventException("Falta toAccount");
        }
        if (event.amount() == null) {
            throw new InvalidEventException("Falta amount");
        }
        if (event.createdAt() == null) {
            throw new InvalidEventException("Falta createdAt");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private Instant now() {
        // PostgreSQL guarda microsegundos: se trunca para que el valor en memoria sea igual al guardado.
        return Instant.now(clock).truncatedTo(ChronoUnit.MICROS);
    }
}
