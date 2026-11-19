package com.example.fraud.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Resultado de evaluar una transferencia. Su {@code id} se usa como {@code eventId} de transfer.evaluated, así un
 * reenvío de la decisión guardada lleva el mismo eventId.
 */
@Entity
@Table(name = "evaluations")
public class Evaluation {

    @Id
    private UUID id;

    @Column(name = "transfer_id", nullable = false, unique = true)
    private UUID transferId;

    @Column(name = "from_account", nullable = false, length = 34)
    private String fromAccount;

    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private Decision decision;

    @Column(length = 50)
    private String reason;

    @Column(name = "transfer_created_at", nullable = false)
    private Instant transferCreatedAt;

    @Column(name = "evaluated_at", nullable = false)
    private Instant evaluatedAt;

    protected Evaluation() {
        // Requerido por JPA
    }

    public Evaluation(UUID id, UUID transferId, String fromAccount, BigDecimal amount, Decision decision,
                      String reason, Instant transferCreatedAt, Instant evaluatedAt) {
        this.id = id;
        this.transferId = transferId;
        this.fromAccount = fromAccount;
        this.amount = amount;
        this.decision = decision;
        this.reason = reason;
        this.transferCreatedAt = transferCreatedAt;
        this.evaluatedAt = evaluatedAt;
    }

    public UUID getId() {
        return id;
    }

    public UUID getTransferId() {
        return transferId;
    }

    public String getFromAccount() {
        return fromAccount;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public Decision getDecision() {
        return decision;
    }

    public String getReason() {
        return reason;
    }

    public Instant getTransferCreatedAt() {
        return transferCreatedAt;
    }

    public Instant getEvaluatedAt() {
        return evaluatedAt;
    }
}
