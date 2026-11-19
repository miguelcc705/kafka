package com.example.fraud.api;

import com.example.fraud.domain.Decision;
import com.example.fraud.domain.Evaluation;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record EvaluationResponse(
        UUID id,
        UUID transferId,
        String fromAccount,
        BigDecimal amount,
        Decision decision,
        String reason,
        Instant transferCreatedAt,
        Instant evaluatedAt) {

    public static EvaluationResponse from(Evaluation evaluation) {
        return new EvaluationResponse(evaluation.getId(), evaluation.getTransferId(), evaluation.getFromAccount(),
                evaluation.getAmount(), evaluation.getDecision(), evaluation.getReason(),
                evaluation.getTransferCreatedAt(), evaluation.getEvaluatedAt());
    }
}
