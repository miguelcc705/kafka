package com.example.transfer.api;

import com.example.transfer.domain.Transfer;
import com.example.transfer.domain.TransferStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransferResponse(
        UUID id,
        String fromAccount,
        String toAccount,
        BigDecimal amount,
        String currency,
        TransferStatus status,
        String reason,
        Instant createdAt,
        Instant updatedAt) {

    public static TransferResponse from(Transfer transfer) {
        return new TransferResponse(
                transfer.getId(),
                transfer.getFromAccount(),
                transfer.getToAccount(),
                transfer.getAmount(),
                transfer.getCurrency(),
                transfer.getStatus(),
                transfer.getReason(),
                transfer.getCreatedAt(),
                transfer.getUpdatedAt());
    }
}
