package com.example.transfer.api;

import com.example.transfer.domain.TransferStatus;

import java.util.UUID;

public record CreateTransferResponse(UUID id, TransferStatus status) {
}
