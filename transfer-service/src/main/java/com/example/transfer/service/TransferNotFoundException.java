package com.example.transfer.service;

import java.util.UUID;

public class TransferNotFoundException extends RuntimeException {

    public TransferNotFoundException(UUID id) {
        super("No existe la transferencia " + id);
    }
}
