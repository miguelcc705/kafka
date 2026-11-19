package com.example.fraud.service;

import java.util.UUID;

public class EvaluationNotFoundException extends RuntimeException {

    public EvaluationNotFoundException(UUID transferId) {
        super("No existe una evaluación para la transferencia " + transferId);
    }
}
