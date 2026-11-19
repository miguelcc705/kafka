package com.example.fraud.service;

/**
 * El evento llegó como JSON válido pero le faltan datos obligatorios. Reintentarlo daría el mismo resultado, así
 * que el manejador de errores lo envía directo a la DLT.
 */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }
}
