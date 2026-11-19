package com.example.transfer.api;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

public record CreateTransferRequest(
        @NotBlank @Size(max = 34) String fromAccount,
        @NotBlank @Size(max = 34) String toAccount,
        // NUMERIC(19,2): hasta 17 dígitos enteros y 2 decimales
        @NotNull @Positive @Digits(integer = 17, fraction = 2) BigDecimal amount,
        @NotNull @Pattern(regexp = "COP", message = "solo se acepta COP") String currency) {
}
