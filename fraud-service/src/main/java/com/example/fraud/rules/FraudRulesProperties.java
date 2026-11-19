package com.example.fraud.rules;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * Parámetros de las reglas (fraud.rules.*), sobrescribibles con variables de entorno.
 *
 * @param amountThreshold      monto máximo permitido; se rechaza lo que lo supere
 * @param velocityMaxTransfers transferencias previas permitidas dentro de la ventana; se rechaza la siguiente
 * @param velocityWindow       tamaño de la ventana de la regla de velocidad
 */
@Validated
@ConfigurationProperties("fraud.rules")
public record FraudRulesProperties(
        @NotNull @Positive @DefaultValue("10000000") BigDecimal amountThreshold,
        @Positive @DefaultValue("3") int velocityMaxTransfers,
        @NotNull @DefaultValue("60s") Duration velocityWindow) {
}
