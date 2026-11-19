package com.example.fraud.unit;

import com.example.fraud.rules.AmountExceededRule;
import com.example.fraud.rules.FraudRulesProperties;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class AmountExceededRuleTest {

    private final AmountExceededRule rule = new AmountExceededRule(
            new FraudRulesProperties(new BigDecimal("10000000"), 3, Duration.ofSeconds(60)));

    @Test
    void appliesWhenAmountIsAboveTheThreshold() {
        assertThat(rule.appliesTo(TestEvents.transfer("ACC-1", "ACC-2", "10000000.01"))).isTrue();
        assertThat(rule.reason()).isEqualTo("AMOUNT_EXCEEDED");
    }

    @Test
    void amountEqualToTheThresholdIsAllowed() {
        assertThat(rule.appliesTo(TestEvents.transfer("ACC-1", "ACC-2", "10000000.00"))).isFalse();
        assertThat(rule.appliesTo(TestEvents.transfer("ACC-1", "ACC-2", "150000.00"))).isFalse();
    }

    @Test
    void usesTheConfiguredThreshold() {
        AmountExceededRule lowThreshold = new AmountExceededRule(
                new FraudRulesProperties(new BigDecimal("500"), 3, Duration.ofSeconds(60)));

        assertThat(lowThreshold.appliesTo(TestEvents.transfer("ACC-1", "ACC-2", "500.01"))).isTrue();
    }
}
