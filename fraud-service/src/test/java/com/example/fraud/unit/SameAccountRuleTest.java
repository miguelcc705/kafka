package com.example.fraud.unit;

import com.example.fraud.rules.SameAccountRule;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SameAccountRuleTest {

    private final SameAccountRule rule = new SameAccountRule();

    @Test
    void appliesWhenOriginEqualsDestination() {
        assertThat(rule.appliesTo(TestEvents.transfer("ACC-1", "ACC-1", "100.00"))).isTrue();
        assertThat(rule.reason()).isEqualTo("SAME_ACCOUNT");
    }

    @Test
    void doesNotApplyToDifferentAccounts() {
        assertThat(rule.appliesTo(TestEvents.transfer("ACC-1", "ACC-2", "100.00"))).isFalse();
    }
}
