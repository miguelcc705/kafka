package com.example.fraud.unit;

import com.example.fraud.repository.EvaluationRepository;
import com.example.fraud.rules.FraudRulesProperties;
import com.example.fraud.rules.VelocityExceededRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VelocityExceededRuleTest {

    private static final Instant WINDOW_START = TestEvents.CREATED_AT.minusSeconds(60);

    @Mock
    private EvaluationRepository evaluationRepository;

    private VelocityExceededRule rule;

    @BeforeEach
    void setUp() {
        rule = new VelocityExceededRule(evaluationRepository,
                new FraudRulesProperties(new BigDecimal("10000000"), 3, Duration.ofSeconds(60)));
    }

    @Test
    void rejectsTheFourthTransferInsideTheWindow() {
        when(evaluationRepository.countInWindow("ACC-1", WINDOW_START, TestEvents.CREATED_AT)).thenReturn(3L);

        assertThat(rule.appliesTo(TestEvents.transfer("ACC-1", "ACC-2", "100.00"))).isTrue();
        assertThat(rule.reason()).isEqualTo("VELOCITY_EXCEEDED");
    }

    @Test
    void allowsTheThirdTransferInsideTheWindow() {
        when(evaluationRepository.countInWindow("ACC-1", WINDOW_START, TestEvents.CREATED_AT)).thenReturn(2L);

        assertThat(rule.appliesTo(TestEvents.transfer("ACC-1", "ACC-2", "100.00"))).isFalse();
    }

    @Test
    void windowIsBasedOnTheEventCreatedAtNotOnProcessingTime() {
        rule.appliesTo(TestEvents.transfer("ACC-1", "ACC-2", "100.00"));

        // [createdAt - 60 s, createdAt): la ventana se ancla al evento.
        verify(evaluationRepository).countInWindow("ACC-1", WINDOW_START, TestEvents.CREATED_AT);
    }
}
