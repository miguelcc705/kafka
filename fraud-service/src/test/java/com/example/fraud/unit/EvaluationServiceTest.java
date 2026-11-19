package com.example.fraud.unit;

import com.example.fraud.domain.Decision;
import com.example.fraud.domain.Evaluation;
import com.example.fraud.messaging.TransferCreatedEvent;
import com.example.fraud.repository.EvaluationRepository;
import com.example.fraud.rules.FraudRule;
import com.example.fraud.service.EvaluationNotFoundException;
import com.example.fraud.service.EvaluationService;
import com.example.fraud.service.InvalidEventException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EvaluationServiceTest {

    private static final Instant NOW = Instant.parse("2026-11-19T17:00:01.123456789Z");

    @Mock
    private EvaluationRepository evaluationRepository;

    @Mock
    private FraudRule firstRule;

    @Mock
    private FraudRule secondRule;

    private EvaluationService service;

    @BeforeEach
    void setUp() {
        lenient().when(firstRule.reason()).thenReturn("FIRST");
        lenient().when(secondRule.reason()).thenReturn("SECOND");
        lenient().when(evaluationRepository.saveAndFlush(any(Evaluation.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        service = new EvaluationService(evaluationRepository, List.of(firstRule, secondRule),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void approvesWhenNoRuleApplies() {
        TransferCreatedEvent event = TestEvents.transfer("ACC-1", "ACC-2", "150000.00");

        Evaluation evaluation = service.evaluate(event);

        assertThat(evaluation.getDecision()).isEqualTo(Decision.APPROVED);
        assertThat(evaluation.getReason()).isNull();
        assertThat(evaluation.getTransferId()).isEqualTo(event.transferId());
        assertThat(evaluation.getFromAccount()).isEqualTo("ACC-1");
        assertThat(evaluation.getAmount()).isEqualByComparingTo("150000.00");
        assertThat(evaluation.getTransferCreatedAt()).isEqualTo(TestEvents.CREATED_AT);
        assertThat(evaluation.getEvaluatedAt()).isEqualTo(Instant.parse("2026-11-19T17:00:01.123456Z"));
        verify(evaluationRepository).saveAndFlush(evaluation);
    }

    @Test
    void storesTheReasonOfTheFirstRuleThatAppliesAndSkipsTheRest() {
        TransferCreatedEvent event = TestEvents.transfer("ACC-1", "ACC-2", "100.00");
        when(firstRule.appliesTo(event)).thenReturn(true);

        Evaluation evaluation = service.evaluate(event);

        assertThat(evaluation.getDecision()).isEqualTo(Decision.REJECTED);
        assertThat(evaluation.getReason()).isEqualTo("FIRST");
        verify(secondRule, never()).appliesTo(any());
    }

    @Test
    void usesALaterRuleWhenEarlierOnesDoNotApply() {
        TransferCreatedEvent event = TestEvents.transfer("ACC-1", "ACC-2", "100.00");
        when(secondRule.appliesTo(event)).thenReturn(true);

        Evaluation evaluation = service.evaluate(event);

        assertThat(evaluation.getDecision()).isEqualTo(Decision.REJECTED);
        assertThat(evaluation.getReason()).isEqualTo("SECOND");
    }

    @Test
    void duplicateReturnsTheStoredEvaluationWithoutEvaluatingAgain() {
        TransferCreatedEvent event = TestEvents.transfer("ACC-1", "ACC-2", "100.00");
        Evaluation stored = stored(event, Decision.REJECTED, "FIRST");
        when(evaluationRepository.findByTransferId(event.transferId())).thenReturn(Optional.of(stored));

        Evaluation evaluation = service.evaluate(event);

        assertThat(evaluation).isSameAs(stored);
        verifyNoInteractions(firstRule, secondRule);
        verify(evaluationRepository, never()).saveAndFlush(any());
    }

    @Test
    void concurrentInsertOfTheSameTransferReturnsTheWinner() {
        TransferCreatedEvent event = TestEvents.transfer("ACC-1", "ACC-2", "100.00");
        Evaluation winner = stored(event, Decision.APPROVED, null);
        when(evaluationRepository.findByTransferId(event.transferId()))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(winner));
        when(evaluationRepository.saveAndFlush(any(Evaluation.class)))
                .thenThrow(new DataIntegrityViolationException("uk_evaluations_transfer_id"));

        assertThat(service.evaluate(event)).isSameAs(winner);
    }

    @Test
    void integrityErrorWithoutAStoredEvaluationIsRethrown() {
        TransferCreatedEvent event = TestEvents.transfer("ACC-1", "ACC-2", "100.00");
        DataIntegrityViolationException error = new DataIntegrityViolationException("numeric overflow");
        when(evaluationRepository.saveAndFlush(any(Evaluation.class))).thenThrow(error);

        assertThatThrownBy(() -> service.evaluate(event)).isSameAs(error);
    }

    @Test
    void rejectsEventsWithMissingRequiredFields() {
        TransferCreatedEvent noAmount = new TransferCreatedEvent(UUID.randomUUID(), UUID.randomUUID(),
                "ACC-1", "ACC-2", null, "COP", TestEvents.CREATED_AT);
        TransferCreatedEvent noTransferId = new TransferCreatedEvent(UUID.randomUUID(), null,
                "ACC-1", "ACC-2", BigDecimal.TEN, "COP", TestEvents.CREATED_AT);
        TransferCreatedEvent longAccount = new TransferCreatedEvent(UUID.randomUUID(), UUID.randomUUID(),
                "A".repeat(35), "ACC-2", BigDecimal.TEN, "COP", TestEvents.CREATED_AT);

        assertThatThrownBy(() -> service.evaluate(noAmount)).isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> service.evaluate(noTransferId)).isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> service.evaluate(longAccount)).isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> service.evaluate(null)).isInstanceOf(InvalidEventException.class);
        verifyNoInteractions(evaluationRepository);
    }

    @Test
    void getByTransferIdThrowsWhenMissing() {
        UUID transferId = UUID.randomUUID();

        assertThatThrownBy(() -> service.getByTransferId(transferId))
                .isInstanceOf(EvaluationNotFoundException.class);
    }

    private static Evaluation stored(TransferCreatedEvent event, Decision decision, String reason) {
        return new Evaluation(UUID.randomUUID(), event.transferId(), event.fromAccount(), event.amount(),
                decision, reason, event.createdAt(), NOW);
    }
}
