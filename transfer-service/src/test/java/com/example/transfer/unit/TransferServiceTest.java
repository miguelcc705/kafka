package com.example.transfer.unit;

import com.example.transfer.api.CreateTransferRequest;
import com.example.transfer.config.CorrelationId;
import com.example.transfer.config.KafkaTopics;
import com.example.transfer.domain.OutboxEvent;
import com.example.transfer.domain.Transfer;
import com.example.transfer.domain.TransferStatus;
import com.example.transfer.messaging.TransferEvaluatedEvent;
import com.example.transfer.messaging.TransferEvaluatedEvent.Decision;
import com.example.transfer.repository.OutboxEventRepository;
import com.example.transfer.repository.TransferRepository;
import com.example.transfer.service.TransferNotFoundException;
import com.example.transfer.service.TransferService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransferServiceTest {

    private static final Instant NOW = Instant.parse("2026-11-19T17:00:00Z");

    @Mock
    private TransferRepository transferRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    private TransferService service;

    @BeforeEach
    void setUp() {
        service = new TransferService(transferRepository, outboxEventRepository, jsonMapper,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    void createSavesPendingTransferAndOutboxEventWithCorrelationId() {
        MDC.put(CorrelationId.MDC_KEY, "corr-123");
        CreateTransferRequest request =
                new CreateTransferRequest("ACC-1", "ACC-2", new BigDecimal("150000.00"), "COP");

        Transfer transfer = service.create(request);

        assertThat(transfer.getStatus()).isEqualTo(TransferStatus.PENDING);
        assertThat(transfer.getCreatedAt()).isEqualTo(NOW);
        verify(transferRepository).save(transfer);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent outbox = captor.getValue();
        assertThat(outbox.getTopic()).isEqualTo(KafkaTopics.TRANSFER_CREATED);
        assertThat(outbox.getMessageKey()).isEqualTo("ACC-1");
        assertThat(outbox.getAggregateId()).isEqualTo(transfer.getId());
        assertThat(outbox.getCorrelationId()).isEqualTo("corr-123");
        assertThat(outbox.getPublishedAt()).isNull();

        JsonNode payload = jsonMapper.readTree(outbox.getPayload());
        assertThat(payload.get("eventId").asString()).isEqualTo(outbox.getId().toString());
        assertThat(payload.get("transferId").asString()).isEqualTo(transfer.getId().toString());
        assertThat(payload.get("fromAccount").asString()).isEqualTo("ACC-1");
        assertThat(payload.get("toAccount").asString()).isEqualTo("ACC-2");
        assertThat(payload.get("amount").decimalValue()).isEqualByComparingTo("150000.00");
        assertThat(payload.get("currency").asString()).isEqualTo("COP");
        assertThat(payload.get("createdAt").asString()).isEqualTo("2026-11-19T17:00:00Z");
    }

    @Test
    void createGeneratesCorrelationIdWhenMdcIsEmpty() {
        service.create(new CreateTransferRequest("ACC-1", "ACC-2", BigDecimal.TEN, "COP"));

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getCorrelationId()).isNotBlank();
    }

    @Test
    void approvedDecisionCompletesPendingTransfer() {
        UUID id = UUID.randomUUID();
        when(transferRepository.updateStatusIfCurrent(id, TransferStatus.PENDING, TransferStatus.COMPLETED, null, NOW))
                .thenReturn(1);

        boolean updated = service.applyDecision(evaluated(id, Decision.APPROVED, null));

        assertThat(updated).isTrue();
    }

    @Test
    void rejectedDecisionRejectsPendingTransferWithReason() {
        UUID id = UUID.randomUUID();
        when(transferRepository.updateStatusIfCurrent(id, TransferStatus.PENDING, TransferStatus.REJECTED,
                "AMOUNT_EXCEEDED", NOW)).thenReturn(1);

        boolean updated = service.applyDecision(evaluated(id, Decision.REJECTED, "AMOUNT_EXCEEDED"));

        assertThat(updated).isTrue();
    }

    @Test
    void decisionIsIgnoredWhenTransferIsNoLongerPending() {
        UUID id = UUID.randomUUID();
        when(transferRepository.updateStatusIfCurrent(eq(id), eq(TransferStatus.PENDING), any(), any(), any()))
                .thenReturn(0);

        boolean updated = service.applyDecision(evaluated(id, Decision.REJECTED, "SAME_ACCOUNT"));

        assertThat(updated).isFalse();
    }

    @Test
    void getThrowsWhenTransferDoesNotExist() {
        UUID id = UUID.randomUUID();
        when(transferRepository.findById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.get(id))
                .isInstanceOf(TransferNotFoundException.class)
                .hasMessageContaining(id.toString());
    }

    private static TransferEvaluatedEvent evaluated(UUID transferId, Decision decision, String reason) {
        return new TransferEvaluatedEvent(UUID.randomUUID(), transferId, decision, reason, NOW);
    }
}
