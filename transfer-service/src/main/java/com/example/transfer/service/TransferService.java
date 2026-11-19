package com.example.transfer.service;

import com.example.transfer.api.CreateTransferRequest;
import com.example.transfer.config.CorrelationId;
import com.example.transfer.config.KafkaTopics;
import com.example.transfer.domain.OutboxEvent;
import com.example.transfer.domain.Transfer;
import com.example.transfer.domain.TransferStatus;
import com.example.transfer.messaging.TransferCreatedEvent;
import com.example.transfer.messaging.TransferEvaluatedEvent;
import com.example.transfer.repository.OutboxEventRepository;
import com.example.transfer.repository.TransferRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Service
public class TransferService {

    private static final Logger log = LoggerFactory.getLogger(TransferService.class);

    private final TransferRepository transferRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final JsonMapper jsonMapper;
    private final Clock clock;

    public TransferService(TransferRepository transferRepository,
                           OutboxEventRepository outboxEventRepository,
                           JsonMapper jsonMapper,
                           Clock clock) {
        this.transferRepository = transferRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.jsonMapper = jsonMapper;
        this.clock = clock;
    }

    /**
     * Guarda la transferencia y su evento en el outbox en la misma transacción: o quedan ambos o ninguno.
     */
    @Transactional
    public Transfer create(CreateTransferRequest request) {
        Instant now = now();
        Transfer transfer = Transfer.pending(UUID.randomUUID(), request.fromAccount(), request.toAccount(),
                request.amount(), request.currency(), now);
        transferRepository.save(transfer);

        UUID eventId = UUID.randomUUID();
        TransferCreatedEvent event = new TransferCreatedEvent(eventId, transfer.getId(), transfer.getFromAccount(),
                transfer.getToAccount(), transfer.getAmount(), transfer.getCurrency(), transfer.getCreatedAt());
        outboxEventRepository.save(new OutboxEvent(eventId, transfer.getId(), KafkaTopics.TRANSFER_CREATED,
                transfer.getFromAccount(), jsonMapper.writeValueAsString(event), CorrelationId.currentOrNew(), now));

        log.info("Transferencia {} creada en PENDING", transfer.getId());
        return transfer;
    }

    @Transactional(readOnly = true)
    public Transfer get(UUID id) {
        return transferRepository.findById(id).orElseThrow(() -> new TransferNotFoundException(id));
    }

    @Transactional(readOnly = true)
    public Page<Transfer> listByFromAccount(String fromAccount, int page, int size) {
        PageRequest pageRequest = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt"));
        return transferRepository.findByFromAccount(fromAccount, pageRequest);
    }

    /**
     * Aplica la decisión de fraude. Es idempotente: si la transferencia ya no está en PENDING (evento duplicado)
     * o no existe, no cambia nada y lo registra en el log.
     *
     * @return true si se actualizó la transferencia
     */
    @Transactional
    public boolean applyDecision(TransferEvaluatedEvent event) {
        TransferStatus newStatus = switch (event.decision()) {
            case APPROVED -> TransferStatus.COMPLETED;
            case REJECTED -> TransferStatus.REJECTED;
        };
        int updated = transferRepository.updateStatusIfCurrent(event.transferId(), TransferStatus.PENDING,
                newStatus, event.reason(), now());
        if (updated == 0) {
            log.info("Evento {} ignorado: la transferencia {} no existe o ya no está en PENDING",
                    event.eventId(), event.transferId());
            return false;
        }
        log.info("Transferencia {} actualizada a {} (motivo: {})", event.transferId(), newStatus, event.reason());
        return true;
    }

    // PostgreSQL guarda microsegundos: truncar evita que el evento y la fila tengan instantes distintos.
    private Instant now() {
        return Instant.now(clock).truncatedTo(ChronoUnit.MICROS);
    }
}
