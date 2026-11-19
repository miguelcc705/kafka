package com.example.transfer.repository;

import com.example.transfer.domain.Transfer;
import com.example.transfer.domain.TransferStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface TransferRepository extends JpaRepository<Transfer, UUID> {

    Page<Transfer> findByFromAccount(String fromAccount, Pageable pageable);

    /**
     * Aplica la decisión solo si la transferencia sigue en PENDING. Devuelve 0 si ya estaba resuelta o no existe,
     * lo que hace idempotente el procesamiento de eventos duplicados.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update Transfer t
               set t.status = :status, t.reason = :reason, t.updatedAt = :updatedAt
             where t.id = :id and t.status = :expected
            """)
    int updateStatusIfCurrent(@Param("id") UUID id,
                              @Param("expected") TransferStatus expected,
                              @Param("status") TransferStatus status,
                              @Param("reason") String reason,
                              @Param("updatedAt") Instant updatedAt);
}
