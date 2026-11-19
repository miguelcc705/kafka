package com.example.fraud.repository;

import com.example.fraud.domain.Evaluation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface EvaluationRepository extends JpaRepository<Evaluation, UUID> {

    Optional<Evaluation> findByTransferId(UUID transferId);

    /** Evaluaciones (aprobadas y rechazadas) de la cuenta con transfer_created_at en [from, to). */
    @Query("""
            select count(e) from Evaluation e
            where e.fromAccount = :fromAccount
              and e.transferCreatedAt >= :from
              and e.transferCreatedAt < :to""")
    long countInWindow(@Param("fromAccount") String fromAccount,
                       @Param("from") Instant from,
                       @Param("to") Instant to);
}
