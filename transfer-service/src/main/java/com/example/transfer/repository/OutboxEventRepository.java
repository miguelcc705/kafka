package com.example.transfer.repository;

import com.example.transfer.domain.OutboxEvent;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    List<OutboxEvent> findByPublishedAtIsNullOrderByCreatedAtAsc(Limit limit);

    @Transactional
    @Modifying
    @Query("update OutboxEvent e set e.publishedAt = :publishedAt where e.id = :id")
    int markPublished(@Param("id") UUID id, @Param("publishedAt") Instant publishedAt);
}
