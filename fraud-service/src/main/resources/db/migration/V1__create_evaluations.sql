CREATE TABLE evaluations (
    id                  UUID           PRIMARY KEY,
    transfer_id         UUID           NOT NULL,
    from_account        VARCHAR(34)    NOT NULL,
    amount              NUMERIC(19, 2) NOT NULL,
    decision            VARCHAR(20)    NOT NULL,
    reason              VARCHAR(50),
    transfer_created_at TIMESTAMPTZ    NOT NULL,
    evaluated_at        TIMESTAMPTZ    NOT NULL,
    -- Idempotencia: una sola evaluación por transferencia, aunque el evento llegue duplicado.
    CONSTRAINT uk_evaluations_transfer_id UNIQUE (transfer_id)
);

-- Regla de velocidad: evaluaciones de una cuenta dentro de una ventana de tiempo.
CREATE INDEX idx_evaluations_from_account_created_at ON evaluations (from_account, transfer_created_at);
