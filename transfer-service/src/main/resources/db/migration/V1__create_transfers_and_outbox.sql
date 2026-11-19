CREATE TABLE transfers (
    id           UUID          PRIMARY KEY,
    from_account VARCHAR(34)   NOT NULL,
    to_account   VARCHAR(34)   NOT NULL,
    amount       NUMERIC(19,2) NOT NULL,
    currency     VARCHAR(3)    NOT NULL,
    status       VARCHAR(20)   NOT NULL,
    reason       VARCHAR(50),
    created_at   TIMESTAMPTZ   NOT NULL,
    updated_at   TIMESTAMPTZ   NOT NULL
);

CREATE INDEX idx_transfers_from_account ON transfers (from_account);

CREATE TABLE outbox_events (
    id             UUID         PRIMARY KEY,
    aggregate_id   UUID         NOT NULL,
    topic          VARCHAR(100) NOT NULL,
    message_key    VARCHAR(100) NOT NULL,
    payload        JSONB        NOT NULL,
    correlation_id VARCHAR(100),
    created_at     TIMESTAMPTZ  NOT NULL,
    published_at   TIMESTAMPTZ
);

-- El publicador solo lee los pendientes: el índice parcial se mantiene pequeño aunque la tabla crezca.
CREATE INDEX idx_outbox_events_unpublished ON outbox_events (created_at) WHERE published_at IS NULL;
