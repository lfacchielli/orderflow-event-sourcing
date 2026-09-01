CREATE SCHEMA IF NOT EXISTS orderflow;

CREATE TABLE IF NOT EXISTS orderflow.order_states (
    order_id VARCHAR(50) PRIMARY KEY,
    status VARCHAR(40) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    items JSONB NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    destination JSONB NOT NULL,
    current_hub VARCHAR(100),
    visited_hubs JSONB NOT NULL DEFAULT '[]'::jsonb,
    total_delay_minutes INTEGER NOT NULL DEFAULT 0,
    has_delay BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    delivered_at TIMESTAMPTZ,
    last_updated_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT order_states_version_positive
        CHECK (aggregate_version > 0),

    CONSTRAINT order_states_delay_non_negative
        CHECK (total_delay_minutes >= 0),

    CONSTRAINT order_states_amount_non_negative
        CHECK (total_amount >= 0)
);

CREATE INDEX IF NOT EXISTS idx_order_states_status
    ON orderflow.order_states (status);

CREATE INDEX IF NOT EXISTS idx_order_states_current_hub
    ON orderflow.order_states (current_hub);

CREATE INDEX IF NOT EXISTS idx_order_states_last_updated
    ON orderflow.order_states (last_updated_at DESC);

CREATE TABLE IF NOT EXISTS orderflow.processed_events (
    event_id UUID PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    topic_name VARCHAR(255) NOT NULL,
    partition_number INTEGER NOT NULL,
    record_offset BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT processed_events_order_version_unique
        UNIQUE (order_id, aggregate_version),

    CONSTRAINT processed_events_topic_position_unique
        UNIQUE (topic_name, partition_number, record_offset)
);

CREATE INDEX IF NOT EXISTS idx_processed_events_order
    ON orderflow.processed_events (
        order_id,
        aggregate_version
    );