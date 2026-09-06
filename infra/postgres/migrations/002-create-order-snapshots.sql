CREATE TABLE IF NOT EXISTS orderflow.order_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    state_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT order_snapshots_version_positive
        CHECK (aggregate_version > 0),

    CONSTRAINT order_snapshots_order_version_unique
        UNIQUE (order_id, aggregate_version)
);

CREATE INDEX IF NOT EXISTS idx_order_snapshots_latest
    ON orderflow.order_snapshots (
        order_id,
        aggregate_version DESC
    );