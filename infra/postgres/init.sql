CREATE SCHEMA IF NOT EXISTS orderflow;

CREATE TABLE IF NOT EXISTS orderflow.infrastructure_check (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    component VARCHAR(50) NOT NULL,
    initialized_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO orderflow.infrastructure_check (component)
VALUES ('postgres');