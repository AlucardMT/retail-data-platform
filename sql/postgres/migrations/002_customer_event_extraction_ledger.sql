
-- Migration 002: Customer CDC extraction ledger
--
-- Tracks CDC events processed by the PostgreSQL extraction stage.
-- Apply using psql --single-transaction.
-- Do not add BEGIN or COMMIT to this file.

CREATE TABLE IF NOT EXISTS pipeline.customer_event_extractions (
    event_uuid UUID PRIMARY KEY,

    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT customer_event_extractions_event_uuid_fk
        FOREIGN KEY (event_uuid)
        REFERENCES cdc.customer_events (event_uuid)
);
