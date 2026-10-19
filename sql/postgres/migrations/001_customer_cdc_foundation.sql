-- Required schemas for CDC events and pipeline metadata.
CREATE SCHEMA IF NOT EXISTS cdc;
CREATE SCHEMA IF NOT EXISTS pipeline;

-- Current operational customer state.
-- Original retail.customers records remain unchanged.

CREATE TABLE retail.customer_profiles (
    customer_unique_id VARCHAR(32) PRIMARY KEY,
    customer_zip_code_prefix VARCHAR(5) NOT NULL,
    customer_city TEXT NOT NULL,
    customer_state VARCHAR(2) NOT NULL,

    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Append-only simulated customer change events.

CREATE TABLE cdc.customer_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_uuid UUID NOT NULL UNIQUE,

    customer_unique_id VARCHAR(32) NOT NULL,
    customer_id VARCHAR(32),

    operation CHAR(1) NOT NULL
        CHECK (operation IN ('I', 'U', 'D')),

    event_timestamp TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    before_data JSONB,
    after_data JSONB,

    schema_version INTEGER NOT NULL DEFAULT 1
        CHECK (schema_version > 0),

    CONSTRAINT customer_event_payload_check CHECK (
        (operation = 'I'
            AND before_data IS NULL
            AND after_data IS NOT NULL)
        OR
        (operation = 'U'
            AND before_data IS NOT NULL
            AND after_data IS NOT NULL)
        OR
        (operation = 'D'
            AND before_data IS NOT NULL
            AND after_data IS NULL)
    )
);

CREATE INDEX idx_customer_events_customer
    ON cdc.customer_events (customer_unique_id, event_id);
