CREATE TABLE IF NOT EXISTS
    `retail-data-platform-512111.bronze.customer_cdc_events`
(
    event_id INT64 NOT NULL,
    event_uuid STRING NOT NULL,
    customer_unique_id STRING NOT NULL,
    customer_id STRING,
    operation STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    before_data JSON,
    after_data JSON,
    schema_version INT64 NOT NULL,
    ingested_at TIMESTAMP NOT NULL
)
OPTIONS (
    description = 'Append-only Bronze customer CDC events from PostgreSQL'
);
