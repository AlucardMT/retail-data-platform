-- Fail when CDC payloads or consecutive transitions are inconsistent.

WITH ordered_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY event_timestamp, event_id, event_uuid
        ) AS event_sequence,
        LAG(operation) OVER event_order AS previous_operation,
        LAG(after_customer_city) OVER event_order AS previous_city,
        LAG(after_customer_state) OVER event_order AS previous_state,
        LAG(after_customer_zip_code_prefix) OVER event_order AS previous_zip
    FROM {{ ref('stg_customer_cdc_events') }}
    WINDOW event_order AS (
        PARTITION BY customer_unique_id
        ORDER BY event_timestamp, event_id, event_uuid
    )
),
conflicts AS (
    SELECT
        *,
        CASE
            WHEN operation = 'I'
                THEN before_json IS NOT NULL OR after_json IS NULL
            WHEN operation = 'U'
                THEN before_json IS NULL OR after_json IS NULL
            WHEN operation = 'D'
                THEN before_json IS NULL OR after_json IS NOT NULL
            ELSE TRUE
        END AS payload_conflict,
        CASE
            WHEN event_sequence = 1 THEN FALSE
            WHEN previous_operation = 'D' THEN operation != 'I'
            WHEN operation = 'I' THEN TRUE
            WHEN previous_operation IN ('I', 'U') THEN NOT (
                before_customer_city IS NOT DISTINCT FROM previous_city
                AND before_customer_state IS NOT DISTINCT FROM previous_state
                AND before_customer_zip_code_prefix IS NOT DISTINCT FROM previous_zip
            )
            ELSE TRUE
        END AS sequence_conflict
    FROM ordered_events
)
SELECT
    customer_unique_id,
    event_uuid,
    event_sequence,
    payload_conflict,
    sequence_conflict
FROM conflicts
WHERE payload_conflict OR sequence_conflict
