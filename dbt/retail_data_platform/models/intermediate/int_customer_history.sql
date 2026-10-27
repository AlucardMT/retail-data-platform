{{ config(materialized='view') }}

WITH source_events AS (

    SELECT
        event_uuid,
        event_id,
        customer_unique_id,
        operation,
        event_timestamp,
        before_json,
        after_json,
        before_customer_city,
        before_customer_state,
        before_customer_zip_code_prefix,
        after_customer_city,
        after_customer_state,
        after_customer_zip_code_prefix
    FROM {{ ref('stg_customer_cdc_events') }}

),

ordered_events AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY event_timestamp, event_id, event_uuid
        ) AS event_sequence,
        COUNT(*) OVER (
            PARTITION BY customer_unique_id, event_timestamp
        ) > 1 AS event_has_ordering_ambiguity
    FROM source_events

),

event_context AS (

    SELECT
        *,
        LAG(operation) OVER event_order AS previous_operation,
        LAG(after_customer_city) OVER event_order AS previous_after_city,
        LAG(after_customer_state) OVER event_order AS previous_after_state,
        LAG(after_customer_zip_code_prefix) OVER event_order AS previous_after_zip
    FROM ordered_events
    WINDOW event_order AS (
        PARTITION BY customer_unique_id
        ORDER BY event_sequence
    )

),

validated_events AS (

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
        END AS event_has_payload_conflict,

        CASE
            WHEN event_sequence = 1 THEN FALSE
            WHEN previous_operation = 'D' THEN operation != 'I'
            WHEN operation = 'I' THEN TRUE
            WHEN previous_operation IN ('I', 'U')
                THEN NOT (
                    before_customer_city
                        IS NOT DISTINCT FROM previous_after_city
                    AND before_customer_state
                        IS NOT DISTINCT FROM previous_after_state
                    AND before_customer_zip_code_prefix
                        IS NOT DISTINCT FROM previous_after_zip
                )
            ELSE TRUE
        END AS event_has_sequence_conflict
    FROM event_context

),

customer_quality AS (

    SELECT
        customer_unique_id,
        LOGICAL_OR(event_has_ordering_ambiguity)
            AS has_ordering_ambiguity,
        LOGICAL_OR(
            event_has_payload_conflict OR event_has_sequence_conflict
        ) AS has_transition_conflict
    FROM validated_events
    GROUP BY customer_unique_id

),

initial_states AS (

    SELECT
        customer_unique_id,
        before_customer_city AS customer_city,
        before_customer_state AS customer_state,
        before_customer_zip_code_prefix AS customer_zip_code_prefix,
        CAST(NULL AS TIMESTAMP) AS valid_from,
        event_timestamp AS boundary_time,
        event_uuid AS source_event_uuid,
        TRUE AS is_inferred,
        0 AS event_sequence,
        FALSE AS is_deleted
    FROM validated_events
    WHERE event_sequence = 1
      AND operation IN ('U', 'D')
      AND before_json IS NOT NULL

),

changed_states AS (

    SELECT
        customer_unique_id,
        after_customer_city AS customer_city,
        after_customer_state AS customer_state,
        after_customer_zip_code_prefix AS customer_zip_code_prefix,
        event_timestamp AS valid_from,
        event_timestamp AS boundary_time,
        event_uuid AS source_event_uuid,
        FALSE AS is_inferred,
        event_sequence,
        FALSE AS is_deleted
    FROM validated_events
    WHERE operation = 'I'
       OR (
           operation = 'U'
           AND NOT (
               before_customer_city
                   IS NOT DISTINCT FROM after_customer_city
               AND before_customer_state
                   IS NOT DISTINCT FROM after_customer_state
               AND before_customer_zip_code_prefix
                   IS NOT DISTINCT FROM after_customer_zip_code_prefix
           )
       )

),

deleted_states AS (

    SELECT
        customer_unique_id,
        CAST(NULL AS STRING) AS customer_city,
        CAST(NULL AS STRING) AS customer_state,
        CAST(NULL AS STRING) AS customer_zip_code_prefix,
        event_timestamp AS valid_from,
        event_timestamp AS boundary_time,
        event_uuid AS source_event_uuid,
        FALSE AS is_inferred,
        event_sequence,
        TRUE AS is_deleted
    FROM validated_events
    WHERE operation = 'D'

),

state_boundaries AS (

    SELECT * FROM initial_states
    UNION ALL
    SELECT * FROM changed_states
    UNION ALL
    SELECT * FROM deleted_states

),

version_intervals AS (

    SELECT
        *,
        LEAD(boundary_time) OVER (
            PARTITION BY customer_unique_id
            ORDER BY event_sequence
        ) AS valid_to
    FROM state_boundaries

),

customer_versions AS (

    SELECT
        TO_HEX(SHA256(CONCAT(
            customer_unique_id,
            '|',
            source_event_uuid,
            '|',
            IF(is_inferred, 'inferred', 'observed')
        ))) AS customer_version_key,
        customer_unique_id,
        customer_city,
        customer_state,
        customer_zip_code_prefix,
        valid_from,
        valid_to,
        valid_to IS NULL AS is_current,
        is_inferred,
        source_event_uuid
    FROM version_intervals
    WHERE NOT is_deleted

)

SELECT
    versions.*,
    quality.has_ordering_ambiguity,
    quality.has_transition_conflict
FROM customer_versions AS versions
INNER JOIN customer_quality AS quality
    USING (customer_unique_id)
