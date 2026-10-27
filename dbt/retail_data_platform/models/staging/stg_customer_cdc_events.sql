{{ config(materialized='view') }}

WITH bronze_events AS (

    SELECT
        event_id,
        event_uuid,
        customer_unique_id,
        customer_id,
        operation,
        event_timestamp,
        recorded_at,
        before_data,
        after_data,
        schema_version,
        ingested_at
    FROM {{ source('bronze', 'customer_cdc_events') }}

),

normalised_payloads AS (

    SELECT
        * EXCEPT (before_data, after_data),

        CASE
            WHEN JSON_TYPE(before_data) = 'string'
                THEN SAFE.PARSE_JSON(JSON_VALUE(before_data, '$'))
            ELSE before_data
        END AS before_json,

        CASE
            WHEN JSON_TYPE(after_data) = 'string'
                THEN SAFE.PARSE_JSON(JSON_VALUE(after_data, '$'))
            ELSE after_data
        END AS after_json

    FROM bronze_events

),

extracted_attributes AS (

    SELECT
        * EXCEPT (before_json, after_json),

        JSON_VALUE(before_json, '$.customer_city')
            AS before_customer_city,

        JSON_VALUE(before_json, '$.customer_state')
            AS before_customer_state,

        JSON_VALUE(before_json, '$.customer_zip_code_prefix')
            AS before_customer_zip_code_prefix,

        JSON_VALUE(after_json, '$.customer_city')
            AS after_customer_city,

        JSON_VALUE(after_json, '$.customer_state')
            AS after_customer_state,

        JSON_VALUE(after_json, '$.customer_zip_code_prefix')
            AS after_customer_zip_code_prefix,

        before_json,
        after_json

    FROM normalised_payloads

),

deduplicated_events AS (

    SELECT *
    FROM extracted_attributes

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_uuid
        ORDER BY
            ingested_at DESC,
            event_id DESC,
            TO_JSON_STRING(STRUCT(
                customer_unique_id,
                customer_id,
                operation,
                event_timestamp,
                recorded_at,
                schema_version,
                before_json,
                after_json
            )) DESC
    ) = 1

)

SELECT *
FROM deduplicated_events
