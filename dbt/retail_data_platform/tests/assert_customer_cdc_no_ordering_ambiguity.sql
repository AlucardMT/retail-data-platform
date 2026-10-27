-- Fail when multiple CDC events share a timestamp for one customer.

SELECT
    customer_unique_id,
    event_timestamp,
    COUNT(*) AS event_count
FROM {{ ref('stg_customer_cdc_events') }}
GROUP BY customer_unique_id, event_timestamp
HAVING COUNT(*) > 1
