-- Fail when consecutive customer versions overlap.

WITH ordered_versions AS (
    SELECT
        customer_unique_id,
        customer_version_key,
        valid_from,
        valid_to,
        LAG(valid_to) OVER (
            PARTITION BY customer_unique_id
            ORDER BY valid_from NULLS FIRST, customer_version_key
        ) AS previous_valid_to
    FROM {{ ref('int_customer_history') }}
)
SELECT *
FROM ordered_versions
WHERE previous_valid_to IS NOT NULL
  AND (valid_from IS NULL OR valid_from < previous_valid_to)
