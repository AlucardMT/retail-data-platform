-- Fail if a customer has more than one current SCD2 version.

SELECT
    customer_unique_id,
    COUNT(*) AS current_version_count
FROM {{ ref('int_customer_history') }}
WHERE is_current = TRUE
GROUP BY customer_unique_id
HAVING COUNT(*) > 1
