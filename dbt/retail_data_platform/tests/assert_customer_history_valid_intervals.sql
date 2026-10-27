-- Fail if an SCD2 version has a zero-length or negative interval.

SELECT
    customer_version_key,
    customer_unique_id,
    valid_from,
    valid_to
FROM {{ ref('int_customer_history') }}
WHERE valid_from IS NOT NULL
  AND valid_to IS NOT NULL
  AND valid_to <= valid_from
