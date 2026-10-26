"""Read-only data quality checks for customer CDC BigQuery Bronze."""

from google.cloud import bigquery


BRONZE_TABLE = (
    "retail-data-platform-512111."
    "bronze.customer_cdc_events"
)


def check_customer_cdc_bronze_quality(
    client: bigquery.Client,
) -> dict:
    """Return table-level quality metrics without modifying Bronze."""

    query = f"""
    SELECT
        COUNT(*) AS total_rows,

        COUNT(DISTINCT event_uuid) AS distinct_events,

        COUNT(*) - COUNT(DISTINCT event_uuid)
            AS duplicate_rows,

        COUNTIF(
            event_uuid IS NULL
            OR TRIM(event_uuid) = ''
            OR customer_unique_id IS NULL
            OR TRIM(customer_unique_id) = ''
        ) AS missing_identifiers,

        COUNTIF(
            (before_data IS NOT NULL
             AND JSON_TYPE(before_data) != 'object')
            OR
            (after_data IS NOT NULL
             AND JSON_TYPE(after_data) != 'object')
        ) AS invalid_json_structure,

        COUNTIF(
            operation IS NULL
            OR operation NOT IN ('I', 'U', 'D')
            OR (
                operation = 'I'
                AND JSON_TYPE(after_data)
                    IS DISTINCT FROM 'object'
            )
            OR (
                operation = 'U'
                AND (
                    JSON_TYPE(before_data)
                        IS DISTINCT FROM 'object'
                    OR JSON_TYPE(after_data)
                        IS DISTINCT FROM 'object'
                )
            )
            OR (
                operation = 'D'
                AND JSON_TYPE(before_data)
                    IS DISTINCT FROM 'object'
            )
        ) AS contract_violations

    FROM `{BRONZE_TABLE}`
    """

    query_job = client.query(
        query,
        location="europe-west2",
    )

    rows = list(query_job.result())

    if len(rows) != 1:
        raise RuntimeError(
            "Expected exactly one Bronze quality summary row"
        )

    return dict(rows[0].items())
