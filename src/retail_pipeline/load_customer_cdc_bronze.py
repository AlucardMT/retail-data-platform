"""Load PostgreSQL customer CDC events into BigQuery Bronze.

Responsibilities:
- Extract pending customer CDC events from PostgreSQL.
- Convert source records into BigQuery-compatible data.
- Append CDC events to the Bronze table.
- Acknowledge events only after confirmed delivery.

Bronze preserves raw events, including duplicates from retries.
Deduplication will take place in the Silver layer.
"""
from datetime import datetime, timezone
from google.cloud import bigquery
from retail_pipeline.extract_customer_cdc import (
    acknowledge_customer_event,
    fetch_pending_customer_events,
)
from retail_pipeline.ingestion_lock import (
    customer_cdc_ingestion_lock
)

def serialize_customer_cdc_event(event: dict) -> dict:
    """Convert a PostgreSQL CDC event into a BigQuery-compatible record."""

    ingested_at = datetime.now(timezone.utc)

    return {
        "event_id": event["event_id"],
        "event_uuid": str(event["event_uuid"]),
        "customer_unique_id": event["customer_unique_id"],
        "customer_id": event["customer_id"],
        "operation": event["operation"],
        "event_timestamp": event["event_timestamp"].isoformat(),
        "recorded_at": event["recorded_at"].isoformat(),
        "before_data": event["before_data"],
        "after_data": event["after_data"],
        "schema_version": event["schema_version"],
        "ingested_at": ingested_at.isoformat(),
    }

def load_customer_cdc_batch(
    client: bigquery.Client,
    records: list[dict],
) -> int:
    """Append serialized CDC records to BigQuery Bronze."""

    if not records:
        return 0

    table_id = (
        "retail-data-platform-512111."
        "bronze.customer_cdc_events"
    )

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        max_bad_records=0,
    )

    load_job = client.load_table_from_json(
        records,
        table_id,
        job_config=job_config,
        location="europe-west2",
    )

    load_job.result()

    if load_job.errors:
        raise RuntimeError(
            f"BigQuery load job failed: {load_job.errors}"
        )

    loaded_count = load_job.output_rows

    if loaded_count != len(records):
        raise RuntimeError(
            "BigQuery row count mismatch: "
            f"submitted={len(records)}, loaded={loaded_count}"
        )

    return loaded_count


def ingest_customer_cdc(
    database_url: str,
    client: bigquery.Client,
    batch_size: int = 100,
) -> int:
    """Ingest pending CDC events with concurrency protection."""

    with customer_cdc_ingestion_lock(database_url):
        events = fetch_pending_customer_events(
            database_url,
            batch_size=batch_size,
        )

        if not events:
            return 0

        records = [
            serialize_customer_cdc_event(event)
            for event in events
        ]

        loaded_count = load_customer_cdc_batch(
            client,
            records,
        )

        if loaded_count != len(events):
            raise RuntimeError(
                "BigQuery loaded count does not match extracted event count"
            )

        for event in events:
            acknowledge_customer_event(
                database_url,
                event["event_uuid"],
            )

        return loaded_count

