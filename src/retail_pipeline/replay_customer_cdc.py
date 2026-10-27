"""Explicit, guarded replay of PostgreSQL customer CDC history."""

import argparse
import os
import re

import psycopg
from psycopg.rows import dict_row
from google.cloud import bigquery

from retail_pipeline.load_customer_cdc_bronze import (
    serialize_customer_cdc_event,
)


PROJECT = "retail-data-platform-512111"
LOCATION = "europe-west2"
LIVE_TABLE = f"{PROJECT}.bronze.customer_cdc_events"

EXPECTED_SCHEMA = (
    ("event_id", "INTEGER", "REQUIRED"),
    ("event_uuid", "STRING", "REQUIRED"),
    ("customer_unique_id", "STRING", "REQUIRED"),
    ("customer_id", "STRING", "NULLABLE"),
    ("operation", "STRING", "REQUIRED"),
    ("event_timestamp", "TIMESTAMP", "REQUIRED"),
    ("recorded_at", "TIMESTAMP", "REQUIRED"),
    ("before_data", "JSON", "NULLABLE"),
    ("after_data", "JSON", "NULLABLE"),
    ("schema_version", "INTEGER", "REQUIRED"),
    ("ingested_at", "TIMESTAMP", "REQUIRED"),
)


def validate_destination(destination: str) -> str:
    """Require an explicit recovery table outside the live Bronze table."""
    if not isinstance(destination, str):
        raise ValueError("A recovery destination is required")

    if destination == LIVE_TABLE:
        raise ValueError("Replay into the live Bronze table is prohibited")

    pattern = (
        r"retail-data-platform-512111\."
        r"bronze\.customer_cdc_events_recovery_[A-Za-z0-9_]+"
    )
    if not re.fullmatch(pattern, destination):
        raise ValueError("Destination must be a table in the project Bronze dataset")

    return destination


def fetch_historical_events(database_url: str) -> list[dict]:
    """Read all CDC events without using or changing acknowledgements."""
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute("SELECT current_database()")
            if cur.fetchone()["current_database"] != "retail_platform_test":
                raise ValueError("Replay source must be retail_platform_test")

            cur.execute(
                """
                SELECT
                    event_id, event_uuid, customer_unique_id,
                    customer_id, operation, event_timestamp,
                    recorded_at, before_data, after_data,
                    schema_version
                FROM cdc.customer_events
                ORDER BY event_id, event_uuid
                """
            )
            return cur.fetchall()


def verify_recovery_table(client: bigquery.Client, destination: str) -> None:
    """Require an existing, empty table with the expected Bronze schema."""
    table = client.get_table(destination)

    if table.table_type != "TABLE":
        raise ValueError("Recovery destination must be a physical table")

    if table.location.lower() != LOCATION:
        raise ValueError("Recovery destination has the wrong location")

    actual_schema = tuple(
        (field.name, field.field_type, field.mode)
        for field in table.schema
    )
    if actual_schema != EXPECTED_SCHEMA:
        raise ValueError("Recovery destination schema does not match Bronze")

    query = (
        f"SELECT EXISTS(SELECT 1 FROM `{destination}` LIMIT 1) AS has_rows"
    )
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=10 * 1024 * 1024,
        use_query_cache=False,
    )
    rows = list(
        client.query(
            query,
            job_config=job_config,
            location=LOCATION,
        ).result()
    )
    if len(rows) != 1 or rows[0]["has_rows"]:
        raise ValueError("Recovery destination must be empty")



def reconcile_recovery_events(
    client: bigquery.Client,
    destination: str,
    source_events: list[dict],
) -> None:
    """Verify that recovery identities exactly match the source."""
    destination = validate_destination(destination)

    expected = [
        (int(event["event_id"]), str(event["event_uuid"]))
        for event in source_events
    ]

    if len(expected) != len(set(expected)):
        raise ValueError("Duplicate source event identities")

    query = (
        f"SELECT event_id, event_uuid FROM `{destination}`"
    )
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=10 * 1024 * 1024,
        use_query_cache=False,
    )

    rows = client.query(
        query,
        job_config=job_config,
        location=LOCATION,
    ).result()

    actual = [
        (int(row["event_id"]), str(row["event_uuid"]))
        for row in rows
    ]

    if len(actual) != len(expected) or set(actual) != set(expected):
        raise RuntimeError(
            "Recovery reconciliation failed: event identities differ"
        )

def replay_customer_cdc(
    database_url: str,
    client: bigquery.Client,
    destination: str,
    execute: bool = False,
) -> int:
    """Preview history or explicitly load it into an empty recovery table."""
    destination = validate_destination(destination)
    events = fetch_historical_events(database_url)

    event_uuids = [str(event["event_uuid"]) for event in events]
    if len(event_uuids) != len(set(event_uuids)):
        raise ValueError("Duplicate event UUIDs found in replay source")

    if not execute:
        return len(events)

    verify_recovery_table(client, destination)

    if not events:
        return 0

    records = [serialize_customer_cdc_event(event) for event in events]

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(name, field_type, mode=mode)
            for name, field_type, mode in EXPECTED_SCHEMA
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
        max_bad_records=0,
    )

    job = client.load_table_from_json(
        records,
        destination,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()

    if job.errors or job.output_rows != len(records):
        raise RuntimeError("Historical replay load verification failed")

    reconcile_recovery_events(client, destination, events)

    return len(records)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preview or replay PostgreSQL customer CDC history"
    )
    parser.add_argument("--destination", required=True)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually load events; omitted means dry run",
    )
    args = parser.parse_args()

    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        parser.error("TEST_DATABASE_URL is not configured")

    client = bigquery.Client(project=PROJECT) if args.execute else None
    count = replay_customer_cdc(
        database_url=database_url,
        client=client,
        destination=args.destination,
        execute=args.execute,
    )

    mode = "loaded" if args.execute else "previewed"
    print(f"Historical CDC events {mode}: {count}")
    print(f"Destination: {args.destination}")


if __name__ == "__main__":
    main()
