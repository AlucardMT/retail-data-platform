
"""Discover customer CDC events awaiting extraction."""

import psycopg
import uuid
from psycopg.rows import dict_row


def fetch_pending_customer_events(
    database_url: str,
    batch_size: int = 100,
) -> list[dict]:
    """Return pending CDC events without acknowledging them."""

    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database_name = cur.fetchone()["current_database"]

            if database_name != "retail_platform_test":
                raise ValueError(
                    "CDC extraction is restricted to retail_platform_test"
                )

            cur.execute(
                """
                SELECT
                    e.event_id,
                    e.event_uuid,
                    e.customer_unique_id,
                    e.customer_id,
                    e.operation,
                    e.event_timestamp,
                    e.recorded_at,
                    e.before_data,
                    e.after_data,
                    e.schema_version
                FROM cdc.customer_events AS e
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM pipeline.customer_event_extractions AS x
                    WHERE x.event_uuid = e.event_uuid
                )
                ORDER BY e.event_id
                LIMIT %s
                """,
                (batch_size,),
            )

            return cur.fetchall()


def acknowledge_customer_event(
    database_url: str,
    event_uuid: uuid.UUID,
    lock_timeout_ms: int = 5000,
) -> bool:
    """Record an event as processed in the PostgreSQL ledger.

    Returns True for a new acknowledgement.
    Returns False if the event was already acknowledged.

    Call only after successful processing or delivery.
    """
    if lock_timeout_ms < 1:
        raise ValueError("lock_timeout_ms must be at least 1")

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")

            if cur.fetchone()[0] != "retail_platform_test":
                raise ValueError(
                    "CDC acknowledgement is restricted to retail_platform_test"
                )

            cur.execute(
                "SELECT set_config('lock_timeout', %s, true)",
                (f"{lock_timeout_ms}ms",),
            )

            cur.execute(
                """
                INSERT INTO pipeline.customer_event_extractions (
                    event_uuid
                )
                VALUES (%s)
                ON CONFLICT (event_uuid) DO NOTHING
                RETURNING event_uuid
                """,
                (event_uuid,),
            )

            inserted = cur.fetchone()

            return inserted is not None
