
"""Prevent overlapping customer CDC ingestion runs."""

from contextlib import contextmanager

import psycopg


# Dedicated advisory lock identifier for customer CDC ingestion.
CUSTOMER_CDC_LOCK_ID = 2026102301


@contextmanager
def customer_cdc_ingestion_lock(database_url: str):
    """Hold a session-level PostgreSQL lock for one ingestion run."""

    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")

            if cur.fetchone()[0] != "retail_platform_test":
                raise ValueError(
                    "CDC ingestion lock is restricted to retail_platform_test"
                )

            cur.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (CUSTOMER_CDC_LOCK_ID,),
            )

            acquired = cur.fetchone()[0]

            if not acquired:
                raise RuntimeError(
                    "Another customer CDC ingestion run is already active"
                )

            try:
                yield
            finally:
                cur.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (CUSTOMER_CDC_LOCK_ID,),
                )
