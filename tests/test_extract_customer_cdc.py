
"""Integration tests for customer CDC extraction."""

import os
import uuid

import psycopg

from retail_pipeline.extract_customer_cdc import (
    acknowledge_customer_event,
    fetch_pending_customer_events,
)
from retail_pipeline.simulate_customer_cdc import (
    update_customer_address,
)


def test_extractor_discovers_committed_event_without_acknowledging():
    """A committed CDC event remains pending after repeated reads."""

    database_url = os.environ["TEST_DATABASE_URL"]
    customer_id = uuid.uuid4().hex
    event_uuid = uuid.uuid4()

    # Verify the database before making changes.
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            if cur.fetchone()[0] != "retail_platform_test":
                raise RuntimeError("Refusing to modify the wrong database")

    try:
        # Create a synthetic customer and commit the transaction.
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO retail.customer_profiles (
                        customer_unique_id,
                        customer_zip_code_prefix,
                        customer_city,
                        customer_state
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    (customer_id, "01001", "sao paulo", "SP"),
                )

        # Update the customer and commit a corresponding CDC event.
        assert update_customer_address(
            database_url=database_url,
            customer_unique_id=customer_id,
            new_zip_code_prefix="20040",
            new_city="rio de janeiro",
            new_state="RJ",
            event_uuid=event_uuid,
        ) is True

        # First extraction: the new event must be discoverable.
        first_batch = fetch_pending_customer_events(
            database_url=database_url,
            batch_size=100,
        )

        matching_events = [
            event for event in first_batch
            if event["event_uuid"] == event_uuid
        ]

        assert len(matching_events) == 1

        event = matching_events[0]

        assert event["customer_unique_id"] == customer_id
        assert event["operation"] == "U"
        assert event["before_data"]["customer_city"] == "sao paulo"
        assert event["after_data"]["customer_city"] == "rio de janeiro"
        assert "customer_id" in event
        assert event["recorded_at"] is not None
        assert event["recorded_at"].tzinfo is not None
        assert event["schema_version"] == 1

        # Reading must not acknowledge the event.
        second_batch = fetch_pending_customer_events(
            database_url=database_url,
            batch_size=100,
        )

        assert sum(
            event["event_uuid"] == event_uuid
            for event in second_batch
        ) == 1

        # Confirm that no ledger entry was created.
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM pipeline.customer_event_extractions
                    WHERE event_uuid = %s
                    """,
                    (event_uuid,),
                )
                assert cur.fetchone()[0] == 0

    finally:
        # Remove only records belonging to this synthetic test.
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                if cur.fetchone()[0] != "retail_platform_test":
                    raise RuntimeError(
                        "Refusing cleanup against the wrong database"
                    )

                cur.execute(
                    """
                    DELETE FROM pipeline.customer_event_extractions
                    WHERE event_uuid = %s
                    """,
                    (event_uuid,),
                )

                cur.execute(
                    """
                    DELETE FROM cdc.customer_events
                    WHERE customer_unique_id = %s
                      AND event_uuid = %s
                    """,
                    (customer_id, event_uuid),
                )

                cur.execute(
                    """
                    DELETE FROM retail.customer_profiles
                    WHERE customer_unique_id = %s
                    """,
                    (customer_id,),
                )


def test_extractor_acknowledgement_is_idempotent():
    """Acknowledgement must hide an event without duplicating its ledger row."""

    database_url = os.environ["TEST_DATABASE_URL"]
    customer_id = uuid.uuid4().hex
    event_uuid = uuid.uuid4()

    # Verify the database before making changes.
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            if cur.fetchone()[0] != "retail_platform_test":
                raise RuntimeError("Refusing to modify the wrong database")

    try:
        # Create a synthetic customer and commit it.
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO retail.customer_profiles (
                        customer_unique_id,
                        customer_zip_code_prefix,
                        customer_city,
                        customer_state
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    (customer_id, "01001", "sao paulo", "SP"),
                )

        # Generate and commit one CDC event.
        assert update_customer_address(
            database_url=database_url,
            customer_unique_id=customer_id,
            new_zip_code_prefix="20040",
            new_city="rio de janeiro",
            new_state="RJ",
            event_uuid=event_uuid,
        ) is True

        # First acknowledgement creates a ledger entry.
        assert acknowledge_customer_event(
            database_url=database_url,
            event_uuid=event_uuid,
        ) is True

        # Capture the original acknowledgement timestamp.
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT extracted_at
                    FROM pipeline.customer_event_extractions
                    WHERE event_uuid = %s
                    """,
                    (event_uuid,),
                )
                original_timestamp = cur.fetchone()[0]

        # Repeating the acknowledgement must not create another row.
        assert acknowledge_customer_event(
            database_url=database_url,
            event_uuid=event_uuid,
        ) is False

        # Verify the original ledger record is unchanged.
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*), MIN(extracted_at)
                    FROM pipeline.customer_event_extractions
                    WHERE event_uuid = %s
                    """,
                    (event_uuid,),
                )
                count, timestamp = cur.fetchone()

                assert count == 1
                assert timestamp == original_timestamp

        # Acknowledged events should no longer be discoverable.
        pending_events = fetch_pending_customer_events(
            database_url=database_url,
            batch_size=100,
        )

        assert not any(
            event["event_uuid"] == event_uuid
            for event in pending_events
        )

    finally:
        # Clean up the synthetic records in foreign-key order.
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                if cur.fetchone()[0] != "retail_platform_test":
                    raise RuntimeError(
                        "Refusing cleanup against the wrong database"
                    )

                cur.execute(
                    """
                    DELETE FROM pipeline.customer_event_extractions
                    WHERE event_uuid = %s
                    """,
                    (event_uuid,),
                )

                cur.execute(
                    """
                    DELETE FROM cdc.customer_events
                    WHERE customer_unique_id = %s
                      AND event_uuid = %s
                    """,
                    (customer_id, event_uuid),
                )

                cur.execute(
                    """
                    DELETE FROM retail.customer_profiles
                    WHERE customer_unique_id = %s
                    """,
                    (customer_id,),
                )


def test_failed_acknowledgement_leaves_event_pending():
    """A failed acknowledgement must leave the CDC event retryable."""

    database_url = os.environ["TEST_DATABASE_URL"]
    customer_id = uuid.uuid4().hex
    event_uuid = uuid.uuid4()

    # Verify database identity before modifying anything.
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            if cur.fetchone()[0] != "retail_platform_test":
                raise RuntimeError("Refusing to modify the wrong database")

    try:
        # Create and commit a synthetic customer.
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO retail.customer_profiles (
                        customer_unique_id,
                        customer_zip_code_prefix,
                        customer_city,
                        customer_state
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    (customer_id, "01001", "sao paulo", "SP"),
                )

        # Generate a committed CDC event.
        assert update_customer_address(
            database_url=database_url,
            customer_unique_id=customer_id,
            new_zip_code_prefix="20040",
            new_city="rio de janeiro",
            new_state="RJ",
            event_uuid=event_uuid,
        ) is True

        # Hold an exclusive lock to simulate an unavailable ledger.
        with psycopg.connect(database_url) as blocking_conn:
            with blocking_conn.cursor() as blocking_cur:
                blocking_cur.execute(
                    """
                    LOCK TABLE pipeline.customer_event_extractions
                    IN ACCESS EXCLUSIVE MODE
                    """
                )

                # Exercise the actual acknowledgement function.
                try:
                    acknowledge_customer_event(
                        database_url=database_url,
                        event_uuid=event_uuid,
                        lock_timeout_ms=200,
                    )
                except psycopg.errors.LockNotAvailable:
                    pass
                else:
                    raise AssertionError(
                        "Expected acknowledgement to fail on lock timeout"
                    )

            # Release the lock without modifying the ledger.
            blocking_conn.rollback()

        # The event must still be pending.
        pending_events = fetch_pending_customer_events(
            database_url=database_url,
            batch_size=100,
        )

        assert any(
            event["event_uuid"] == event_uuid
            for event in pending_events
        )

        # Retry acknowledgement after the simulated failure.
        assert acknowledge_customer_event(
            database_url=database_url,
            event_uuid=event_uuid,
        ) is True

        # After successful acknowledgement, it is no longer pending.
        pending_events = fetch_pending_customer_events(
            database_url=database_url,
            batch_size=100,
        )

        assert not any(
            event["event_uuid"] == event_uuid
            for event in pending_events
        )

    finally:
        # Remove synthetic records in foreign-key order.
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                if cur.fetchone()[0] != "retail_platform_test":
                    raise RuntimeError(
                        "Refusing cleanup against the wrong database"
                    )

                cur.execute(
                    """
                    DELETE FROM pipeline.customer_event_extractions
                    WHERE event_uuid = %s
                    """,
                    (event_uuid,),
                )

                cur.execute(
                    """
                    DELETE FROM cdc.customer_events
                    WHERE customer_unique_id = %s
                      AND event_uuid = %s
                    """,
                    (customer_id, event_uuid),
                )

                cur.execute(
                    """
                    DELETE FROM retail.customer_profiles
                    WHERE customer_unique_id = %s
                    """,
                    (customer_id,),
                )
