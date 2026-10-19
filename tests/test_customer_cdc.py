
"""Integration tests for the customer CDC transactional outbox."""

import os
import uuid
import time
import psycopg
import pytest
import threading

from concurrent.futures import ThreadPoolExecutor

from retail_pipeline.simulate_customer_cdc import (
    _update_customer_address_in_transaction,
    update_customer_address,
)


TEST_CUSTOMER_ID = "0000366f3b9a7992bf8c76cfdf3221e2"

@pytest.fixture
def rollback_connection():
    """Provide a test connection and always roll back changes."""

    database_url = os.environ["TEST_DATABASE_URL"]

    conn = psycopg.connect(database_url)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database_name = cur.fetchone()[0]

        if database_name != "retail_platform_test":
            raise RuntimeError(
                f"Refusing to test against database: {database_name}"
            )

        # The first query started an implicit transaction.
        # The CDC helper participates in this same transaction.
        yield conn

    finally:
        conn.rollback()
        conn.close()



def test_customer_update_creates_cdc_event(rollback_connection):
    """A profile update must produce a matching CDC event."""

    conn = rollback_connection
    event_id = uuid.uuid4()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                customer_zip_code_prefix,
                customer_city,
                customer_state
            FROM retail.customer_profiles
            WHERE customer_unique_id = %s
            """,
            (TEST_CUSTOMER_ID,),
        )

        original_address = cur.fetchone()
        assert original_address is not None

    result = _update_customer_address_in_transaction(
        conn=conn,
        customer_unique_id=TEST_CUSTOMER_ID,
        new_zip_code_prefix="13000",
        new_city="campinas",
        new_state="SP",
        event_uuid=event_id,
    )

    assert result is True

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                customer_zip_code_prefix,
                customer_city,
                customer_state
            FROM retail.customer_profiles
            WHERE customer_unique_id = %s
            """,
            (TEST_CUSTOMER_ID,),
        )

        assert cur.fetchone() == ("13000", "campinas", "SP")

        cur.execute(
            """
            SELECT
                operation,
                before_data,
                after_data
            FROM cdc.customer_events
            WHERE event_uuid = %s
            """,
            (event_id,),
        )

        event = cur.fetchone()
        assert event is not None

        operation, before_data, after_data = event

        assert operation == "U"

        assert before_data == {
            "customer_zip_code_prefix": original_address[0],
            "customer_city": original_address[1],
            "customer_state": original_address[2],
        }

        assert after_data == {
            "customer_zip_code_prefix": "13000",
            "customer_city": "campinas",
            "customer_state": "SP",
        }


def test_customer_update_is_idempotent(rollback_connection):
    """Retrying the same event must not create duplicate changes."""

    conn = rollback_connection
    event_id = uuid.uuid4()

    change = {
        "conn": conn,
        "customer_unique_id": TEST_CUSTOMER_ID,
        "new_zip_code_prefix": "13000",
        "new_city": "campinas",
        "new_state": "SP",
        "event_uuid": event_id,
    }

    # First submission: apply the change.
    first_result = _update_customer_address_in_transaction(**change)

    # Second submission: simulate a retry of the same event.
    second_result = _update_customer_address_in_transaction(**change)

    assert first_result is True
    assert second_result is False

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM cdc.customer_events
            WHERE event_uuid = %s
            """,
            (event_id,),
        )

        assert cur.fetchone()[0] == 1

        cur.execute(
            """
            SELECT
                customer_zip_code_prefix,
                customer_city,
                customer_state
            FROM retail.customer_profiles
            WHERE customer_unique_id = %s
            """,
            (TEST_CUSTOMER_ID,),
        )

        assert cur.fetchone() == ("13000", "campinas", "SP")


def test_customer_update_retry_after_commit():
    """Retrying a committed CDC event must not duplicate the change."""

    database_url = os.environ["TEST_DATABASE_URL"]
    customer_id = uuid.uuid4().hex
    event_id = uuid.uuid4()

    # Verify the database before making any changes.
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            if cur.fetchone()[0] != "retail_platform_test":
                raise RuntimeError("Refusing to modify the wrong database")

    try:
        # Create a synthetic customer in a committed transaction.
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
                    (customer_id, "07787", "cajamar", "SP"),
                )

        change = {
            "database_url": database_url,
            "customer_unique_id": customer_id,
            "new_zip_code_prefix": "13000",
            "new_city": "campinas",
            "new_state": "SP",
            "event_uuid": event_id,
        }

        # First call commits the customer update and CDC event.
        assert update_customer_address(**change) is True

        # Second call opens another transaction and retries.
        assert update_customer_address(**change) is False

        # Verify the final committed state.
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        customer_zip_code_prefix,
                        customer_city,
                        customer_state
                    FROM retail.customer_profiles
                    WHERE customer_unique_id = %s
                    """,
                    (customer_id,),
                )

                assert cur.fetchone() == ("13000", "campinas", "SP")

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM cdc.customer_events
                    WHERE event_uuid = %s
                    """,
                    (event_id,),
                )

                assert cur.fetchone()[0] == 1

    finally:
        # Clean up only records belonging to this synthetic customer.
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                if cur.fetchone()[0] != "retail_platform_test":
                    raise RuntimeError(
                        "Refusing cleanup against the wrong database"
                    )

                cur.execute(
                    """
                    DELETE FROM cdc.customer_events
                    WHERE customer_unique_id = %s
                      AND event_uuid = %s
                    """,
                    (customer_id, event_id),
                )

                cur.execute(
                    """
                    DELETE FROM retail.customer_profiles
                    WHERE customer_unique_id = %s
                    """,
                    (customer_id,),
                )


def test_customer_update_rejects_conflicting_event_uuid(
    rollback_connection,
):
    """An event UUID cannot be reused with different change data."""

    conn = rollback_connection
    event_id = uuid.uuid4()

    first_result = _update_customer_address_in_transaction(
        conn=conn,
        customer_unique_id=TEST_CUSTOMER_ID,
        new_zip_code_prefix="13000",
        new_city="campinas",
        new_state="SP",
        event_uuid=event_id,
    )

    assert first_result is True

    # Reuse the same UUID but request a different city.
    with pytest.raises(
        ValueError,
        match="Event UUID reused with different change data",
    ):
        _update_customer_address_in_transaction(
            conn=conn,
            customer_unique_id=TEST_CUSTOMER_ID,
            new_zip_code_prefix="13000",
            new_city="santos",
            new_state="SP",
            event_uuid=event_id,
        )

    # The conflicting request must not change the profile.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT customer_city
            FROM retail.customer_profiles
            WHERE customer_unique_id = %s
            """,
            (TEST_CUSTOMER_ID,),
        )

        assert cur.fetchone()[0] == "campinas"

        # Only the original event should exist.
        cur.execute(
            """
            SELECT COUNT(*)
            FROM cdc.customer_events
            WHERE event_uuid = %s
            """,
            (event_id,),
        )

        assert cur.fetchone()[0] == 1




def test_customer_update_rolls_back_when_event_insert_fails(
    rollback_connection,
):
    """A failed CDC insert must roll back the customer update."""

    conn = rollback_connection

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                customer_zip_code_prefix,
                customer_city,
                customer_state
            FROM retail.customer_profiles
            WHERE customer_unique_id = %s
            """,
            (TEST_CUSTOMER_ID,),
        )
        original_address = cur.fetchone()

    assert original_address is not None

    # Create a temporary, transaction-scoped trigger function
    # that deliberately rejects CDC event inserts.
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE FUNCTION pg_temp.reject_cdc_insert()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                RAISE EXCEPTION 'Simulated CDC insert failure';
            END;
            $$
            """
        )

        cur.execute(
            """
            CREATE TRIGGER test_reject_cdc_insert
            BEFORE INSERT ON cdc.customer_events
            FOR EACH ROW
            EXECUTE FUNCTION pg_temp.reject_cdc_insert()
            """
        )

    # A nested transaction creates a savepoint.
    # The simulator updates the profile first, then attempts
    # to insert the event, which our trigger rejects.
    with pytest.raises(
        psycopg.errors.RaiseException,
        match="Simulated CDC insert failure",
    ):
        with conn.transaction():
            _update_customer_address_in_transaction(
                conn=conn,
                customer_unique_id=TEST_CUSTOMER_ID,
                new_zip_code_prefix="13000",
                new_city="campinas",
                new_state="SP",
                event_uuid=uuid.uuid4(),
            )

    # The failed operation was rolled back to the savepoint.
    # The original customer address must still be present.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                customer_zip_code_prefix,
                customer_city,
                customer_state
            FROM retail.customer_profiles
            WHERE customer_unique_id = %s
            """,
            (TEST_CUSTOMER_ID,),
        )

        assert cur.fetchone() == original_address

        cur.execute(
            """
            SELECT COUNT(*)
            FROM cdc.customer_events
            WHERE customer_unique_id = %s
            """,
            (TEST_CUSTOMER_ID,),
        )

        assert cur.fetchone()[0] == 0


def test_customer_row_lock_blocks_concurrent_update(
    rollback_connection,
):
    """A second transaction cannot lock an already-locked customer."""

    conn1 = rollback_connection
    database_url = os.environ["TEST_DATABASE_URL"]

    # First connection acquires a customer row lock.
    with conn1.cursor() as cur:
        cur.execute(
            """
            SELECT customer_unique_id
            FROM retail.customer_profiles
            WHERE customer_unique_id = %s
            FOR UPDATE
            """,
            (TEST_CUSTOMER_ID,),
        )

        assert cur.fetchone() is not None

    # A separate connection attempts to lock the same row.
    with psycopg.connect(database_url) as conn2:
        with conn2.cursor() as cur:
            cur.execute("SELECT current_database()")
            assert cur.fetchone()[0] == "retail_platform_test"

            # Limit how long this transaction can wait.
            cur.execute("SET LOCAL lock_timeout = '1s'")
            cur.execute("SET LOCAL statement_timeout = '5s'")

            with pytest.raises(psycopg.errors.LockNotAvailable):
                cur.execute(
                    """
                    SELECT customer_unique_id
                    FROM retail.customer_profiles
                    WHERE customer_unique_id = %s
                    FOR UPDATE
                    """,
                    (TEST_CUSTOMER_ID,),
                )

            # The timeout aborts conn2's transaction.
            # Its connection context will roll it back on exit.


def test_concurrent_customer_updates_preserve_cdc_history():
    """Concurrent updates must produce a consistent CDC chain."""
    database_url = os.environ["TEST_DATABASE_URL"]
    customer_id = uuid.uuid4().hex
    event_a = uuid.uuid4()
    event_b = uuid.uuid4()

    first_update_done = threading.Event()
    allow_first_commit = threading.Event()
    second_update_starting = threading.Event()

    connection_a_pid = {}
    connection_b_pid = {}

    def transaction_a():
        with psycopg.connect(database_url) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL lock_timeout = '5s'")
                    cur.execute("SET LOCAL statement_timeout = '10s'")
                    cur.execute("SELECT pg_backend_pid()")
                    connection_a_pid["pid"] = cur.fetchone()[0]

                assert _update_customer_address_in_transaction(
                    conn, customer_id, "11111", "city_a", "SP", event_a
                )

                first_update_done.set()
                assert allow_first_commit.wait(timeout=5)

    def transaction_b():
        assert first_update_done.wait(timeout=5)

        with psycopg.connect(database_url) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL lock_timeout = '5s'")
                    cur.execute("SET LOCAL statement_timeout = '10s'")
                    cur.execute("SELECT pg_backend_pid()")
                    connection_b_pid["pid"] = cur.fetchone()[0]

                second_update_starting.set()

                return _update_customer_address_in_transaction(
                    conn, customer_id, "22222", "city_b", "SP", event_b
                )

    # Synthetic customer is committed so both connections can see it.
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            assert cur.fetchone()[0] == "retail_platform_test"
            cur.execute(
                """
                INSERT INTO retail.customer_profiles (
                    customer_unique_id,
                    customer_zip_code_prefix,
                    customer_city,
                    customer_state
                )
                VALUES (%s, '00000', 'original_city', 'SP')
                """,
                (customer_id,),
            )

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_a = executor.submit(transaction_a)
            future_b = executor.submit(transaction_b)


            try:
                assert first_update_done.wait(timeout=5)
                assert second_update_starting.wait(timeout=5)

                pid_a = connection_a_pid["pid"]
                pid_b = connection_b_pid["pid"]

                # Check whether PostgreSQL reports A as blocking B.
                deadline = time.monotonic() + 3
                blocking_confirmed = False

                with psycopg.connect(database_url) as monitor_conn:
                   with monitor_conn.cursor() as cur:
                        cur.execute("SELECT current_database()")
                        assert cur.fetchone()[0] == "retail_platform_test"

                        while time.monotonic() < deadline:
                            cur.execute(
                                "SELECT pg_blocking_pids(%s)",
                                (pid_b,),
                            )
                            blocking_pids = cur.fetchone()[0]

                            if pid_a in blocking_pids:
                                blocking_confirmed = True
                                break

                            time.sleep(0.05)

                assert blocking_confirmed, (
                    f"Expected PID {pid_a} to block PID {pid_b}"
                )

            finally:
                # Release A even if monitoring or an assertion fails.
                allow_first_commit.set()

            assert future_a.result(timeout=10) is None
            assert future_b.result(timeout=10) is True

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT customer_city
                    FROM retail.customer_profiles
                    WHERE customer_unique_id = %s
                    """,
                    (customer_id,),
                )
                assert cur.fetchone()[0] == "city_b"

                cur.execute(
                    """
                    SELECT before_data, after_data
                    FROM cdc.customer_events
                    WHERE customer_unique_id = %s
                    ORDER BY event_id
                    """,
                    (customer_id,),
                )
                events = cur.fetchall()

                assert len(events) == 2
                assert events[0][0]["customer_city"] == "original_city"
                assert events[0][1]["customer_city"] == "city_a"
                assert events[1][0]["customer_city"] == "city_a"
                assert events[1][1]["customer_city"] == "city_b"
    finally:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                assert cur.fetchone()[0] == "retail_platform_test"

                cur.execute(
                    "DELETE FROM cdc.customer_events WHERE customer_unique_id = %s",
                    (customer_id,),
                )
                cur.execute(
                    "DELETE FROM retail.customer_profiles WHERE customer_unique_id = %s",
                    (customer_id,),
                )
