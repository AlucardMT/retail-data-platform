import os

import psycopg
import pytest


@pytest.fixture
def test_db():
    """Connect only to the dedicated integration-test database."""

    database_url = os.environ["TEST_DATABASE_URL"]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database_name = cur.fetchone()[0]

        if database_name != "retail_platform_test":
            raise RuntimeError(
                f"Refusing to run against database: {database_name}"
            )

        yield conn


def test_uses_isolated_database(test_db):
    """Integration tests must target the dedicated test database."""

    with test_db.cursor() as cur:
        cur.execute("SELECT current_database()")
        assert cur.fetchone()[0] == "retail_platform_test"


def test_pipeline_rolls_back_when_later_step_fails(monkeypatch):
    """Earlier loader inserts must roll back if a later step fails."""

    from retail_pipeline import load_typed

    database_url = os.environ["TEST_DATABASE_URL"]

    # Safety check using the actual connected database.
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            assert cur.fetchone()[0] == "retail_platform_test"

    # Replace the first loader with an operation that inserts
    # a temporary customer into the actual pipeline transaction.
    original_load_customers = load_typed.load_customers
    test_id = "__pipeline_rollback_test__"

    def insert_test_customer(conn):
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO retail.customers (
                    customer_id,
                    customer_unique_id,
                    customer_zip_code_prefix,
                    customer_city,
                    customer_state
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (
                test_id,
                test_id,
                12345,
                "test_city",
                "SP",
            ))
        return 1

    def fail_later_loader(conn):
        raise RuntimeError("Simulated failure in order payments")

    monkeypatch.setattr(
        load_typed,
        "load_customers",
        insert_test_customer,
    )
    monkeypatch.setattr(
        load_typed,
        "load_order_payments",
        fail_later_loader,
    )
    monkeypatch.setenv("DATABASE_URL", database_url)

    with pytest.raises(
        RuntimeError,
        match="Simulated failure in order payments",
    ):
        load_typed.main()

    # Verify the temporary insert did not survive the failure.
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM retail.customers WHERE customer_id = %s",
                (test_id,),
            )
            remaining = cur.fetchone()[0]

    assert remaining == 0


def reset_typed_tables():
    """Clear typed tables only in the dedicated test database."""

    database_url = os.environ["TEST_DATABASE_URL"]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database_name = cur.fetchone()[0]

            if database_name != "retail_platform_test":
                raise RuntimeError(
                    f"Refusing to reset database: {database_name}"
                )

            cur.execute("""
                TRUNCATE TABLE
                    retail.order_items,
                    retail.order_payments,
                    retail.order_reviews,
                    retail.orders,
                    retail.customers,
                    retail.products,
                    retail.sellers,
                    retail.category_translations
            """)


def test_first_load_and_replay_are_idempotent(monkeypatch, restore_typed_tables_after_test):
    """A clean first load succeeds and replay creates no duplicates."""

    from retail_pipeline import load_typed

    database_url = os.environ["TEST_DATABASE_URL"]

    expected_counts = {
        "customers": 99441,
        "products": 32951,
        "sellers": 3095,
        "category_translations": 71,
        "orders": 99441,
        "order_items": 112650,
        "order_payments": 103886,
        "order_reviews": 99224,
    }

    # Reset only the dedicated integration-test database.
    reset_typed_tables()

    monkeypatch.setenv("DATABASE_URL", database_url)

    def get_counts():
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                assert cur.fetchone()[0] == "retail_platform_test"

                counts = {}
                for table in expected_counts:
                    cur.execute(f"SELECT COUNT(*) FROM retail.{table}")
                    counts[table] = cur.fetchone()[0]

                return counts

    # Verify the starting state.
    assert all(count == 0 for count in get_counts().values())

    # First load: populate all eight typed tables.
    load_typed.main()

    first_load_counts = get_counts()
    assert first_load_counts == expected_counts

    # Second load: replay the same raw source data.
    load_typed.main()

    second_load_counts = get_counts()
    assert second_load_counts == first_load_counts


def test_real_first_load_rolls_back_on_failure(monkeypatch, restore_typed_tables_after_test):
    """A failure during a real first load must roll back all inserts."""

    from retail_pipeline import load_typed

    database_url = os.environ["TEST_DATABASE_URL"]

    # This helper checks the actual database name before truncating.
    reset_typed_tables()

    monkeypatch.setenv("DATABASE_URL", database_url)

    def fail_payments(conn):
        raise RuntimeError("Simulated payment loader failure")

    monkeypatch.setattr(
        load_typed,
        "load_order_payments",
        fail_payments,
    )

    with pytest.raises(
        RuntimeError,
        match="Simulated payment loader failure",
    ):
        load_typed.main()

    # Check every typed table after the failed transaction.
    tables = [
        "customers",
        "products",
        "sellers",
        "category_translations",
        "orders",
        "order_items",
        "order_payments",
        "order_reviews",
    ]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            assert cur.fetchone()[0] == "retail_platform_test"

            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM retail.{table}")
                count = cur.fetchone()[0]
                assert count == 0, (
                    f"Rollback failed: retail.{table} contains "
                    f"{count} records"
                )


@pytest.fixture
def restore_typed_tables_after_test(monkeypatch):
    """Restore the populated test database after a destructive test."""

    yield

    from retail_pipeline import load_typed

    database_url = os.environ["TEST_DATABASE_URL"]

    # Verify the actual database before performing any writes.
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            if cur.fetchone()[0] != "retail_platform_test":
                raise RuntimeError("Refusing to restore the wrong database")

    # Undo any loader monkeypatches before restoring.
    monkeypatch.undo()

    # The existing loader is idempotent, so it can restore
    # missing records without duplicating existing ones.
    monkeypatch.setenv("DATABASE_URL", database_url)
    load_typed.main()
