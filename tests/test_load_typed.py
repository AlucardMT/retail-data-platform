
import os

import psycopg
import pytest

from retail_pipeline.load_typed import (
        load_customers,
        load_products,
        load_sellers,
        load_category_translations,
        load_orders,
        load_order_items,
        load_order_payments,
        load_order_reviews
        )


@pytest.fixture
def db_connection():
    """Provide a database connection for integration tests."""
    with psycopg.connect(
        os.environ["DATABASE_URL"]
    ) as conn:
        yield conn


def test_customers_replay_is_idempotent(db_connection):
    """Replaying the customer load must not add rows."""

    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM retail.customers"
        )
        before = cur.fetchone()[0]

    inserted = load_customers(db_connection)

    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM retail.customers"
        )
        after = cur.fetchone()[0]

    assert inserted == 0
    assert after == before


def test_all_raw_customers_exist(db_connection):
    """Every raw customer must exist in the typed table."""

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM olist_raw.customers AS raw
            WHERE NOT EXISTS (
                SELECT 1
                FROM retail.customers AS typed
                WHERE typed.customer_id = raw.customer_id
            )
        """)

        missing = cur.fetchone()[0]

    assert missing == 0

@pytest.mark.parametrize(
    "loader,table_name,expected_count",
    [
        (load_customers, "customers", 99441),
        (load_products, "products", 32951),
        (load_sellers, "sellers", 3095),
        (load_category_translations,"category_translations",71),
        (load_orders, "orders", 99441),
        (load_order_items, "order_items", 112650),
        (load_order_payments, "order_payments", 103886),
        (load_order_reviews, "order_reviews", 99224)
    ],
)

def test_parent_loader_is_idempotent(
    db_connection,
    loader,
    table_name,
    expected_count,
):
    """Replaying a parent load must not insert duplicates."""

    with db_connection.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM retail.{table_name}"
        )
        before = cur.fetchone()[0]

    inserted = loader(db_connection)

    with db_connection.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM retail.{table_name}"
        )
        after = cur.fetchone()[0]

    assert before == expected_count
    assert inserted == 0
    assert after == before


def test_orders_referential_integrity(db_connection):
    """Every typed order must reference an existing customer."""

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM retail.orders AS o
            LEFT JOIN retail.customers AS c
                ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """)

        orphan_count = cur.fetchone()[0]

    assert orphan_count == 0


def test_all_raw_orders_exist(db_connection):
    """Every raw order must exist in the typed orders table."""

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM olist_raw.orders AS raw
            WHERE NOT EXISTS (
                SELECT 1
                FROM retail.orders AS typed
                WHERE typed.order_id = raw.order_id
            )
        """)

        missing_count = cur.fetchone()[0]

    assert missing_count == 0


def test_order_items_referential_integrity(db_connection):
    """Every order item must reference valid parent records."""

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM retail.order_items AS oi
            LEFT JOIN retail.orders AS o
                ON oi.order_id = o.order_id
            LEFT JOIN retail.products AS p
                ON oi.product_id = p.product_id
            LEFT JOIN retail.sellers AS s
                ON oi.seller_id = s.seller_id
            WHERE o.order_id IS NULL
               OR p.product_id IS NULL
               OR s.seller_id IS NULL
        """)

        orphan_count = cur.fetchone()[0]

    assert orphan_count == 0


def test_all_raw_order_items_exist(db_connection):
    """Every raw order item must exist in the typed table."""

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM olist_raw.order_items AS raw
            WHERE NOT EXISTS (
                SELECT 1
                FROM retail.order_items AS typed
                WHERE typed.order_id = raw.order_id
                  AND typed.order_item_id =
                      raw.order_item_id::integer
            )
        """)

        missing_count = cur.fetchone()[0]

    assert missing_count == 0


@pytest.mark.parametrize(
    "table_name,sequence_column",
    [
        ("order_payments", "payment_sequential"),
        ("order_reviews", "review_id"),
    ],
)

def test_dependent_tables_integrity(
    db_connection, table_name, sequence_column
):
    """Check source completeness and valid order references."""

    # Table and column names come from fixed test parameters,
    # not from external user input.
    with db_connection.cursor() as cur:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM olist_raw.{table_name} AS raw
            WHERE NOT EXISTS (
                SELECT 1
                FROM retail.{table_name} AS typed
                WHERE typed.order_id = raw.order_id
                  AND typed.{sequence_column}::text =
                      raw.{sequence_column}
            )
        """)
        missing_count = cur.fetchone()[0]

        cur.execute(f"""
            SELECT COUNT(*)
            FROM retail.{table_name} AS child
            LEFT JOIN retail.orders AS parent
                ON child.order_id = parent.order_id
            WHERE parent.order_id IS NULL
        """)
        orphan_count = cur.fetchone()[0]

    assert missing_count == 0
    assert orphan_count == 0


def test_transaction_rolls_back_on_failure():
    """A failed transaction must not retain earlier inserts."""

    test_customer_id = "rollback_test_customer_001"

    # Use a separate connection so this test controls
    # the transaction lifecycle explicitly.
    with psycopg.connect(
        os.environ["DATABASE_URL"]
    ) as conn:

        with pytest.raises(psycopg.errors.NotNullViolation):
            with conn.transaction():
                with conn.cursor() as cur:
                    # First statement succeeds.
                    cur.execute("""
                        INSERT INTO retail.customers (
                            customer_id,
                            customer_unique_id,
                            customer_zip_code_prefix,
                            customer_city,
                            customer_state
                        )
                        VALUES (
                            %s, %s, %s, %s, %s
                        )
                    """, (
                        test_customer_id,
                        "rollback_test_unique_001",
                        "00000",
                        "test_city",
                        "TS",
                    ))

                    # Second statement deliberately fails
                    # because customer_unique_id is NOT NULL.
                    cur.execute("""
                        INSERT INTO retail.customers (
                            customer_id,
                            customer_unique_id,
                            customer_zip_code_prefix,
                            customer_city,
                            customer_state
                        )
                        VALUES (
                            %s, NULL, %s, %s, %s
                        )
                    """, (
                        "rollback_test_customer_002",
                        "00000",
                        "test_city",
                        "TS",
                    ))

        # The transaction has rolled back.
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM retail.customers
                WHERE customer_id = %s
            """, (test_customer_id,))

            remaining_count = cur.fetchone()[0]

        assert remaining_count == 0


def test_payment_values_match_raw_source(db_connection):
    """Typed payment amounts and installments must match raw data."""

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM olist_raw.order_payments AS raw
            JOIN retail.order_payments AS typed
                ON typed.order_id = raw.order_id
               AND typed.payment_sequential =
                   raw.payment_sequential::integer
            WHERE typed.payment_value IS DISTINCT FROM
                      raw.payment_value::numeric
               OR typed.payment_installments IS DISTINCT FROM
                      raw.payment_installments::integer
               OR typed.payment_type IS DISTINCT FROM
                      raw.payment_type
        """)

        mismatch_count = cur.fetchone()[0]

    assert mismatch_count == 0


def test_order_item_values_match_raw_source(db_connection):
    """Typed order-item values must match the raw source."""

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM olist_raw.order_items AS raw
            JOIN retail.order_items AS typed
                ON typed.order_id = raw.order_id
               AND typed.order_item_id =
                   raw.order_item_id::integer
            WHERE typed.product_id IS DISTINCT FROM
                      raw.product_id
               OR typed.seller_id IS DISTINCT FROM
                      raw.seller_id
               OR typed.shipping_limit_date IS DISTINCT FROM
                      raw.shipping_limit_date::timestamp
               OR typed.price IS DISTINCT FROM
                      raw.price::numeric
               OR typed.freight_value IS DISTINCT FROM
                      raw.freight_value::numeric
        """)

        mismatch_count = cur.fetchone()[0]

    assert mismatch_count == 0


def test_order_timestamps_match_raw_source(db_connection):
    """Order lifecycle timestamps must match the raw source."""

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM olist_raw.orders AS raw
            JOIN retail.orders AS typed
                ON typed.order_id = raw.order_id
            WHERE typed.order_purchase_timestamp
                IS DISTINCT FROM
                    NULLIF(BTRIM(raw.order_purchase_timestamp), '')::timestamp

               OR typed.order_approved_at
                IS DISTINCT FROM
                    NULLIF(BTRIM(raw.order_approved_at), '')::timestamp

               OR typed.order_delivered_carrier_date
                IS DISTINCT FROM
                    NULLIF(BTRIM(raw.order_delivered_carrier_date), '')::timestamp

               OR typed.order_delivered_customer_date
                IS DISTINCT FROM
                    NULLIF(BTRIM(raw.order_delivered_customer_date), '')::timestamp

               OR typed.order_estimated_delivery_date
                IS DISTINCT FROM
                    NULLIF(BTRIM(raw.order_estimated_delivery_date), '')::timestamp
        """)

        mismatch_count = cur.fetchone()[0]

    assert mismatch_count == 0


def test_review_values_match_raw_source(db_connection):
    """Review scores, comments and timestamps match the source."""

    with db_connection.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM olist_raw.order_reviews AS raw
            JOIN retail.order_reviews AS typed
                ON typed.review_id = raw.review_id
               AND typed.order_id = raw.order_id
            WHERE typed.review_score IS DISTINCT FROM
                      NULLIF(BTRIM(raw.review_score), '')::integer

               OR typed.review_comment_title IS DISTINCT FROM
                      NULLIF(raw.review_comment_title, '')

               OR typed.review_comment_message IS DISTINCT FROM
                      NULLIF(raw.review_comment_message, '')

               OR typed.review_creation_date IS DISTINCT FROM
                      NULLIF(BTRIM(raw.review_creation_date), '')::timestamp

               OR typed.review_answer_timestamp IS DISTINCT FROM
                      NULLIF(BTRIM(raw.review_answer_timestamp), '')::timestamp
        """)

        mismatch_count = cur.fetchone()[0]

    assert mismatch_count == 0

