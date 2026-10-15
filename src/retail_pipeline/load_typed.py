
import logging
import os

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)


def load_customers(conn: psycopg.Connection) -> int:
    """Load new customers from raw into typed tables."""

    sql = """
        INSERT INTO retail.customers (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        )
        SELECT
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        FROM olist_raw.customers
        ON CONFLICT (customer_id) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.rowcount



def load_products(conn: psycopg.Connection) -> int:
    """Load products, converting optional numeric fields."""

    sql = """
        INSERT INTO retail.products (
            product_id,
            product_category_name,
            product_name_lenght,
            product_description_lenght,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        )
        SELECT
            product_id,
            NULLIF(product_category_name, ''),
            NULLIF(product_name_lenght, '')::integer,
            NULLIF(product_description_lenght, '')::integer,
            NULLIF(product_photos_qty, '')::integer,
            NULLIF(product_weight_g, '')::integer,
            NULLIF(product_length_cm, '')::integer,
            NULLIF(product_height_cm, '')::integer,
            NULLIF(product_width_cm, '')::integer
        FROM olist_raw.products
        ON CONFLICT (product_id) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.rowcount


def load_sellers(conn: psycopg.Connection) -> int:
    """Load sellers from the raw table."""

    sql = """
        INSERT INTO retail.sellers (
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        )
        SELECT
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        FROM olist_raw.sellers
        ON CONFLICT (seller_id) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.rowcount


def load_category_translations(
    conn: psycopg.Connection,
) -> int:
    """Load Portuguese-to-English category translations."""

    sql = """
        INSERT INTO retail.category_translations (
            product_category_name,
            product_category_name_english
        )
        SELECT
            product_category_name,
            product_category_name_english
        FROM olist_raw.product_category_name_translation
        ON CONFLICT (product_category_name) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.rowcount


def load_orders(conn: psycopg.Connection) -> int:
    """Load orders after customers, converting timestamps."""

    sql = """
        INSERT INTO retail.orders (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        )
        SELECT
            order_id,
            customer_id,
            order_status,
            NULLIF(BTRIM(order_purchase_timestamp), '')::timestamp,
            NULLIF(BTRIM(order_approved_at), '')::timestamp,
            NULLIF(BTRIM(order_delivered_carrier_date), '')::timestamp,
            NULLIF(BTRIM(order_delivered_customer_date), '')::timestamp,
            NULLIF(BTRIM(order_estimated_delivery_date), '')::timestamp
        FROM olist_raw.orders
        ON CONFLICT (order_id) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.rowcount


def load_order_items(conn: psycopg.Connection) -> int:
    """Load typed order items using a composite primary key."""

    sql = """
        INSERT INTO retail.order_items (
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value
        )
        SELECT
            order_id,
            NULLIF(BTRIM(order_item_id), '')::integer,
            product_id,
            seller_id,
            NULLIF(BTRIM(shipping_limit_date), '')::timestamp,
            NULLIF(BTRIM(price), '')::numeric,
            NULLIF(BTRIM(freight_value), '')::numeric
        FROM olist_raw.order_items
        ON CONFLICT (order_id, order_item_id) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.rowcount


def load_order_payments(conn: psycopg.Connection) -> int:
    """Load typed order payments with precise numeric values."""

    sql = """
        INSERT INTO retail.order_payments (
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value
        )
        SELECT
            order_id,
            NULLIF(
                BTRIM(payment_sequential), ''
            )::integer,
            payment_type,
            NULLIF(
                BTRIM(payment_installments), ''
            )::integer,
            NULLIF(
                BTRIM(payment_value), ''
            )::numeric
        FROM olist_raw.order_payments
        ON CONFLICT (order_id, payment_sequential)
        DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.rowcount


def load_order_reviews(conn: psycopg.Connection) -> int:
    """Load reviews with nullable comments and typed timestamps."""

    sql = """
        INSERT INTO retail.order_reviews (
            review_id,
            order_id,
            review_score,
            review_comment_title,
            review_comment_message,
            review_creation_date,
            review_answer_timestamp
        )
        SELECT
            review_id,
            order_id,
            NULLIF(BTRIM(review_score), '')::integer,
            NULLIF(review_comment_title, ''),
            NULLIF(review_comment_message, ''),
            NULLIF(BTRIM(review_creation_date), '')::timestamp,
            NULLIF(BTRIM(review_answer_timestamp), '')::timestamp
        FROM olist_raw.order_reviews
        ON CONFLICT (review_id, order_id)
        DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.rowcount


def main() -> None:
    database_url = os.environ["DATABASE_URL"]

    with psycopg.connect(database_url) as conn:
        results = {
            "customers": load_customers(conn),
            "products": load_products(conn),
            "sellers": load_sellers(conn),
            "category_translations":load_category_translations(conn),
            "orders": load_orders(conn),
            "order_items": load_order_items(conn),
            "order_payments": load_order_payments(conn),
            "order_reviews": load_order_reviews(conn)
        }

    for table_name, inserted in results.items():
        logger.info(
            "%s inserted: %s",
            table_name,
            inserted,
        )


if __name__ == "__main__":
    main()

