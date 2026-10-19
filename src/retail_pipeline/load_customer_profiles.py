"""Initialize operational customer profiles from the Olist snapshot."""
import argparse
import os
import psycopg

BASELINE_INSERT_SQL = """
WITH ranked_customers AS (
    SELECT
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY
                o.order_purchase_timestamp DESC NULLS LAST,
                c.customer_id DESC
        ) AS rn
    FROM retail.customers AS c
    LEFT JOIN retail.orders AS o
        ON o.customer_id = c.customer_id
)
INSERT INTO retail.customer_profiles (
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
SELECT
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM ranked_customers
WHERE rn = 1
ON CONFLICT (customer_unique_id) DO NOTHING;
"""

def load_baseline(database_url: str) -> int:
    """Insert missing customer profiles in one transaction."""

    with psycopg.connect(database_url) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                database_name = cur.fetchone()[0]

                if database_name != "retail_platform_test":
                    raise RuntimeError(
                        f"Refusing to modify database: {database_name}"
                    )

                cur.execute(BASELINE_INSERT_SQL)
                inserted_rows = cur.rowcount

    return inserted_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Initialize operational customer profiles"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the baseline load against the test database",
    )
    args = parser.parse_args()

    if not args.execute:
        print("Dry run: no database changes made.")
        return

    database_url = os.environ.get("TEST_DATABASE_URL")

    if not database_url:
        raise RuntimeError("TEST_DATABASE_URL is not configured")

    inserted_rows = load_baseline(database_url)

    print(f"Inserted customer profiles: {inserted_rows}")


if __name__ == "__main__":
    main()
