
"""Simulate customer address changes using a transactional outbox."""

import json
import uuid

import psycopg


def update_customer_address(
    database_url: str,
    customer_unique_id: str,
    new_zip_code_prefix: str,
    new_city: str,
    new_state: str,
    event_uuid: uuid.UUID,
) -> bool:
    """Atomically update a customer profile and append a CDC event.

    Returns True when a new change is applied.
    Returns False when an identical event was already processed.

    This function owns the database connection and transaction.
    """

    with psycopg.connect(database_url) as conn:
        with conn.transaction():
            return _update_customer_address_in_transaction(
                conn=conn,
                customer_unique_id=customer_unique_id,
                new_zip_code_prefix=new_zip_code_prefix,
                new_city=new_city,
                new_state=new_state,
                event_uuid=event_uuid,
            )


def _update_customer_address_in_transaction(
    conn: psycopg.Connection,
    customer_unique_id: str,
    new_zip_code_prefix: str,
    new_city: str,
    new_state: str,
    event_uuid: uuid.UUID,
) -> bool:
    """Perform CDC writes using a caller-managed transaction.

    The caller is responsible for committing or rolling back.
    This function never commits the supplied connection.
    """

    expected_after = {
        "customer_zip_code_prefix": new_zip_code_prefix,
        "customer_city": new_city,
        "customer_state": new_state,
    }

    def check_existing_event(cur) -> bool:
        """Validate a previously processed event, if one exists."""

        cur.execute(
            """
            SELECT
                customer_unique_id,
                operation,
                after_data
            FROM cdc.customer_events
            WHERE event_uuid = %s
            """,
            (event_uuid,),
        )

        existing_event = cur.fetchone()

        if existing_event is None:
            return False

        existing_customer_id, operation, stored_after = existing_event

        if (
            existing_customer_id != customer_unique_id
            or operation != "U"
            or stored_after != expected_after
        ):
            raise ValueError(
                "Event UUID reused with different change data"
            )

        return True

    with conn.cursor() as cur:

        # Safety: never modify the development database.
        cur.execute("SELECT current_database()")
        database_name = cur.fetchone()[0]

        if database_name != "retail_platform_test":
            raise RuntimeError(
                f"Refusing to modify database: {database_name}"
            )

        # First duplicate check: handles ordinary retries,
        # including retries after a customer becomes inactive.
        if check_existing_event(cur):
            return False

        # Lock this customer to serialize concurrent updates.
        cur.execute(
            """
            SELECT
                customer_zip_code_prefix,
                customer_city,
                customer_state
            FROM retail.customer_profiles
            WHERE customer_unique_id = %s
              AND is_deleted = FALSE
            FOR UPDATE
            """,
            (customer_unique_id,),
        )

        row = cur.fetchone()

        if row is None:
            raise ValueError(
                f"Active customer not found: {customer_unique_id}"
            )

        # Second duplicate check: another transaction may have
        # committed the event while we waited for the row lock.
        if check_existing_event(cur):
            return False

        before_data = {
            "customer_zip_code_prefix": row[0],
            "customer_city": row[1],
            "customer_state": row[2],
        }

        after_data = expected_after

        # Update the operational customer profile.
        cur.execute(
            """
            UPDATE retail.customer_profiles
            SET
                customer_zip_code_prefix = %s,
                customer_city = %s,
                customer_state = %s,
                updated_at = NOW()
            WHERE customer_unique_id = %s
            """,
            (
                new_zip_code_prefix,
                new_city,
                new_state,
                customer_unique_id,
            ),
        )

        # Append the corresponding event in the same transaction.
        cur.execute(
            """
            INSERT INTO cdc.customer_events (
                event_uuid,
                customer_unique_id,
                operation,
                event_timestamp,
                before_data,
                after_data
            )
            VALUES (
                %s,
                %s,
                'U',
                NOW(),
                %s::jsonb,
                %s::jsonb
            )
            """,
            (
                event_uuid,
                customer_unique_id,
                json.dumps(before_data),
                json.dumps(after_data),
            ),
        )

    return True
