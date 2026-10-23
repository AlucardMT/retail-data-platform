
"""Unit tests for the customer CDC ingestion lock."""
import pytest

from unittest.mock import MagicMock, patch

from retail_pipeline.ingestion_lock import (
    CUSTOMER_CDC_LOCK_ID,
    customer_cdc_ingestion_lock,
)


def test_customer_cdc_ingestion_lock_success():
    """Acquire and release the advisory lock successfully."""

    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value

    cur.fetchone.side_effect = [
        ("retail_platform_test",),
        (True,),
    ]

    with patch(
        "retail_pipeline.ingestion_lock.psycopg.connect"
    ) as mock_connect:
        mock_connect.return_value.__enter__.return_value = conn

        with customer_cdc_ingestion_lock("test_database_url"):
            cur.execute.assert_any_call(
                "SELECT pg_try_advisory_lock(%s)",
                (CUSTOMER_CDC_LOCK_ID,),
            )

        cur.execute.assert_any_call(
            "SELECT pg_advisory_unlock(%s)",
            (CUSTOMER_CDC_LOCK_ID,),
        )

        mock_connect.assert_called_once_with(
            "test_database_url",
            autocommit=True,
        )


def test_customer_cdc_ingestion_lock_already_held():
    """Reject a second ingestion run when the lock is unavailable."""

    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value

    cur.fetchone.side_effect = [
        ("retail_platform_test",),
        (False,),
    ]

    with patch(
        "retail_pipeline.ingestion_lock.psycopg.connect"
    ) as mock_connect:
        mock_connect.return_value.__enter__.return_value = conn

        with pytest.raises(
            RuntimeError,
            match="Another customer CDC ingestion run is already active",
        ):
            with customer_cdc_ingestion_lock("test_database_url"):
                pytest.fail("Ingestion must not start without the lock")

        cur.execute.assert_any_call(
            "SELECT pg_try_advisory_lock(%s)",
            (CUSTOMER_CDC_LOCK_ID,),
        )

        # We must not unlock a lock that we never acquired.
        unlock_calls = [
            call
            for call in cur.execute.call_args_list
            if "pg_advisory_unlock" in call.args[0]
        ]

        assert unlock_calls == []


def test_customer_cdc_ingestion_lock_released_on_exception():
    """Release the advisory lock when ingestion raises an exception."""

    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value

    cur.fetchone.side_effect = [
        ("retail_platform_test",),
        (True,),
    ]

    with patch(
        "retail_pipeline.ingestion_lock.psycopg.connect"
    ) as mock_connect:
        mock_connect.return_value.__enter__.return_value = conn

        with pytest.raises(
            RuntimeError,
            match="Simulated ingestion failure",
        ):
            with customer_cdc_ingestion_lock("test_database_url"):
                raise RuntimeError("Simulated ingestion failure")

        cur.execute.assert_any_call(
            "SELECT pg_advisory_unlock(%s)",
            (CUSTOMER_CDC_LOCK_ID,),
        )

        mock_connect.return_value.__exit__.assert_called_once()


