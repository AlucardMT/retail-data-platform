"""Unit tests for guarded historical customer CDC replay."""

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery

from retail_pipeline.replay_customer_cdc import (
    EXPECTED_SCHEMA,
    LIVE_TABLE,
    replay_customer_cdc,
    validate_destination,
    verify_recovery_table,
)


DESTINATION = (
    "retail-data-platform-512111."
    "bronze.customer_cdc_events_recovery_test"
)


@pytest.fixture
def historical_events():
    timestamp = datetime(
        2026, 10, 23, 10, 58, 26, 999539,
        tzinfo=timezone.utc,
    )

    return [
        {
            "event_id": 69,
            "event_uuid": uuid.UUID(
                "c35bbf9d-ff64-49c5-8e6c-3a8e3d2bf120"
            ),
            "customer_unique_id": "customer_test",
            "customer_id": None,
            "operation": "U",
            "event_timestamp": timestamp,
            "recorded_at": timestamp,
            "before_data": {"customer_city": "old city"},
            "after_data": {"customer_city": "new city"},
            "schema_version": 1,
        }
    ]


def mock_recovery_table():
    table = MagicMock()
    table.table_type = "TABLE"
    table.location = "europe-west2"
    table.schema = [
        bigquery.SchemaField(name, field_type, mode=mode)
        for name, field_type, mode in EXPECTED_SCHEMA
    ]
    return table


def test_destination_must_be_explicit():
    with pytest.raises(ValueError):
        validate_destination(None)


def test_live_bronze_destination_is_rejected():
    with pytest.raises(ValueError, match="prohibited"):
        validate_destination(LIVE_TABLE)


@pytest.mark.parametrize(
    "destination",
    [
        "other-project.bronze.recovery",
        "retail-data-platform-512111.silver.recovery",
        "retail-data-platform-512111.bronze.bad-name",
        "retail-data-platform-512111.bronze.recovery;DROP",
    ],
)
def test_invalid_destinations_are_rejected(destination):
    with pytest.raises(ValueError):
        validate_destination(destination)


def test_valid_recovery_destination():
    assert validate_destination(DESTINATION) == DESTINATION


def test_dry_run_does_not_contact_bigquery(historical_events):
    client = MagicMock(spec=bigquery.Client)

    with patch(
        "retail_pipeline.replay_customer_cdc.fetch_historical_events",
        return_value=historical_events,
    ):
        count = replay_customer_cdc(
            "mock_database_url",
            client,
            DESTINATION,
        )

    assert count == 1
    client.get_table.assert_not_called()
    client.query.assert_not_called()
    client.load_table_from_json.assert_not_called()


def test_dry_run_rejects_duplicate_source_uuids(historical_events):
    events = historical_events + [historical_events[0].copy()]

    with patch(
        "retail_pipeline.replay_customer_cdc.fetch_historical_events",
        return_value=events,
    ):
        with pytest.raises(ValueError, match="Duplicate event UUIDs"):
            replay_customer_cdc(
                "mock_database_url",
                None,
                DESTINATION,
            )


def test_recovery_table_must_be_empty():
    client = MagicMock(spec=bigquery.Client)
    client.get_table.return_value = mock_recovery_table()
    client.query.return_value.result.return_value = [
        {"has_rows": True}
    ]

    with pytest.raises(ValueError, match="must be empty"):
        verify_recovery_table(client, DESTINATION)


def test_recovery_table_schema_must_match():
    client = MagicMock(spec=bigquery.Client)
    table = mock_recovery_table()
    table.schema = table.schema[:-1]
    client.get_table.return_value = table

    with pytest.raises(ValueError, match="schema"):
        verify_recovery_table(client, DESTINATION)

    client.query.assert_not_called()


def test_recovery_table_location_must_match():
    client = MagicMock(spec=bigquery.Client)
    table = mock_recovery_table()
    table.location = "US"
    client.get_table.return_value = table

    with pytest.raises(ValueError, match="location"):
        verify_recovery_table(client, DESTINATION)


def test_successful_replay_preserves_source_identity(historical_events):
    client = MagicMock(spec=bigquery.Client)
    job = MagicMock()
    job.errors = None
    job.output_rows = 1
    client.load_table_from_json.return_value = job

    with (
        patch(
            "retail_pipeline.replay_customer_cdc.fetch_historical_events",
            return_value=historical_events,
        ),
        patch(
            "retail_pipeline.replay_customer_cdc.verify_recovery_table",
        ),
        patch(
            "retail_pipeline.replay_customer_cdc.reconcile_recovery_events",
        ),
    ):
        count = replay_customer_cdc(
            "mock_database_url",
            client,
            DESTINATION,
            execute=True,
        )

    assert count == 1
    args, kwargs = client.load_table_from_json.call_args
    record = args[0][0]

    assert args[1] == DESTINATION
    assert record["event_id"] == 69
    assert record["event_uuid"] == str(
        historical_events[0]["event_uuid"]
    )
    assert record["event_timestamp"] == (
        historical_events[0]["event_timestamp"].isoformat()
    )
    assert record["before_data"] == {
        "customer_city": "old city"
    }
    assert record["after_data"] == {
        "customer_city": "new city"
    }

    assert kwargs["location"] == "europe-west2"
    assert kwargs["job_config"].write_disposition == (
        bigquery.WriteDisposition.WRITE_EMPTY
    )

    schema = {
        field.name: field
        for field in kwargs["job_config"].schema
    }

    assert len(schema) == 11
    assert schema["before_data"].field_type == "JSON"
    assert schema["after_data"].field_type == "JSON"
    assert schema["before_data"].mode == "NULLABLE"
    assert schema["after_data"].mode == "NULLABLE"

    job.result.assert_called_once_with()


def test_failed_replay_propagates_error(historical_events):
    client = MagicMock(spec=bigquery.Client)
    client.load_table_from_json.return_value.result.side_effect = (
        RuntimeError("Simulated load failure")
    )

    with (
        patch(
            "retail_pipeline.replay_customer_cdc.fetch_historical_events",
            return_value=historical_events,
        ),
        patch(
            "retail_pipeline.replay_customer_cdc.verify_recovery_table",
        ),
        patch(
            "retail_pipeline.replay_customer_cdc.reconcile_recovery_events",
        ),
    ):
        with pytest.raises(
            RuntimeError,
            match="Simulated load failure",
        ):
            replay_customer_cdc(
                "mock_database_url",
                client,
                DESTINATION,
                execute=True,
            )


def test_empty_history_creates_no_load_job():
    client = MagicMock(spec=bigquery.Client)

    with (
        patch(
            "retail_pipeline.replay_customer_cdc.fetch_historical_events",
            return_value=[],
        ),
        patch(
            "retail_pipeline.replay_customer_cdc.verify_recovery_table",
        ),
        patch(
            "retail_pipeline.replay_customer_cdc.reconcile_recovery_events",
        ),
    ):
        assert replay_customer_cdc(
            "mock_database_url",
            client,
            DESTINATION,
            execute=True,
        ) == 0

    client.load_table_from_json.assert_not_called()


def test_replay_never_calls_acknowledgement(historical_events):
    client = MagicMock(spec=bigquery.Client)

    with (
        patch(
            "retail_pipeline.replay_customer_cdc.fetch_historical_events",
            return_value=historical_events,
        ),
        patch(
            "retail_pipeline.replay_customer_cdc.verify_recovery_table",
        ),
        patch(
            "retail_pipeline.replay_customer_cdc.reconcile_recovery_events",
        ),
        patch(
            "retail_pipeline.extract_customer_cdc.acknowledge_customer_event"
        ) as acknowledge,
    ):
        job = client.load_table_from_json.return_value
        job.errors = None
        job.output_rows = 1

        replay_customer_cdc(
            "mock_database_url",
            client,
            DESTINATION,
            execute=True,
        )

        acknowledge.assert_not_called()


def test_unrelated_bronze_table_is_rejected():
    with pytest.raises(ValueError):
        validate_destination(
            "retail-data-platform-512111.bronze.customer_profiles"
        )


def test_recovery_prefix_requires_a_suffix():
    with pytest.raises(ValueError):
        validate_destination(
            "retail-data-platform-512111.bronze.customer_cdc_events_recovery_"
        )


def test_historical_extraction_uses_read_only_transaction():
    from retail_pipeline.replay_customer_cdc import (
        fetch_historical_events,
    )

    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchone.return_value = {
        "current_database": "retail_platform_test"
    }
    cursor.fetchall.return_value = [
        {"event_id": 69, "event_uuid": "historical-event"}
    ]

    with patch(
        "retail_pipeline.replay_customer_cdc.psycopg.connect",
        return_value=connection,
    ) as connect:
        events = fetch_historical_events("mock_database_url")

    assert events == [
        {"event_id": 69, "event_uuid": "historical-event"}
    ]

    connect.assert_called_once()
    cursor.execute.assert_any_call("SET TRANSACTION READ ONLY")

    sql_statements = [
        call.args[0]
        for call in cursor.execute.call_args_list
    ]

    assert any(
        "FROM cdc.customer_events" in statement
        for statement in sql_statements
    )
    assert all(
        "customer_event_extractions" not in statement
        for statement in sql_statements
    )


def test_historical_extraction_rejects_wrong_database():
    from retail_pipeline.replay_customer_cdc import (
        fetch_historical_events,
    )

    connection = MagicMock()
    cursor = MagicMock()

    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchone.return_value = {
        "current_database": "retail"
    }

    with patch(
        "retail_pipeline.replay_customer_cdc.psycopg.connect",
        return_value=connection,
    ):
        with pytest.raises(ValueError, match="retail_platform_test"):
            fetch_historical_events("mock_database_url")

    cursor.fetchall.assert_not_called()


def test_reconciliation_matching_identities(historical_events):
    from retail_pipeline.replay_customer_cdc import (
        reconcile_recovery_events,
    )

    client = MagicMock(spec=bigquery.Client)
    client.query.return_value.result.return_value = [
        {
            "event_id": 69,
            "event_uuid": str(historical_events[0]["event_uuid"]),
        }
    ]

    reconcile_recovery_events(client, DESTINATION, historical_events)

    client.query.assert_called_once()


def test_reconciliation_rejects_missing_event(historical_events):
    from retail_pipeline.replay_customer_cdc import (
        reconcile_recovery_events,
    )

    client = MagicMock(spec=bigquery.Client)
    client.query.return_value.result.return_value = []

    with pytest.raises(RuntimeError, match="reconciliation failed"):
        reconcile_recovery_events(
            client, DESTINATION, historical_events
        )


def test_reconciliation_rejects_unexpected_event(historical_events):
    from retail_pipeline.replay_customer_cdc import (
        reconcile_recovery_events,
    )

    client = MagicMock(spec=bigquery.Client)
    client.query.return_value.result.return_value = [
        {
            "event_id": 69,
            "event_uuid": str(historical_events[0]["event_uuid"]),
        },
        {
            "event_id": 999,
            "event_uuid": "unexpected-event",
        },
    ]

    with pytest.raises(RuntimeError, match="reconciliation failed"):
        reconcile_recovery_events(
            client, DESTINATION, historical_events
        )


def test_reconciliation_rejects_duplicate_destination_rows(
    historical_events,
):
    from retail_pipeline.replay_customer_cdc import (
        reconcile_recovery_events,
    )

    client = MagicMock(spec=bigquery.Client)
    row = {
        "event_id": 69,
        "event_uuid": str(historical_events[0]["event_uuid"]),
    }
    client.query.return_value.result.return_value = [row, row.copy()]

    with pytest.raises(RuntimeError, match="reconciliation failed"):
        reconcile_recovery_events(
            client, DESTINATION, historical_events
        )


def test_reconciliation_rejects_wrong_uuid(historical_events):
    from retail_pipeline.replay_customer_cdc import (
        reconcile_recovery_events,
    )

    client = MagicMock(spec=bigquery.Client)
    client.query.return_value.result.return_value = [
        {
            "event_id": 69,
            "event_uuid": "wrong-uuid",
        }
    ]

    with pytest.raises(RuntimeError, match="reconciliation failed"):
        reconcile_recovery_events(
            client, DESTINATION, historical_events
        )

