"""Unit tests for customer CDC Bronze serialization."""

import pytest
import uuid
import json
from datetime import datetime, timezone
from retail_pipeline.load_customer_cdc_bronze import (
    ingest_customer_cdc,
    load_customer_cdc_batch,
    serialize_customer_cdc_event,
)
from unittest.mock import (
    MagicMock,
    patch
)
from google.cloud import bigquery
from contextlib import contextmanager

@pytest.fixture(autouse=True)
def mock_customer_cdc_ingestion_lock():
    """Prevent ingestion unit tests from opening PostgreSQL connections."""
    with patch(
        "retail_pipeline.load_customer_cdc_bronze.customer_cdc_ingestion_lock"
    ) as mock_lock:
        yield mock_lock


def test_serialize_customer_cdc_event():
    """A PostgreSQL CDC event is converted into a Bronze record."""

    event_uuid = uuid.uuid4()
    event_timestamp = datetime(
        2026, 10, 20, 10, 30, tzinfo=timezone.utc
    )
    recorded_at = datetime(
        2026, 10, 20, 10, 31, tzinfo=timezone.utc
    )

    event = {
        "event_id": 1,
        "event_uuid": event_uuid,
        "customer_unique_id": "customer_001",
        "customer_id": None,
        "operation": "U",
        "event_timestamp": event_timestamp,
        "recorded_at": recorded_at,
        "before_data": {"customer_city": "sao paulo"},
        "after_data": {"customer_city": "rio de janeiro"},
        "schema_version": 1,
    }

    result = serialize_customer_cdc_event(event)

    assert result["event_id"] == 1
    assert result["event_uuid"] == str(event_uuid)
    assert result["customer_unique_id"] == "customer_001"
    assert result["customer_id"] is None
    assert result["operation"] == "U"

    assert result["event_timestamp"] == event_timestamp.isoformat()
    assert result["recorded_at"] == recorded_at.isoformat()

    assert json.loads(result["before_data"]) == {
        "customer_city": "sao paulo"
    }
    assert json.loads(result["after_data"]) == {
        "customer_city": "rio de janeiro"
    }

    assert result["schema_version"] == 1

    ingested_at = datetime.fromisoformat(result["ingested_at"])
    assert ingested_at.tzinfo is not None

def test_load_customer_cdc_batch_success():
    """A successful BigQuery load appends records and waits for completion."""

    client = MagicMock(spec=bigquery.Client)
    load_job = MagicMock()
    load_job.errors = None
    load_job.output_rows = 2

    client.load_table_from_json.return_value = load_job

    records = [
        {"event_id": 1, "event_uuid": "event-001"},
        {"event_id": 2, "event_uuid": "event-002"},
    ]

    result = load_customer_cdc_batch(client, records)

    assert result == 2

    client.load_table_from_json.assert_called_once()

    args, kwargs = client.load_table_from_json.call_args

    assert args[0] == records
    assert args[1] == (
        "retail-data-platform-512111."
        "bronze.customer_cdc_events"
    )

    assert kwargs["location"] == "europe-west2"

    job_config = kwargs["job_config"]

    assert job_config.write_disposition == (
        bigquery.WriteDisposition.WRITE_APPEND
    )

    assert job_config.source_format == (
        bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    assert job_config.max_bad_records == 0

    load_job.result.assert_called_once_with()

def test_load_customer_cdc_batch_empty():
    """An empty batch must not create a BigQuery load job."""

    client = MagicMock(spec=bigquery.Client)

    result = load_customer_cdc_batch(client, [])

    assert result == 0

    client.load_table_from_json.assert_not_called()

def test_load_customer_cdc_batch_failure():
    """A failed BigQuery load must propagate the error."""

    client = MagicMock(spec=bigquery.Client)
    load_job = MagicMock()

    load_job.result.side_effect = RuntimeError(
        "Simulated BigQuery load failure"
    )

    client.load_table_from_json.return_value = load_job

    records = [
        {"event_id": 1, "event_uuid": "event-001"},
    ]

    with pytest.raises(
        RuntimeError,
        match="Simulated BigQuery load failure",
    ):
        load_customer_cdc_batch(client, records)

    client.load_table_from_json.assert_called_once()
    load_job.result.assert_called_once_with()

def test_ingest_customer_cdc_does_not_acknowledge_failed_load():
    """Failed BigQuery delivery must not acknowledge PostgreSQL events."""

    client = MagicMock(spec=bigquery.Client)

    event = {
        "event_id": 1,
        "event_uuid": uuid.uuid4(),
        "customer_unique_id": "customer_001",
        "customer_id": None,
        "operation": "I",
        "event_timestamp": datetime.now(timezone.utc),
        "recorded_at": datetime.now(timezone.utc),
        "before_data": None,
        "after_data": {"customer_city": "london"},
        "schema_version": 1,
    }

    with (
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "fetch_pending_customer_events",
            return_value=[event],
        ) as mock_fetch,
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "load_customer_cdc_batch",
            side_effect=RuntimeError("BigQuery unavailable"),
        ) as mock_load,
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "acknowledge_customer_event",
        ) as mock_acknowledge,
    ):
        with pytest.raises(
            RuntimeError,
            match="BigQuery unavailable",
        ):
            ingest_customer_cdc(
                database_url="mock_database_url",
                client=client,
            )

        mock_fetch.assert_called_once_with(
            "mock_database_url",
            batch_size=100,
        )

        mock_load.assert_called_once()
        mock_acknowledge.assert_not_called()

def test_ingest_customer_cdc_acknowledges_successful_load():
    """Acknowledge every event after successful BigQuery delivery."""

    client = MagicMock(spec=bigquery.Client)

    first_uuid = uuid.uuid4()
    second_uuid = uuid.uuid4()

    events = [
        {
            "event_id": event_id,
            "event_uuid": event_uuid,
            "customer_unique_id": f"customer_{event_id}",
            "customer_id": None,
            "operation": "I",
            "event_timestamp": datetime.now(timezone.utc),
            "recorded_at": datetime.now(timezone.utc),
            "before_data": None,
            "after_data": {"customer_city": "london"},
            "schema_version": 1,
        }
        for event_id, event_uuid in [
            (1, first_uuid),
            (2, second_uuid),
        ]
    ]

    with (
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "fetch_pending_customer_events",
            return_value=events,
        ) as mock_fetch,
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "load_customer_cdc_batch",
            return_value=2,
        ) as mock_load,
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "acknowledge_customer_event",
        ) as mock_acknowledge,
    ):
        result = ingest_customer_cdc(
            database_url="mock_database_url",
            client=client,
            batch_size=2,
        )

        assert result == 2

        mock_fetch.assert_called_once_with(
            "mock_database_url",
            batch_size=2,
        )

        mock_load.assert_called_once()

        loaded_records = mock_load.call_args.args[1]

        assert len(loaded_records) == 2
        assert loaded_records[0]["event_uuid"] == str(first_uuid)
        assert loaded_records[1]["event_uuid"] == str(second_uuid)

        assert mock_acknowledge.call_count == 2

        assert mock_acknowledge.call_args_list[0].args == (
            "mock_database_url",
            first_uuid,
        )

        assert mock_acknowledge.call_args_list[1].args == (
            "mock_database_url",
            second_uuid,
        )

def test_ingest_customer_cdc_acknowledgement_failure():
    """A failed acknowledgement must propagate after successful loading."""

    client = MagicMock(spec=bigquery.Client)

    event = {
        "event_id": 1,
        "event_uuid": uuid.uuid4(),
        "customer_unique_id": "customer_001",
        "customer_id": None,
        "operation": "I",
        "event_timestamp": datetime.now(timezone.utc),
        "recorded_at": datetime.now(timezone.utc),
        "before_data": None,
        "after_data": {"customer_city": "london"},
        "schema_version": 1,
    }

    with (
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "fetch_pending_customer_events",
            return_value=[event],
        ),
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "load_customer_cdc_batch",
            return_value=1,
        ) as mock_load,
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "acknowledge_customer_event",
            side_effect=RuntimeError("PostgreSQL unavailable"),
        ) as mock_acknowledge,
    ):
        with pytest.raises(
            RuntimeError,
            match="PostgreSQL unavailable",
        ):
            ingest_customer_cdc(
                database_url="mock_database_url",
                client=client,
            )

        mock_load.assert_called_once()
        mock_acknowledge.assert_called_once_with(
            "mock_database_url",
            event["event_uuid"],
        )

def test_load_customer_cdc_batch_row_count_mismatch():
    """Reject a BigQuery load with an unexpected row count."""

    client = MagicMock(spec=bigquery.Client)

    load_job = MagicMock()
    load_job.errors = None
    load_job.output_rows = 1

    client.load_table_from_json.return_value = load_job

    records = [
        {"event_id": 1, "event_uuid": "event-001"},
        {"event_id": 2, "event_uuid": "event-002"},
    ]

    with pytest.raises(
        RuntimeError,
        match="BigQuery row count mismatch",
    ):
        load_customer_cdc_batch(client, records)

    client.load_table_from_json.assert_called_once()
    load_job.result.assert_called_once_with()

def test_ingest_customer_cdc_row_count_mismatch_prevents_acknowledgement():
    """Do not acknowledge events when delivery counts differ."""

    client = MagicMock(spec=bigquery.Client)

    events = [
        {
            "event_id": event_id,
            "event_uuid": uuid.uuid4(),
            "customer_unique_id": f"customer_{event_id}",
            "customer_id": None,
            "operation": "I",
            "event_timestamp": datetime.now(timezone.utc),
            "recorded_at": datetime.now(timezone.utc),
            "before_data": None,
            "after_data": {"customer_city": "london"},
            "schema_version": 1,
        }
        for event_id in (1, 2)
    ]

    with (
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "fetch_pending_customer_events",
            return_value=events,
        ),
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "load_customer_cdc_batch",
            return_value=1,
        ) as mock_load,
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "acknowledge_customer_event",
        ) as mock_acknowledge,
    ):
        with pytest.raises(
            RuntimeError,
            match="BigQuery loaded count does not match",
        ):
            ingest_customer_cdc(
                database_url="mock_database_url",
                client=client,
            )

        mock_load.assert_called_once()
        mock_acknowledge.assert_not_called()

def test_ingest_customer_cdc_no_pending_events():
    """No pending events must produce no loads or acknowledgements."""

    client = MagicMock(spec=bigquery.Client)

    with (
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "fetch_pending_customer_events",
            return_value=[],
        ) as mock_fetch,
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "load_customer_cdc_batch",
        ) as mock_load,
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "acknowledge_customer_event",
        ) as mock_acknowledge,
    ):
        result = ingest_customer_cdc(
            database_url="mock_database_url",
            client=client,
        )

        assert result == 0

        mock_fetch.assert_called_once_with(
            "mock_database_url",
            batch_size=100,
        )

        mock_load.assert_not_called()
        mock_acknowledge.assert_not_called()


def test_ingest_customer_cdc_holds_lock_until_acknowledgement(
    mock_customer_cdc_ingestion_lock,
):
    """Keep the advisory lock throughout extraction, loading and acknowledgement."""

    execution_order = []

    @contextmanager
    def tracking_lock(database_url):
        execution_order.append("lock_acquired")
        try:
            yield
        finally:
            execution_order.append("lock_released")

    mock_customer_cdc_ingestion_lock.side_effect = tracking_lock

    event = {
        "event_uuid": uuid.uuid4(),
    }

    def mock_fetch(*args, **kwargs):
        execution_order.append("extract")
        return [event]

    def mock_load(*args, **kwargs):
        execution_order.append("load")
        return 1

    def mock_acknowledge(*args, **kwargs):
        execution_order.append("acknowledge")
        return True

    with (
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "fetch_pending_customer_events",
            side_effect=mock_fetch,
        ),
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "serialize_customer_cdc_event",
            return_value={"event_uuid": str(event["event_uuid"])},
        ),
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "load_customer_cdc_batch",
            side_effect=mock_load,
        ),
        patch(
            "retail_pipeline.load_customer_cdc_bronze."
            "acknowledge_customer_event",
            side_effect=mock_acknowledge,
        ),
    ):
        result = ingest_customer_cdc(
            "test_database_url",
            MagicMock(),
        )

    assert result == 1

    assert execution_order == [
        "lock_acquired",
        "extract",
        "load",
        "acknowledge",
        "lock_released",
    ]
