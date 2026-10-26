"""Unit tests for read-only customer CDC Bronze quality checks."""

from unittest.mock import MagicMock

from google.cloud import bigquery

from retail_pipeline.check_customer_cdc_bronze_quality import (
    check_customer_cdc_bronze_quality,
)


def test_check_customer_cdc_bronze_quality_success():
    """Return quality metrics from a mocked BigQuery query result."""

    client = MagicMock(spec=bigquery.Client)

    expected_metrics = {
        "total_rows": 2,
        "distinct_events": 2,
        "duplicate_rows": 0,
        "missing_identifiers": 0,
        "invalid_json_structure": 1,
        "contract_violations": 1,
    }

    mock_row = MagicMock()
    mock_row.items.return_value = expected_metrics.items()

    query_job = MagicMock()
    query_job.result.return_value = [mock_row]

    client.query.return_value = query_job

    result = check_customer_cdc_bronze_quality(client)

    assert result == expected_metrics

    client.query.assert_called_once()

    args, kwargs = client.query.call_args

    query = args[0]

    assert "SELECT" in query.upper()
    assert (
        "retail-data-platform-512111."
        "bronze.customer_cdc_events"
    ) in query

    assert kwargs["location"] == "europe-west2"

    query_job.result.assert_called_once_with()


def test_check_customer_cdc_bronze_quality_rejects_empty_result():
    """An unexpected empty BigQuery result must raise an error."""

    import pytest

    client = MagicMock(spec=bigquery.Client)

    query_job = MagicMock()
    query_job.result.return_value = []

    client.query.return_value = query_job

    with pytest.raises(
        RuntimeError,
        match="Expected exactly one Bronze quality summary row",
    ):
        check_customer_cdc_bronze_quality(client)

    client.query.assert_called_once()
    query_job.result.assert_called_once_with()
