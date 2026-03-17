from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery as real_bigquery

from dbt_cost.adapters.bigquery import BigQueryAdapter, get_adapter
from dbt_cost.core.adapter import CostAdapter


class TestBigQueryAdapter:
    def test_implements_cost_adapter(self):
        with patch("dbt_cost.adapters.bigquery.bigquery"):
            adapter = BigQueryAdapter()
            assert isinstance(adapter, CostAdapter)

    def test_adapter_type(self):
        with patch("dbt_cost.adapters.bigquery.bigquery"):
            adapter = BigQueryAdapter()
            assert adapter.adapter_type() == "bigquery"

    def test_dry_run_returns_bytes(self):
        with patch("dbt_cost.adapters.bigquery.bigquery") as mock_bq:
            mock_job = MagicMock()
            mock_job.total_bytes_processed = 2_400_000_000
            mock_bq.Client.return_value.query.return_value = mock_job

            adapter = BigQueryAdapter()
            result = adapter.dry_run("SELECT 1", "my-project", "my_dataset")

            assert result == 2_400_000_000

    def test_dry_run_sets_dry_run_flag(self):
        with patch("dbt_cost.adapters.bigquery.bigquery") as mock_bq:
            mock_bq.QueryJobConfig = real_bigquery.QueryJobConfig
            mock_bq.DatasetReference = real_bigquery.DatasetReference

            mock_job = MagicMock()
            mock_job.total_bytes_processed = 0
            mock_bq.Client.return_value.query.return_value = mock_job

            adapter = BigQueryAdapter()
            adapter.dry_run("SELECT 1", "my-project", "my_dataset")

            call_args = mock_bq.Client.return_value.query.call_args
            job_config = call_args[1]["job_config"]
            assert job_config.dry_run is True
            assert job_config.use_legacy_sql is False

    def test_dry_run_sets_default_dataset(self):
        with patch("dbt_cost.adapters.bigquery.bigquery") as mock_bq:
            mock_bq.QueryJobConfig = real_bigquery.QueryJobConfig
            mock_bq.DatasetReference = real_bigquery.DatasetReference

            mock_job = MagicMock()
            mock_job.total_bytes_processed = 0
            mock_bq.Client.return_value.query.return_value = mock_job

            adapter = BigQueryAdapter()
            adapter.dry_run("SELECT 1", "my-project", "my_dataset")

            call_args = mock_bq.Client.return_value.query.call_args
            job_config = call_args[1]["job_config"]
            default_ds = job_config.default_dataset
            assert default_ds.project == "my-project"
            assert default_ds.dataset_id == "my_dataset"

    def test_credentials_path_uses_service_account(self):
        with patch("dbt_cost.adapters.bigquery.bigquery") as mock_bq, \
             patch("dbt_cost.adapters.bigquery.service_account") as mock_sa:
            mock_creds = MagicMock()
            mock_sa.Credentials.from_service_account_file.return_value = mock_creds

            BigQueryAdapter(credentials_path="/path/to/key.json")

            mock_sa.Credentials.from_service_account_file.assert_called_once_with("/path/to/key.json")
            mock_bq.Client.assert_called_once_with(credentials=mock_creds)


class TestGetAdapter:
    def test_bigquery(self):
        with patch("dbt_cost.adapters.bigquery.bigquery"):
            adapter = get_adapter("bigquery")
            assert adapter.adapter_type() == "bigquery"

    def test_unsupported_raises(self):
        with pytest.raises(ValueError, match="not yet supported"):
            get_adapter("snowflake")
