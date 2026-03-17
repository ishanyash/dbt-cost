from __future__ import annotations

from google.cloud import bigquery
from google.oauth2 import service_account

from dbt_cost.core.adapter import CostAdapter


class BigQueryAdapter(CostAdapter):
    def __init__(self, credentials_path: str | None = None):
        if credentials_path:
            creds = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = bigquery.Client(credentials=creds)
        else:
            self.client = bigquery.Client()  # Uses ADC

    def dry_run(self, sql: str, project: str, dataset: str) -> int:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_legacy_sql=False)
        job_config.default_dataset = bigquery.DatasetReference(project, dataset)
        query_job = self.client.query(sql, job_config=job_config)
        return int(query_job.total_bytes_processed)

    def adapter_type(self) -> str:
        return "bigquery"


def get_adapter(adapter_type: str, credentials_path: str | None = None) -> CostAdapter:
    if adapter_type == "bigquery":
        return BigQueryAdapter(credentials_path)
    raise ValueError(
        f"Adapter '{adapter_type}' is not yet supported. "
        f"Supported adapters: bigquery."
    )
