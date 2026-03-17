from pathlib import Path

import pytest

from dbt_cost.core.adapter import CostAdapter


@pytest.fixture
def small_manifest_path() -> Path:
    return Path(__file__).parent / "fixtures" / "small_manifest.json"


@pytest.fixture
def graph(small_manifest_path: Path):
    from dbt_lens.core.manifest import load_manifest
    return load_manifest(small_manifest_path)


class MockAdapter(CostAdapter):
    """Returns predefined byte counts for testing."""

    def __init__(self, default_bytes: int = 1_000_000_000, model_bytes: dict[str, int] | None = None):
        self.default_bytes = default_bytes
        self.model_bytes = model_bytes or {}
        self.calls: list[tuple[str, str, str]] = []

    def dry_run(self, sql: str, project: str, dataset: str) -> int:
        self.calls.append((sql, project, dataset))
        for model_name, byte_count in self.model_bytes.items():
            if model_name in sql:
                return byte_count
        return self.default_bytes

    def adapter_type(self) -> str:
        return "bigquery"


@pytest.fixture
def mock_adapter() -> MockAdapter:
    return MockAdapter()
