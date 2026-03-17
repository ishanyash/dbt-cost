from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from dbt_cost.cli import cli
from tests.conftest import MockAdapter


class TestEstimateCommand:
    def test_single_model_table_output(self, small_manifest_path: Path):
        mock_adapter = MockAdapter(default_bytes=2_400_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.estimate.get_adapter", return_value=mock_adapter):
            result = runner.invoke(cli, [
                "--manifest", str(small_manifest_path),
                "estimate", "dim_customer",
            ])
        assert result.exit_code == 0
        assert "dim_customer" in result.output
        assert "2.4 GB" in result.output

    def test_downstream_selector(self, small_manifest_path: Path):
        mock_adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.estimate.get_adapter", return_value=mock_adapter):
            result = runner.invoke(cli, [
                "--manifest", str(small_manifest_path),
                "estimate", "stg_orders+",
            ])
        assert result.exit_code == 0
        assert "TOTAL" in result.output

    def test_json_output(self, small_manifest_path: Path):
        mock_adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.estimate.get_adapter", return_value=mock_adapter):
            result = runner.invoke(cli, [
                "--manifest", str(small_manifest_path),
                "--format", "json",
                "estimate", "dim_customer",
            ])
        assert result.exit_code == 0
        assert '"selector"' in result.output
        assert '"dim_customer"' in result.output

    def test_skips_models_without_compiled_sql(self, small_manifest_path: Path):
        mock_adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.estimate.get_adapter", return_value=mock_adapter):
            result = runner.invoke(cli, [
                "--manifest", str(small_manifest_path),
                "estimate", "dim_order+",
            ])
        assert result.exit_code == 0
        assert "skipped" in result.output.lower()

    def test_tag_selector(self, small_manifest_path: Path):
        mock_adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.estimate.get_adapter", return_value=mock_adapter):
            result = runner.invoke(cli, [
                "--manifest", str(small_manifest_path),
                "estimate", "--selector", "tag:mrt_marketing",
            ])
        assert result.exit_code == 0
        assert "mrt_order_summary" in result.output
