from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from dbt_cost.cli import cli
from tests.conftest import MockAdapter


class TestReportCommand:
    def test_report_table_output(self, small_manifest_path: Path):
        mock_adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.report.get_adapter", return_value=mock_adapter):
            result = runner.invoke(cli, [
                "--manifest", str(small_manifest_path),
                "report",
            ])
        assert result.exit_code == 0
        assert "Project Cost Report" in result.output
        assert "Cost by Layer" in result.output

    def test_report_json_output(self, small_manifest_path: Path):
        mock_adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.report.get_adapter", return_value=mock_adapter):
            result = runner.invoke(cli, [
                "--manifest", str(small_manifest_path),
                "--format", "json",
                "report",
            ])
        assert result.exit_code == 0
        assert '"total_models"' in result.output
        assert '"by_layer"' in result.output

    def test_report_top_n(self, small_manifest_path: Path):
        mock_adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.report.get_adapter", return_value=mock_adapter):
            result = runner.invoke(cli, [
                "--manifest", str(small_manifest_path),
                "report", "--top", "3",
            ])
        assert result.exit_code == 0
        assert "Top 3" in result.output

    def test_report_counts_skipped(self, small_manifest_path: Path):
        mock_adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.report.get_adapter", return_value=mock_adapter):
            result = runner.invoke(cli, [
                "--manifest", str(small_manifest_path),
                "report",
            ])
        assert result.exit_code == 0
        assert "skipped" in result.output.lower()
