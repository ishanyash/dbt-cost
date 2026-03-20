from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from dbt_cost.cli import cli
from tests.conftest import MockAdapter


class TestDiffCommand:
    def test_table_output(self, small_manifest_path: Path, modified_manifest_path: Path):
        adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.diff.get_adapter", return_value=adapter):
            result = runner.invoke(cli, [
                "--format", "table",
                "diff",
                "--base-manifest", str(small_manifest_path),
                "--pr-manifest", str(modified_manifest_path),
            ])
        assert result.exit_code == 0
        assert "Cost Diff" in result.output

    def test_json_output(self, small_manifest_path: Path, modified_manifest_path: Path):
        adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.diff.get_adapter", return_value=adapter):
            result = runner.invoke(cli, [
                "--format", "json",
                "diff",
                "--base-manifest", str(small_manifest_path),
                "--pr-manifest", str(modified_manifest_path),
            ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "models" in data
        assert "total_delta_usd" in data
        assert "changed" in data

    def test_markdown_output(self, small_manifest_path: Path, modified_manifest_path: Path):
        adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.diff.get_adapter", return_value=adapter):
            result = runner.invoke(cli, [
                "--format", "markdown",
                "diff",
                "--base-manifest", str(small_manifest_path),
                "--pr-manifest", str(modified_manifest_path),
            ])
        assert result.exit_code == 0
        assert "| Model |" in result.output
        assert "**Total:**" in result.output

    def test_threshold_passes(self, small_manifest_path: Path, modified_manifest_path: Path):
        adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.diff.get_adapter", return_value=adapter):
            result = runner.invoke(cli, [
                "diff",
                "--base-manifest", str(small_manifest_path),
                "--pr-manifest", str(modified_manifest_path),
                "--threshold", "1000.0",
            ])
        assert result.exit_code == 0

    def test_threshold_fails(self, small_manifest_path: Path, modified_manifest_path: Path):
        # Use high bytes for added model (payments) to guarantee positive delta
        adapter = MockAdapter(
            default_bytes=1_000_000_000,
            model_bytes={"`raw`.`payments`": 500_000_000_000},
        )
        runner = CliRunner()
        with patch("dbt_cost.commands.diff.get_adapter", return_value=adapter):
            result = runner.invoke(cli, [
                "diff",
                "--base-manifest", str(small_manifest_path),
                "--pr-manifest", str(modified_manifest_path),
                "--threshold", "0.001",
            ])
        assert result.exit_code == 1
        assert "exceeds threshold" in result.output

    def test_identical_manifests(self, small_manifest_path: Path):
        adapter = MockAdapter(default_bytes=1_000_000_000)
        runner = CliRunner()
        with patch("dbt_cost.commands.diff.get_adapter", return_value=adapter):
            result = runner.invoke(cli, [
                "diff",
                "--base-manifest", str(small_manifest_path),
                "--pr-manifest", str(small_manifest_path),
            ])
        assert result.exit_code == 0
        assert "No model changes" in result.output
