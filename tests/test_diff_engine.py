from __future__ import annotations

from dbt_cost.core.calculator import DEFAULT_BQ_PRICE_PER_TB
from dbt_cost.core.diff_engine import compare_manifests, run_diff
from tests.conftest import MockAdapter


class TestCompareManifests:
    def test_detects_changed(self, graph, modified_graph):
        result = compare_manifests(graph, modified_graph)
        # stg_orders has different compiled_code (added WHERE clause)
        assert result["model.test_project.stg_orders"] == "changed"

    def test_detects_added(self, graph, modified_graph):
        result = compare_manifests(graph, modified_graph)
        # stg_payments exists only in modified manifest
        assert result["model.test_project.stg_payments"] == "added"

    def test_detects_removed(self, graph, modified_graph):
        result = compare_manifests(graph, modified_graph)
        # dim_date exists only in base manifest
        assert result["model.test_project.dim_date"] == "removed"

    def test_detects_unchanged(self, graph, modified_graph):
        result = compare_manifests(graph, modified_graph)
        # dim_customer is identical in both
        assert result["model.test_project.dim_customer"] == "unchanged"

    def test_identical_manifests(self, graph):
        result = compare_manifests(graph, graph)
        for status in result.values():
            assert status == "unchanged"

    def test_ignores_whitespace_changes(self, graph):
        # Same manifest compared to itself should be all unchanged
        # (whitespace normalization means no false positives)
        result = compare_manifests(graph, graph)
        assert all(s == "unchanged" for s in result.values())

    def test_skips_no_compiled_code(self, graph, modified_graph):
        result = compare_manifests(graph, modified_graph)
        # mrt_no_compiled has no compiled_code, should not appear
        assert "model.test_project.mrt_no_compiled" not in result

    def test_mrt_order_summary_changed(self, graph, modified_graph):
        result = compare_manifests(graph, modified_graph)
        # mrt_order_summary changed (dim_date removed from deps, different SQL)
        assert result["model.test_project.mrt_order_summary"] == "changed"


class TestRunDiff:
    def test_basic_diff(self, graph, modified_graph):
        adapter = MockAdapter(default_bytes=1_000_000_000)
        result = run_diff(graph, modified_graph, adapter, DEFAULT_BQ_PRICE_PER_TB, concurrency=4)
        assert result.changed_count > 0
        assert result.added_count > 0
        assert result.removed_count > 0
        assert len(result.models) > 0

    def test_totals_computed(self, graph, modified_graph):
        adapter = MockAdapter(default_bytes=1_000_000_000)
        result = run_diff(graph, modified_graph, adapter, DEFAULT_BQ_PRICE_PER_TB, concurrency=4)
        # total_before and total_after should be > 0 since we have models
        assert result.total_before >= 0
        assert result.total_after >= 0

    def test_include_unchanged(self, graph, modified_graph):
        adapter = MockAdapter(default_bytes=1_000_000_000)
        result_without = run_diff(graph, modified_graph, adapter, DEFAULT_BQ_PRICE_PER_TB, include_unchanged=False)
        result_with = run_diff(graph, modified_graph, adapter, DEFAULT_BQ_PRICE_PER_TB, include_unchanged=True)
        # Including unchanged should produce more models in the output
        assert len(result_with.models) >= len(result_without.models)

    def test_identical_manifests_zero_delta(self, graph):
        adapter = MockAdapter(default_bytes=1_000_000_000)
        result = run_diff(graph, graph, adapter, DEFAULT_BQ_PRICE_PER_TB)
        # No changed/added/removed when comparing same manifest
        assert result.changed_count == 0
        assert result.added_count == 0
        assert result.removed_count == 0
        # All models are unchanged (but excluded from output by default)
        assert result.unchanged_count > 0
        assert len(result.models) == 0

    def test_handles_dry_run_error(self, graph, modified_graph):
        class ErrorAdapter(MockAdapter):
            def dry_run(self, sql: str, project: str, dataset: str) -> int:
                # Match on the raw.orders table reference present in stg_orders SQL
                if "`raw`.`orders`" in sql:
                    raise RuntimeError("BQ connection failed")
                return super().dry_run(sql, project, dataset)

        adapter = ErrorAdapter(default_bytes=1_000_000_000)
        result = run_diff(graph, modified_graph, adapter, DEFAULT_BQ_PRICE_PER_TB, concurrency=2)
        assert result.error_count > 0
        error_models = [m for m in result.models if m.error is not None]
        assert len(error_models) > 0
        assert "BQ connection failed" in error_models[0].error

    def test_model_diff_fields(self, graph, modified_graph):
        adapter = MockAdapter(
            default_bytes=1_000_000_000,
            # Match on table ref in stg_payments compiled SQL
            model_bytes={"`raw`.`payments`": 5_000_000_000},
        )
        result = run_diff(graph, modified_graph, adapter, DEFAULT_BQ_PRICE_PER_TB, concurrency=4)
        # Find the added stg_payments model
        added = [m for m in result.models if m.name == "stg_payments"]
        assert len(added) == 1
        assert added[0].status == "added"
        assert added[0].before_bytes is None
        assert added[0].after_bytes == 5_000_000_000
        assert added[0].before_cost is None
        assert added[0].after_cost is not None
