from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from dbt_lens.core.graph import ProjectGraph

from dbt_cost.core.adapter import CostAdapter
from dbt_cost.core.calculator import DEFAULT_BQ_PRICE_PER_TB, bytes_to_cost


@dataclass
class ModelDiff:
    unique_id: str
    name: str
    layer: str
    materialization: str
    status: str  # "changed" | "added" | "removed" | "unchanged"
    before_bytes: int | None
    after_bytes: int | None
    before_cost: float | None
    after_cost: float | None
    cost_delta: float
    pct_change: float | None
    error: str | None


@dataclass
class DiffResult:
    models: list[ModelDiff]
    total_before: float
    total_after: float
    total_delta: float
    changed_count: int
    added_count: int
    removed_count: int
    unchanged_count: int
    error_count: int


def _normalize_sql(sql: str) -> str:
    """Normalize SQL for comparison: strip and collapse whitespace."""
    return re.sub(r"\s+", " ", sql.strip())


def compare_manifests(
    base_graph: ProjectGraph,
    pr_graph: ProjectGraph,
) -> dict[str, str]:
    """Compare two manifests and classify each model.

    Returns a dict of {unique_id: status} where status is one of:
    "added", "removed", "changed", "unchanged".
    """
    base_models = {
        uid: node
        for uid, node in base_graph.nodes.items()
        if node.resource_type == "model" and node.compiled_code
    }
    pr_models = {
        uid: node
        for uid, node in pr_graph.nodes.items()
        if node.resource_type == "model" and node.compiled_code
    }

    all_uids = set(base_models.keys()) | set(pr_models.keys())
    result: dict[str, str] = {}

    for uid in all_uids:
        in_base = uid in base_models
        in_pr = uid in pr_models

        if in_base and not in_pr:
            result[uid] = "removed"
        elif in_pr and not in_base:
            result[uid] = "added"
        else:
            base_sql = _normalize_sql(base_models[uid].compiled_code)
            pr_sql = _normalize_sql(pr_models[uid].compiled_code)
            result[uid] = "changed" if base_sql != pr_sql else "unchanged"

    return result


def run_diff(
    base_graph: ProjectGraph,
    pr_graph: ProjectGraph,
    adapter: CostAdapter,
    price_per_tb: float = DEFAULT_BQ_PRICE_PER_TB,
    concurrency: int = 10,
    include_unchanged: bool = False,
) -> DiffResult:
    """Run cost diff between two manifests.

    Dry-runs changed/added/removed models and produces a DiffResult.
    """
    classifications = compare_manifests(base_graph, pr_graph)

    base_nodes = {
        uid: node
        for uid, node in base_graph.nodes.items()
        if node.resource_type == "model" and node.compiled_code
    }
    pr_nodes = {
        uid: node
        for uid, node in pr_graph.nodes.items()
        if node.resource_type == "model" and node.compiled_code
    }

    # Collect dry-run tasks: (uid, "before"|"after", sql, project, dataset)
    tasks: list[tuple[str, str, str, str, str]] = []

    for uid, status in classifications.items():
        if status == "unchanged" and not include_unchanged:
            continue
        if status in ("changed", "removed"):
            node = base_nodes[uid]
            tasks.append((uid, "before", node.compiled_code, node.database, node.schema_))
        if status in ("changed", "added"):
            node = pr_nodes[uid]
            tasks.append((uid, "after", node.compiled_code, node.database, node.schema_))

    # Run dry-runs concurrently
    # Results: {(uid, side): bytes}
    dry_run_results: dict[tuple[str, str], int] = {}
    dry_run_errors: dict[str, str] = {}

    if tasks:
        with ThreadPoolExecutor(max_workers=min(concurrency, len(tasks))) as executor:
            future_to_key = {
                executor.submit(adapter.dry_run, sql, project, dataset): (uid, side)
                for uid, side, sql, project, dataset in tasks
            }
            for future in as_completed(future_to_key):
                uid, side = future_to_key[future]
                try:
                    dry_run_results[(uid, side)] = future.result()
                except Exception as e:
                    dry_run_errors[uid] = str(e)

    # Build ModelDiff list
    model_diffs: list[ModelDiff] = []
    error_count = 0
    changed_count = 0
    added_count = 0
    removed_count = 0
    unchanged_count = 0

    for uid, status in sorted(classifications.items()):
        if status == "unchanged" and not include_unchanged:
            unchanged_count += 1
            continue

        # Get node metadata from whichever graph has it
        node = pr_nodes.get(uid) or base_nodes.get(uid)
        assert node is not None

        error = dry_run_errors.get(uid)
        if error:
            error_count += 1
            model_diffs.append(ModelDiff(
                unique_id=uid,
                name=node.name,
                layer=node.layer,
                materialization=node.materialization,
                status=status,
                before_bytes=None,
                after_bytes=None,
                before_cost=None,
                after_cost=None,
                cost_delta=0.0,
                pct_change=None,
                error=error,
            ))
            continue

        before_bytes = dry_run_results.get((uid, "before"))
        after_bytes = dry_run_results.get((uid, "after"))
        before_cost = bytes_to_cost(before_bytes, price_per_tb) if before_bytes is not None else None
        after_cost = bytes_to_cost(after_bytes, price_per_tb) if after_bytes is not None else None

        cost_delta = (after_cost or 0.0) - (before_cost or 0.0)
        pct_change: float | None = None
        if before_cost is not None and before_cost > 0 and after_cost is not None:
            pct_change = ((after_cost - before_cost) / before_cost) * 100

        if status == "changed":
            changed_count += 1
        elif status == "added":
            added_count += 1
        elif status == "removed":
            removed_count += 1
        elif status == "unchanged":
            unchanged_count += 1

        model_diffs.append(ModelDiff(
            unique_id=uid,
            name=node.name,
            layer=node.layer,
            materialization=node.materialization,
            status=status,
            before_bytes=before_bytes,
            after_bytes=after_bytes,
            before_cost=before_cost,
            after_cost=after_cost,
            cost_delta=cost_delta,
            pct_change=pct_change,
            error=None,
        ))

    # Sort: errors first, then by absolute cost_delta descending
    model_diffs.sort(key=lambda m: (m.error is None, -abs(m.cost_delta)))

    total_before = sum(m.before_cost for m in model_diffs if m.before_cost is not None)
    total_after = sum(m.after_cost for m in model_diffs if m.after_cost is not None)
    total_delta = total_after - total_before

    return DiffResult(
        models=model_diffs,
        total_before=total_before,
        total_after=total_after,
        total_delta=total_delta,
        changed_count=changed_count,
        added_count=added_count,
        removed_count=removed_count,
        unchanged_count=unchanged_count,
        error_count=error_count,
    )
