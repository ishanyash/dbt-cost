from __future__ import annotations

import json

from rich.console import Console

from dbt_cost.core.diff_engine import DiffResult

console = Console()


def render_estimate_json(
    selector: str,
    models: list[dict],
    skipped: int,
    total_bytes: int,
    total_cost: float,
    price_per_tb: float,
) -> None:
    output = {
        "selector": selector,
        "models": [
            {
                "unique_id": m["unique_id"],
                "name": m["name"],
                "layer": m.get("layer", ""),
                "bytes_processed": m["bytes_processed"],
                "cost_usd": round(m["cost_usd"], 3),
                "materialization": m.get("materialization", ""),
            }
            for m in sorted(models, key=lambda m: m["bytes_processed"], reverse=True)
        ],
        "skipped": skipped,
        "total_bytes": total_bytes,
        "total_cost_usd": round(total_cost, 3),
        "price_per_tb": price_per_tb,
    }
    console.print(json.dumps(output, indent=2))


def render_diff_json(
    result: DiffResult,
    price_per_tb: float,
) -> None:
    output = {
        "models": [
            {
                "unique_id": m.unique_id,
                "name": m.name,
                "layer": m.layer,
                "status": m.status,
                "before_bytes": m.before_bytes,
                "after_bytes": m.after_bytes,
                "before_cost_usd": round(m.before_cost, 3) if m.before_cost is not None else None,
                "after_cost_usd": round(m.after_cost, 3) if m.after_cost is not None else None,
                "cost_delta_usd": round(m.cost_delta, 3),
                "pct_change": round(m.pct_change, 1) if m.pct_change is not None else None,
                "error": m.error,
            }
            for m in result.models
        ],
        "total_before_usd": round(result.total_before, 3),
        "total_after_usd": round(result.total_after, 3),
        "total_delta_usd": round(result.total_delta, 3),
        "changed": result.changed_count,
        "added": result.added_count,
        "removed": result.removed_count,
        "unchanged": result.unchanged_count,
        "errors": result.error_count,
        "price_per_tb": price_per_tb,
    }
    console.print(json.dumps(output, indent=2))


def render_report_json(
    total_models: int,
    estimated_models: int,
    skipped_models: int,
    models: list[dict],
    by_layer: dict[str, dict],
    total_bytes: int,
    total_cost: float,
    price_per_tb: float,
) -> None:
    output = {
        "total_models": total_models,
        "estimated_models": estimated_models,
        "skipped_models": skipped_models,
        "models": [
            {
                "unique_id": m["unique_id"],
                "name": m["name"],
                "layer": m.get("layer", ""),
                "bytes_processed": m["bytes_processed"],
                "cost_usd": round(m["cost_usd"], 3),
                "materialization": m.get("materialization", ""),
            }
            for m in sorted(models, key=lambda m: m["bytes_processed"], reverse=True)
        ],
        "by_layer": by_layer,
        "total_bytes": total_bytes,
        "total_cost_usd": round(total_cost, 3),
        "price_per_tb": price_per_tb,
    }
    console.print(json.dumps(output, indent=2))
