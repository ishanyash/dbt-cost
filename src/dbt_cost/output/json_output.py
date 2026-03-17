from __future__ import annotations

import json

from rich.console import Console

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
