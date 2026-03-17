from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import click

from dbt_cost.adapters.bigquery import get_adapter
from dbt_cost.cli import cli
from dbt_cost.core.calculator import DEFAULT_BQ_PRICE_PER_TB, bytes_to_cost
from dbt_cost.output.json_output import render_report_json
from dbt_cost.output.terminal import console, create_progress, render_report


@cli.command()
@click.option("--top", "top_n", default=10, type=int, help="Show top N most expensive models")
@click.option("--layer", default=None, help="Filter to a specific layer")
@click.option("--price", default=DEFAULT_BQ_PRICE_PER_TB, type=float, help="Price per TB in USD")
@click.option("--concurrency", default=10, type=int, help="Concurrent dry-runs (max 50)")
@click.pass_context
def report(
    ctx: click.Context,
    top_n: int,
    layer: str | None,
    price: float,
    concurrency: int,
) -> None:
    """Full project cost breakdown."""
    from dbt_lens.core.manifest import load_manifest

    concurrency = min(concurrency, 50)
    manifest_path = Path(ctx.obj["manifest"])
    graph = load_manifest(manifest_path)
    adapter = get_adapter(graph.adapter_type, ctx.obj.get("credentials"))

    # Get all model nodes
    all_models = [n for n in graph.nodes.values() if n.resource_type == "model"]
    if layer:
        all_models = [n for n in all_models if n.layer == layer]

    total_models = len(all_models)
    estimable = [n for n in all_models if n.compiled_code]
    skipped_count = total_models - len(estimable)

    if not estimable:
        console.print("[red]No models with compiled SQL found. Run 'dbt compile' before estimating costs.[/red]")
        raise SystemExit(1)

    model_results: list[dict] = []

    with create_progress() as progress:
        task = progress.add_task("Estimating all models...", total=len(estimable))

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            future_to_node = {
                executor.submit(adapter.dry_run, n.compiled_code, n.database, n.schema_): n
                for n in estimable
            }
            for future in as_completed(future_to_node):
                node = future_to_node[future]
                try:
                    total_bytes = future.result()
                    model_results.append({
                        "unique_id": node.unique_id,
                        "name": node.name,
                        "layer": node.layer,
                        "materialization": node.materialization,
                        "bytes_processed": total_bytes,
                        "cost_usd": bytes_to_cost(total_bytes, price),
                    })
                except Exception as e:
                    console.print(f"[yellow]⚠ Could not estimate model '{node.name}': {e}[/yellow]")
                    skipped_count += 1
                progress.advance(task)

    # Aggregate by layer
    by_layer: dict[str, dict] = {}
    for m in model_results:
        layer_name = m["layer"]
        if layer_name not in by_layer:
            by_layer[layer_name] = {"cost_usd": 0.0, "bytes": 0, "count": 0}
        by_layer[layer_name]["cost_usd"] += m["cost_usd"]
        by_layer[layer_name]["bytes"] += m["bytes_processed"]
        by_layer[layer_name]["count"] += 1

    # Round layer costs
    for v in by_layer.values():
        v["cost_usd"] = round(v["cost_usd"], 2)

    output_format = ctx.obj.get("output_format", "table")
    total_bytes = sum(m["bytes_processed"] for m in model_results)
    total_cost = bytes_to_cost(total_bytes, price)

    if output_format == "json":
        render_report_json(
            total_models, len(model_results), skipped_count,
            model_results, by_layer, total_bytes, total_cost, price,
        )
    else:
        render_report(
            total_models, len(model_results), skipped_count,
            model_results, by_layer, price, top_n,
        )
