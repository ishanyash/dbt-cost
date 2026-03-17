from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import click

from dbt_cost.adapters.bigquery import get_adapter
from dbt_cost.cli import cli
from dbt_cost.core.calculator import DEFAULT_BQ_PRICE_PER_TB, bytes_to_cost
from dbt_cost.core.selector import resolve_selector
from dbt_cost.output.json_output import render_estimate_json
from dbt_cost.output.terminal import console, create_progress, render_estimate_table


@cli.command()
@click.argument("selector", required=False)
@click.option("--selector", "tag_selector", default=None, help="Tag-based selector (e.g., tag:mrt_marketing)")
@click.option("--price", default=DEFAULT_BQ_PRICE_PER_TB, type=float, help="Price per TB in USD")
@click.option("--concurrency", default=10, type=int, help="Concurrent dry-runs")
@click.option("--top", "top_n", default=None, type=int, help="Show only top N most expensive models")
@click.pass_context
def estimate(
    ctx: click.Context,
    selector: str | None,
    tag_selector: str | None,
    price: float,
    concurrency: int,
    top_n: int | None,
) -> None:
    """Estimate the cost of running dbt models."""
    from dbt_lens.core.manifest import load_manifest

    effective_selector = tag_selector or selector
    if not effective_selector:
        raise click.UsageError("Provide a model selector or use --selector for tag-based selection.")

    manifest_path = Path(ctx.obj["manifest"])
    graph = load_manifest(manifest_path)
    adapter = get_adapter(graph.adapter_type, ctx.obj.get("credentials"))

    nodes = resolve_selector(graph, effective_selector)

    # Split into estimable (has compiled_code) and skipped
    estimable = [n for n in nodes if n.compiled_code and n.resource_type == "model"]
    skipped = len(nodes) - len(estimable)

    if not estimable:
        console.print("[red]No models with compiled SQL found. Run 'dbt compile' before estimating costs.[/red]")
        raise SystemExit(1)

    # Run dry-runs concurrently
    model_results: list[dict] = []

    with create_progress() as progress:
        task = progress.add_task("Estimating costs...", total=len(estimable))

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
                        "config": node.config,
                    })
                except Exception as e:
                    console.print(f"[yellow]⚠ Could not estimate model '{node.name}': {e}[/yellow]")
                    skipped += 1
                progress.advance(task)

    output_format = ctx.obj.get("output_format", "table")
    if output_format == "json":
        total_bytes = sum(m["bytes_processed"] for m in model_results)
        total_cost = bytes_to_cost(total_bytes, price)
        render_estimate_json(effective_selector, model_results, skipped, total_bytes, total_cost, price)
    else:
        render_estimate_table(effective_selector, model_results, skipped, price, top_n)
