from __future__ import annotations

import sys
from pathlib import Path

import click

from dbt_cost.adapters.bigquery import get_adapter
from dbt_cost.cli import cli
from dbt_cost.core.calculator import DEFAULT_BQ_PRICE_PER_TB
from dbt_cost.core.diff_engine import run_diff
from dbt_cost.output.json_output import render_diff_json
from dbt_cost.output.markdown import render_diff_markdown
from dbt_cost.output.terminal import console, render_diff_table


@cli.command()
@click.option("--base-manifest", required=True, type=click.Path(exists=True), help="Path to base branch manifest.json")
@click.option("--pr-manifest", required=True, type=click.Path(exists=True), help="Path to PR branch manifest.json")
@click.option("--price", default=DEFAULT_BQ_PRICE_PER_TB, type=float, help="Price per TB in USD")
@click.option("--concurrency", default=10, type=int, help="Concurrent dry-runs")
@click.option("--threshold", default=None, type=float, help="Max allowed cost increase in USD; exit 1 if exceeded")
@click.option("--include-unchanged", is_flag=True, default=False, help="Include unchanged models in output")
@click.pass_context
def diff(
    ctx: click.Context,
    base_manifest: str,
    pr_manifest: str,
    price: float,
    concurrency: int,
    threshold: float | None,
    include_unchanged: bool,
) -> None:
    """Compare costs between two manifests (e.g., base vs PR branch)."""
    from dbt_lens.core.manifest import load_manifest

    base_graph = load_manifest(Path(base_manifest))
    pr_graph = load_manifest(Path(pr_manifest))

    adapter = get_adapter(base_graph.adapter_type, ctx.obj.get("credentials"))

    result = run_diff(
        base_graph=base_graph,
        pr_graph=pr_graph,
        adapter=adapter,
        price_per_tb=price,
        concurrency=concurrency,
        include_unchanged=include_unchanged,
    )

    output_format = ctx.obj.get("output_format", "table")
    if output_format == "json":
        render_diff_json(result, price)
    elif output_format == "markdown":
        click.echo(render_diff_markdown(result, price))
    else:
        render_diff_table(result, price, include_unchanged)

    if threshold is not None and result.total_delta > threshold:
        console.print(
            f"\n  [red]Cost increase ${result.total_delta:.2f} exceeds threshold ${threshold:.2f}[/red]"
        )
        sys.exit(1)
