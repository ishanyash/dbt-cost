from __future__ import annotations

import click


@click.group()
@click.option("--manifest", default="target/manifest.json", help="Path to manifest.json", type=click.Path())
@click.option("--credentials", default=None, help="Path to GCP service account key JSON", type=click.Path(exists=True))
@click.option(
    "--format", "output_format", default="table",
    type=click.Choice(["table", "json", "markdown"]), help="Output format",
)
@click.pass_context
def cli(ctx: click.Context, manifest: str, credentials: str | None, output_format: str) -> None:
    """Estimate BigQuery costs for dbt models before execution."""
    ctx.ensure_object(dict)
    ctx.obj["manifest"] = manifest
    ctx.obj["credentials"] = credentials
    ctx.obj["output_format"] = output_format


# Import commands to register them
from dbt_cost.commands import diff, estimate, report  # noqa: E402, F401
