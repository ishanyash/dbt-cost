from __future__ import annotations

from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn
from rich.table import Table

from dbt_cost.core.calculator import bytes_to_cost, format_bytes
from dbt_cost.core.diff_engine import DiffResult

console = Console()

_SEPARATOR = " \u2502 "


def render_estimate_table(
    selector: str,
    models: list[dict],
    skipped: int,
    price_per_tb: float,
    top_n: int | None = None,
) -> None:
    total_bytes = sum(m["bytes_processed"] for m in models)
    total_cost = bytes_to_cost(total_bytes, price_per_tb)
    models_sorted = sorted(models, key=lambda m: m["bytes_processed"], reverse=True)

    if len(models_sorted) == 1:
        m = models_sorted[0]
        console.print(f"\n  [bold]{m['name']}[/bold] ({m.get('layer', '')} | {m.get('materialization', '')})")
        console.print("  Compiled SQL: [green]\u2713[/green]")
        console.print(f"  Bytes scanned: {format_bytes(m['bytes_processed'])}")
        console.print(f"  Estimated cost: ${bytes_to_cost(m['bytes_processed'], price_per_tb):.3f}")
        config = m.get("config", {})
        partition_by = config.get("partition_by")
        if partition_by:
            field = partition_by.get("field", "N/A")
            dtype = partition_by.get("data_type", "")
            console.print(f"  Partition pruning: {field} ({dtype})")
        cluster_by = config.get("cluster_by")
        if cluster_by:
            console.print(f"  Cluster columns: {', '.join(cluster_by)}")
        console.print()
        return

    display_count = top_n if top_n and top_n < len(models_sorted) else len(models_sorted)
    remaining = len(models_sorted) - display_count

    console.print(f"\n  [bold]{selector}[/bold] ({len(models)} models)\n")

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    table.add_column("Model", style="cyan")
    table.add_column("Bytes Scanned", justify="right")
    table.add_column("Cost", justify="right")

    for m in models_sorted[:display_count]:
        table.add_row(
            m["name"],
            format_bytes(m["bytes_processed"]),
            f"${bytes_to_cost(m['bytes_processed'], price_per_tb):.3f}",
        )

    if remaining > 0:
        table.add_row(f"... ({remaining} more models)", "", "")

    console.print(table)
    console.print(f"\n  [bold]TOTAL ({len(models)} models)[/bold]  {format_bytes(total_bytes)}  ${total_cost:.3f}")
    if skipped > 0:
        console.print(f"  [yellow]\u26a0 {skipped} models skipped (no compiled SQL)[/yellow]")
    console.print()


def render_report(
    total_models: int,
    estimated_models: int,
    skipped_models: int,
    models: list[dict],
    by_layer: dict[str, dict],
    price_per_tb: float,
    top_n: int = 10,
) -> None:
    total_bytes = sum(m["bytes_processed"] for m in models)
    total_cost = bytes_to_cost(total_bytes, price_per_tb)
    models_sorted = sorted(models, key=lambda m: m["bytes_processed"], reverse=True)

    header = f"({total_models} models, {estimated_models} estimated, {skipped_models} skipped)"
    console.print(f"\n  [bold]Project Cost Report[/bold] {header}\n")
    console.print(f"  [bold]Top {min(top_n, len(models_sorted))} Most Expensive Models:[/bold]")

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    table.add_column("Model", style="cyan")
    table.add_column("Bytes Scanned", justify="right")
    table.add_column("Cost", justify="right")

    for m in models_sorted[:top_n]:
        table.add_row(
            m["name"],
            format_bytes(m["bytes_processed"]),
            f"${bytes_to_cost(m['bytes_processed'], price_per_tb):.3f}",
        )

    console.print(table)

    layer_parts = []
    for layer_name, layer_data in sorted(by_layer.items()):
        layer_parts.append(f"{layer_name}: ${layer_data['cost_usd']:.2f}")
    console.print("\n  [bold]Cost by Layer:[/bold]")
    console.print(f"  {_SEPARATOR.join(layer_parts)}")

    console.print(f"\n  [bold]Full project rebuild: ${total_cost:.2f}[/bold]")
    if skipped_models > 0:
        msg = f"{skipped_models} models skipped (no compiled SQL \u2014 run 'dbt compile')"
        console.print(f"  [yellow]\u26a0 {msg}[/yellow]")
    console.print()


def render_diff_table(
    result: DiffResult,
    price_per_tb: float,
    include_unchanged: bool = False,
) -> None:
    models = result.models
    if not models:
        console.print("\n  [bold]No model changes detected.[/bold]\n")
        return

    console.print("\n  [bold]Cost Diff[/bold]\n")

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    table.add_column("Model", style="cyan")
    table.add_column("Before", justify="right")
    table.add_column("After", justify="right")
    table.add_column("Change", justify="right")

    for m in models:
        if m.status == "unchanged" and not include_unchanged:
            continue

        before = f"${m.before_cost:.3f}" if m.before_cost is not None else "--"
        after = f"${m.after_cost:.3f}" if m.after_cost is not None else "--"

        if m.error:
            change_str = "[red]ERROR[/red]"
        elif m.status == "unchanged":
            change_str = "[dim]no change[/dim]"
        elif m.status == "added":
            change_str = f"[green]+ new (+${m.after_cost:.3f})[/green]"
        elif m.status == "removed":
            change_str = f"[yellow]- removed (-${m.before_cost:.3f})[/yellow]"
        else:
            sign = "+" if m.cost_delta >= 0 else ""
            pct = f" ({sign}{m.pct_change:.0f}%)" if m.pct_change is not None else ""
            color = "red" if m.cost_delta > 0 else "green"
            change_str = f"[{color}]{sign}${m.cost_delta:.3f}{pct}[/{color}]"

        table.add_row(m.name, before, after, change_str)

    console.print(table)

    sign = "+" if result.total_delta >= 0 else ""
    console.print(
        f"\n  [bold]Total:[/bold] ${result.total_before:.2f} -> ${result.total_after:.2f} "
        f"({sign}${result.total_delta:.2f})"
    )

    parts: list[str] = []
    if result.changed_count:
        parts.append(f"{result.changed_count} changed")
    if result.added_count:
        parts.append(f"{result.added_count} added")
    if result.removed_count:
        parts.append(f"{result.removed_count} removed")
    if result.error_count:
        parts.append(f"[red]{result.error_count} errors[/red]")
    if parts:
        console.print(f"  {', '.join(parts)}")
    console.print()


def create_progress() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
    )
