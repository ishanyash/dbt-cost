from __future__ import annotations

from dbt_cost.core.diff_engine import DiffResult


def _format_cost(cost: float | None) -> str:
    if cost is None:
        return "--"
    return f"${cost:,.2f}"


def _format_change(model_diff) -> str:
    if model_diff.error:
        return "dry-run failed"

    if model_diff.status == "unchanged":
        return "no change"

    if model_diff.status == "added":
        return f"new (+{_format_cost(model_diff.after_cost)})"

    if model_diff.status == "removed":
        return f"removed (-{_format_cost(model_diff.before_cost)})"

    # Changed model
    delta = model_diff.cost_delta
    sign = "+" if delta >= 0 else ""
    delta_str = f"{sign}{_format_cost(abs(delta))}" if delta >= 0 else f"-{_format_cost(abs(delta))}"

    if model_diff.pct_change is not None:
        pct = model_diff.pct_change
        pct_str = f"{sign}{pct:,.0f}%"
        prefix = "" if abs(pct) < 20 else "Warning: "
        return f"{prefix}{delta_str} ({pct_str})"

    return delta_str


def render_diff_markdown(result: DiffResult, price_per_tb: float) -> str:
    """Render a DiffResult as a markdown string for PR comments."""
    lines: list[str] = []

    lines.append("**dbt-cost: Cost Impact of this PR**")
    lines.append("")

    if not result.models:
        lines.append("No model changes detected.")
        return "\n".join(lines)

    # Table header
    lines.append("| Model | Before | After | Change |")
    lines.append("|-------|--------|-------|--------|")

    for m in result.models:
        before = _format_cost(m.before_cost)
        after = _format_cost(m.after_cost) if not m.error else "ERROR"
        change = _format_change(m)
        lines.append(f"| {m.name} | {before} | {after} | {change} |")

    lines.append("")

    # Totals
    before_total = _format_cost(result.total_before)
    after_total = _format_cost(result.total_after)
    sign = "+" if result.total_delta >= 0 else ""
    abs_delta = _format_cost(abs(result.total_delta))
    delta_str = f"{sign}{abs_delta}" if result.total_delta >= 0 else f"-{abs_delta}"
    lines.append(f"**Total:** {before_total} -> {after_total} ({delta_str})")
    lines.append("")

    # Summary
    parts: list[str] = []
    if result.changed_count:
        parts.append(f"{result.changed_count} model{'s' if result.changed_count != 1 else ''} changed")
    if result.added_count:
        parts.append(f"{result.added_count} added")
    if result.removed_count:
        parts.append(f"{result.removed_count} removed")
    if result.error_count:
        parts.append(f"{result.error_count} error{'s' if result.error_count != 1 else ''}")
    if parts:
        lines.append(f"*{', '.join(parts)}*")

    return "\n".join(lines)
