from __future__ import annotations

import re
from dataclasses import dataclass

from dbt_lens.core.graph import downstream, upstream
from dbt_lens.core.manifest import DbtNode, ProjectGraph


@dataclass
class SelectorResult:
    name: str | None = None
    tag: str | None = None
    upstream_depth: int | None = None  # None=no upstream, -1=unlimited
    downstream_depth: int | None = None  # None=no downstream, -1=unlimited


_SELECTOR_RE = re.compile(
    r"^(?:(?P<up_depth>\d+)?\+)?"
    r"(?P<name>[a-zA-Z0-9_.:\-]+)"
    r"(?:\+(?P<down_depth>\d+)?)?$"
)


def parse_selector(selector: str) -> SelectorResult:
    if selector.startswith("tag:"):
        return SelectorResult(tag=selector[4:])

    match = _SELECTOR_RE.match(selector)
    if not match:
        raise ValueError(
            f"Invalid selector '{selector}'. "
            f"Supported formats: model, model+, +model, model+N, tag:X, source:name.table+"
        )

    name = match.group("name")
    up_depth_str = match.group("up_depth")
    down_depth_str = match.group("down_depth")

    upstream_depth = None
    prefix = selector[:match.start("name")]
    if "+" in prefix:
        upstream_depth = int(up_depth_str) if up_depth_str else -1

    downstream_depth = None
    suffix = selector[match.end("name"):]
    if "+" in suffix:
        downstream_depth = int(down_depth_str) if down_depth_str else -1

    return SelectorResult(name=name, upstream_depth=upstream_depth, downstream_depth=downstream_depth)


def resolve_selector(graph: ProjectGraph, selector: str) -> list[DbtNode]:
    parsed = parse_selector(selector)

    if parsed.tag is not None:
        return [
            node for node in graph.nodes.values()
            if parsed.tag in node.tags and node.resource_type == "model"
        ]

    assert parsed.name is not None
    node = graph.resolve_node(parsed.name)

    result_set: dict[str, DbtNode] = {node.unique_id: node}

    if parsed.downstream_depth is not None:
        max_depth = None if parsed.downstream_depth == -1 else parsed.downstream_depth
        for n in downstream(graph, node.unique_id, max_depth=max_depth, include_tests=False):
            result_set[n.unique_id] = n

    if parsed.upstream_depth is not None:
        max_depth = None if parsed.upstream_depth == -1 else parsed.upstream_depth
        for n in upstream(graph, node.unique_id, max_depth=max_depth, include_tests=False):
            result_set[n.unique_id] = n

    return list(result_set.values())
