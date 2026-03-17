import pytest
from dbt_lens.core.manifest import ProjectGraph

from dbt_cost.core.selector import parse_selector, resolve_selector


class TestParseSelector:
    def test_plain_model(self):
        result = parse_selector("dim_customer")
        assert result.name == "dim_customer"
        assert result.upstream_depth is None
        assert result.downstream_depth is None

    def test_downstream_all(self):
        result = parse_selector("stg_orders+")
        assert result.name == "stg_orders"
        assert result.downstream_depth == -1
        assert result.upstream_depth is None

    def test_upstream_all(self):
        result = parse_selector("+mrt_order_summary")
        assert result.name == "mrt_order_summary"
        assert result.upstream_depth == -1
        assert result.downstream_depth is None

    def test_both_directions(self):
        result = parse_selector("+dim_customer+")
        assert result.name == "dim_customer"
        assert result.upstream_depth == -1
        assert result.downstream_depth == -1

    def test_downstream_depth(self):
        result = parse_selector("stg_orders+2")
        assert result.name == "stg_orders"
        assert result.downstream_depth == 2

    def test_upstream_depth(self):
        result = parse_selector("2+mrt_order_summary")
        assert result.name == "mrt_order_summary"
        assert result.upstream_depth == 2

    def test_source_downstream(self):
        result = parse_selector("source:raw.orders+")
        assert result.name == "source:raw.orders"
        assert result.downstream_depth == -1

    def test_tag_selector(self):
        result = parse_selector("tag:mrt_marketing")
        assert result.tag == "mrt_marketing"
        assert result.name is None

    def test_invalid_selector(self):
        with pytest.raises(ValueError, match="Invalid selector"):
            parse_selector("foo^^bar")


class TestResolveSelector:
    def test_single_model(self, graph: ProjectGraph):
        nodes = resolve_selector(graph, "dim_customer")
        assert len(nodes) == 1
        assert nodes[0].name == "dim_customer"

    def test_downstream(self, graph: ProjectGraph):
        nodes = resolve_selector(graph, "stg_orders+")
        names = {n.name for n in nodes}
        assert "stg_orders" in names
        assert "int_orders_enriched" in names
        assert "dim_order" in names

    def test_upstream(self, graph: ProjectGraph):
        nodes = resolve_selector(graph, "+dim_order")
        names = {n.name for n in nodes}
        assert "dim_order" in names
        assert "int_orders_enriched" in names
        assert "stg_orders" in names

    def test_downstream_depth_limited(self, graph: ProjectGraph):
        nodes = resolve_selector(graph, "stg_orders+1")
        names = {n.name for n in nodes}
        assert "stg_orders" in names
        assert "int_orders_enriched" in names
        assert "dim_order" not in names

    def test_tag_selector(self, graph: ProjectGraph):
        nodes = resolve_selector(graph, "tag:mrt_marketing")
        names = {n.name for n in nodes}
        assert "mrt_order_summary" in names

    def test_source_downstream(self, graph: ProjectGraph):
        nodes = resolve_selector(graph, "source:raw.orders+")
        names = {n.name for n in nodes}
        assert "stg_orders" in names
        assert "int_orders_enriched" in names

    def test_excludes_tests(self, graph: ProjectGraph):
        nodes = resolve_selector(graph, "dim_order+")
        resource_types = {n.resource_type for n in nodes}
        assert "test" not in resource_types

    def test_not_found_raises(self, graph: ProjectGraph):
        with pytest.raises(KeyError):
            resolve_selector(graph, "nonexistent_model")
