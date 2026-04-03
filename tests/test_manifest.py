"""Tests for manifest parsing and downstream discovery."""

import json

from dbt_plan.manifest import ModelNode, find_downstream, find_node_by_name, load_manifest


class TestFindDownstream:
    def test_direct_children(self):
        """Node with direct children returns them."""
        child_map = {
            "model.p.A": ["model.p.B", "model.p.C"],
            "model.p.B": [],
            "model.p.C": [],
        }
        result = find_downstream("model.p.A", child_map)
        assert result == ["model.p.B", "model.p.C"]

    def test_recursive_downstream(self):
        """Transitive dependencies are included."""
        child_map = {
            "model.p.A": ["model.p.B"],
            "model.p.B": ["model.p.C"],
            "model.p.C": [],
        }
        result = find_downstream("model.p.A", child_map)
        assert result == ["model.p.B", "model.p.C"]

    def test_cycle_protection(self):
        """Cycles do not cause infinite loops."""
        child_map = {
            "model.p.A": ["model.p.B"],
            "model.p.B": ["model.p.A"],
        }
        result = find_downstream("model.p.A", child_map)
        assert result == ["model.p.B"]

    def test_filter_test_nodes(self):
        """Test nodes are excluded when models_only=True."""
        child_map = {
            "model.p.A": ["model.p.B", "test.p.not_null_a.abc123"],
            "model.p.B": [],
        }
        result = find_downstream("model.p.A", child_map, models_only=True)
        assert result == ["model.p.B"]

    def test_no_children(self):
        """Leaf node returns empty list."""
        child_map = {"model.p.A": []}
        result = find_downstream("model.p.A", child_map)
        assert result == []


class TestFindNodeByName:
    def test_finds_model_node(self):
        """Finds a model node by its short name."""
        manifest = {
            "nodes": {
                "model.airflux.int_unified": {
                    "name": "int_unified",
                    "config": {
                        "materialized": "incremental",
                        "on_schema_change": "sync_all_columns",
                    },
                },
            },
        }
        node = find_node_by_name("int_unified", manifest)
        assert node is not None
        assert node.node_id == "model.airflux.int_unified"
        assert node.name == "int_unified"
        assert node.materialization == "incremental"
        assert node.on_schema_change == "sync_all_columns"

    def test_returns_none_when_not_found(self):
        """Returns None for nonexistent model name."""
        manifest = {"nodes": {}}
        assert find_node_by_name("nonexistent", manifest) is None


class TestLoadManifest:
    def test_load_manifest(self, tmp_path):
        """Loads and parses manifest.json."""
        manifest_path = tmp_path / "manifest.json"
        data = {"nodes": {}, "child_map": {}}
        manifest_path.write_text(json.dumps(data))
        result = load_manifest(str(manifest_path))
        assert result == data
