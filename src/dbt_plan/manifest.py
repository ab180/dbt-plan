"""Manifest.json parsing and downstream model discovery."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ModelNode:
    """A dbt model node from manifest.json."""

    node_id: str  # "model.airflux.int_unified"
    name: str  # "int_unified"
    materialization: str  # "table", "view", "incremental", "ephemeral"
    on_schema_change: str | None  # "ignore", "fail", "append_new_columns", "sync_all_columns"


def load_manifest(manifest_path: str | Path) -> dict:
    """Load and parse manifest.json."""
    path = Path(manifest_path)
    return json.loads(path.read_text())


def find_node_by_name(name: str, manifest: dict) -> ModelNode | None:
    """Find a model node in manifest by short name.

    Returns None if not found.
    """
    for node_id, node in manifest.get("nodes", {}).items():
        if node_id.startswith("model.") and node.get("name") == name:
            config = node.get("config", {})
            return ModelNode(
                node_id=node_id,
                name=name,
                materialization=config.get("materialized", "table"),
                on_schema_change=config.get("on_schema_change"),
            )
    return None


def find_downstream(
    node_id: str,
    child_map: dict[str, list[str]],
    models_only: bool = True,
) -> list[str]:
    """Find all recursive downstream dependents of a node.

    Uses BFS with visited set for cycle protection.
    Filters out test/source nodes when models_only=True.

    Returns:
        Sorted list of downstream node_ids (excluding starting node).
    """
    visited = {node_id}
    queue = deque()
    result = []

    for child in child_map.get(node_id, []):
        if child not in visited:
            visited.add(child)
            queue.append(child)

    while queue:
        current = queue.popleft()
        if not models_only or current.startswith("model."):
            result.append(current)
        for child in child_map.get(current, []):
            if child not in visited:
                visited.add(child)
                queue.append(child)

    return sorted(result)
