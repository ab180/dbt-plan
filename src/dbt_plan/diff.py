"""Compiled SQL directory comparison."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ModelDiff:
    """A change detected between base and current compiled SQL."""

    model_name: str  # filename stem, e.g. "dim_device"
    status: str  # "modified", "added", "removed"
    base_path: Path | None  # None if added
    current_path: Path | None  # None if removed


def diff_compiled_dirs(
    base_dir: str | Path,
    current_dir: str | Path,
) -> list[ModelDiff]:
    """Compare two directories of compiled SQL files.

    Recursively finds .sql files, extracts model names from filename stems,
    and compares file contents. Unchanged models are excluded.

    Returns:
        List of ModelDiff sorted by model_name.
    """
    base_dir = Path(base_dir)
    current_dir = Path(current_dir)

    base_models = {f.stem: f for f in base_dir.rglob("*.sql")}
    current_models = {f.stem: f for f in current_dir.rglob("*.sql")}

    all_names = sorted(set(base_models) | set(current_models))
    diffs: list[ModelDiff] = []

    for name in all_names:
        base_path = base_models.get(name)
        current_path = current_models.get(name)

        if base_path and current_path:
            if base_path.read_text() != current_path.read_text():
                diffs.append(ModelDiff(name, "modified", base_path, current_path))
        elif current_path and not base_path:
            diffs.append(ModelDiff(name, "added", None, current_path))
        elif base_path and not current_path:
            diffs.append(ModelDiff(name, "removed", base_path, None))

    return diffs
