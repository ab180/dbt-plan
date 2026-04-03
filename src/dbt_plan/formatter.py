"""Output formatting for DDL predictions."""

from __future__ import annotations

import difflib
from dataclasses import dataclass, field
from pathlib import Path

from dbt_plan.predictor import DDLPrediction, Safety

_SAFETY_ORDER = {Safety.DESTRUCTIVE: 0, Safety.WARNING: 1, Safety.SAFE: 2}
_SAFETY_ICON = {
    Safety.DESTRUCTIVE: "\U0001f534",  # red circle
    Safety.WARNING: "\u26a0\ufe0f",  # warning
    Safety.SAFE: "\u2705",  # check mark
}


@dataclass
class CheckResult:
    """Full result of a dbt-plan check."""

    predictions: list[DDLPrediction] = field(default_factory=list)
    downstream_map: dict[str, list[str]] = field(default_factory=dict)
    parse_failures: list[str] = field(default_factory=list)
    sql_diffs: dict[str, tuple[Path | None, Path | None]] = field(
        default_factory=dict
    )


def _make_sql_diff(base_path: Path | None, current_path: Path | None) -> str | None:
    """Generate unified diff between base and current SQL files."""
    base_lines = base_path.read_text().splitlines(keepends=True) if base_path else []
    current_lines = (
        current_path.read_text().splitlines(keepends=True) if current_path else []
    )
    base_name = base_path.name if base_path else "/dev/null"
    current_name = current_path.name if current_path else "/dev/null"
    diff = list(
        difflib.unified_diff(base_lines, current_lines, fromfile=base_name, tofile=current_name)
    )
    if not diff:
        return None
    return "".join(diff)


def format_text(result: CheckResult) -> str:
    """Format result for terminal output."""
    if not result.predictions:
        return "dbt-plan -- no model changes detected"

    sorted_preds = sorted(
        result.predictions, key=lambda p: _SAFETY_ORDER.get(p.safety, 9)
    )
    lines = [f"dbt-plan -- {len(result.predictions)} model(s) changed", ""]

    for pred in sorted_preds:
        mat_info = pred.materialization
        if pred.on_schema_change:
            mat_info += f", {pred.on_schema_change}"
        lines.append(f"{pred.safety.value.upper()}  {pred.model_name} ({mat_info})")
        for op in pred.operations:
            if op.column:
                lines.append(f"  {op.operation}  {op.column}")
            else:
                lines.append(f"  {op.operation}")
        downstream = result.downstream_map.get(pred.model_name, [])
        if downstream:
            names = ", ".join(downstream)
            lines.append(f"  Downstream: {names} ({len(downstream)} model(s))")
        lines.append("")

    if result.parse_failures:
        names = ", ".join(result.parse_failures)
        lines.append(
            f"WARNING: Could not extract columns for: {names}"
            " -- manual review required"
        )

    return "\n".join(lines)


def format_github(result: CheckResult) -> str:
    """Format result as GitHub-flavored markdown with SQL diffs."""
    if not result.predictions:
        return "<!-- dbt-plan -->\n### dbt-plan -- no model changes detected"

    sorted_preds = sorted(
        result.predictions, key=lambda p: _SAFETY_ORDER.get(p.safety, 9)
    )
    lines = ["<!-- dbt-plan -->", f"### dbt-plan -- {len(result.predictions)} model(s) changed", ""]

    for pred in sorted_preds:
        icon = _SAFETY_ICON.get(pred.safety, "")
        mat_info = pred.materialization
        if pred.on_schema_change:
            mat_info += f", {pred.on_schema_change}"
        lines.append(
            f"{icon} **{pred.safety.value.upper()}** `{pred.model_name}`"
            f" ({mat_info})"
        )
        for op in pred.operations:
            if op.column:
                lines.append(f"- `{op.operation}` {op.column}")
            else:
                lines.append(f"- {op.operation}")
        downstream = result.downstream_map.get(pred.model_name, [])
        if downstream:
            names = ", ".join(downstream)
            lines.append(f"- Downstream: {names} ({len(downstream)} model(s))")

        # SQL diff in collapsible details block
        paths = result.sql_diffs.get(pred.model_name)
        if paths:
            diff_text = _make_sql_diff(paths[0], paths[1])
            if diff_text:
                lines.append("")
                lines.append(
                    f"<details><summary>compiled SQL diff</summary>\n"
                    f"\n```diff\n{diff_text}```\n</details>"
                )
        lines.append("")

    if result.parse_failures:
        names = ", ".join(result.parse_failures)
        lines.append(
            f"> **WARNING**: Could not extract columns for: {names}"
            " -- manual review required"
        )

    return "\n".join(lines)
