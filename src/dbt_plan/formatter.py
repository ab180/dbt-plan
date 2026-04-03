"""Output formatting for DDL predictions."""

from __future__ import annotations

from dataclasses import dataclass, field

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
    """Format result as GitHub-flavored markdown."""
    if not result.predictions:
        return "### dbt-plan -- no model changes detected"

    sorted_preds = sorted(
        result.predictions, key=lambda p: _SAFETY_ORDER.get(p.safety, 9)
    )
    lines = [f"### dbt-plan -- {len(result.predictions)} model(s) changed", ""]

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
        lines.append("")

    if result.parse_failures:
        names = ", ".join(result.parse_failures)
        lines.append(
            f"> **WARNING**: Could not extract columns for: {names}"
            " -- manual review required"
        )

    return "\n".join(lines)
