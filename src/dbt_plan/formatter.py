"""Output formatting for DDL predictions."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field

from dbt_plan.predictor import DDLPrediction, Safety

_SAFETY_ORDER = {Safety.DESTRUCTIVE: 0, Safety.WARNING: 1, Safety.SAFE: 2}
_SAFETY_ICON = {
    Safety.DESTRUCTIVE: "\U0001f534",  # red circle
    Safety.WARNING: "\u26a0\ufe0f",  # warning
    Safety.SAFE: "\u2705",  # check mark
}

# ANSI color codes for terminal output
_SAFETY_COLOR = {
    Safety.DESTRUCTIVE: "\033[31m",  # red
    Safety.WARNING: "\033[33m",  # yellow
    Safety.SAFE: "\033[32m",  # green
}
_RESET = "\033[0m"
_BOLD = "\033[1m"


@dataclass
class CheckResult:
    """Full result of a dbt-plan check."""

    predictions: list[DDLPrediction] = field(default_factory=list)
    downstream_map: dict[str, list[str]] = field(default_factory=dict)
    parse_failures: list[str] = field(default_factory=list)
    skipped_models: list[str] = field(default_factory=list)


def format_text(result: CheckResult, *, color: bool | None = None) -> str:
    """Format result for terminal output.

    Args:
        color: Force color on/off. None = auto-detect from sys.stdout.isatty().
    """
    use_color = color if color is not None else sys.stdout.isatty()

    def _colored(text: str, safety: Safety) -> str:
        if not use_color:
            return text
        c = _SAFETY_COLOR.get(safety, "")
        return f"{c}{_BOLD}{text}{_RESET}"

    if not result.predictions:
        return "dbt-plan -- no model changes detected"

    sorted_preds = sorted(result.predictions, key=lambda p: _SAFETY_ORDER.get(p.safety, 9))
    lines = [f"dbt-plan -- {len(result.predictions)} model(s) changed", ""]

    for pred in sorted_preds:
        mat_info = pred.materialization
        if pred.on_schema_change:
            mat_info += f", {pred.on_schema_change}"
        label = _colored(pred.safety.value.upper(), pred.safety)
        lines.append(f"{label}  {pred.model_name} ({mat_info})")
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
        warn = _colored("WARNING", Safety.WARNING) if use_color else "WARNING"
        lines.append(f"{warn}: Could not extract columns for: {names} -- manual review required")

    if result.skipped_models:
        names = ", ".join(result.skipped_models)
        warn = _colored("WARNING", Safety.WARNING) if use_color else "WARNING"
        lines.append(
            f"{warn}: Skipped {len(result.skipped_models)} model(s) not found in manifest: {names}"
        )

    # Summary line (grepable for CI: grep "^dbt-plan:" output)
    lines.append(_summary_line(result))

    return "\n".join(lines)


def _summary_line(result: CheckResult) -> str:
    """One-line summary for CI pipeline grep."""
    n = len(result.predictions)
    safe = sum(1 for p in result.predictions if p.safety == Safety.SAFE)
    warn = sum(1 for p in result.predictions if p.safety == Safety.WARNING)
    dest = sum(1 for p in result.predictions if p.safety == Safety.DESTRUCTIVE)
    return f"dbt-plan: {n} checked, {safe} safe, {warn} warning, {dest} destructive"


def format_github(result: CheckResult) -> str:
    """Format result as GitHub-flavored markdown."""
    if not result.predictions:
        return "### dbt-plan -- no model changes detected"

    sorted_preds = sorted(result.predictions, key=lambda p: _SAFETY_ORDER.get(p.safety, 9))
    lines = [f"### dbt-plan -- {len(result.predictions)} model(s) changed", ""]

    for pred in sorted_preds:
        icon = _SAFETY_ICON.get(pred.safety, "")
        mat_info = pred.materialization
        if pred.on_schema_change:
            mat_info += f", {pred.on_schema_change}"
        lines.append(f"{icon} **{pred.safety.value.upper()}** `{pred.model_name}` ({mat_info})")
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
            f"> **WARNING**: Could not extract columns for: {names} -- manual review required"
        )

    if result.skipped_models:
        names = ", ".join(result.skipped_models)
        lines.append(
            f"> **WARNING**: Skipped {len(result.skipped_models)} model(s)"
            f" not found in manifest: {names}"
        )

    lines.append(f"\n`{_summary_line(result)}`")

    return "\n".join(lines)


def format_json(result: CheckResult) -> str:
    """Format result as JSON for programmatic consumption."""
    models = []
    for pred in result.predictions:
        model = {
            "model_name": pred.model_name,
            "materialization": pred.materialization,
            "on_schema_change": pred.on_schema_change,
            "safety": pred.safety.value,
            "operations": [
                {"operation": op.operation, "column": op.column} for op in pred.operations
            ],
            "columns_added": pred.columns_added,
            "columns_removed": pred.columns_removed,
        }
        downstream = result.downstream_map.get(pred.model_name, [])
        if downstream:
            model["downstream"] = downstream
        models.append(model)

    output = {
        "summary": {
            "total": len(result.predictions),
            "safe": sum(1 for p in result.predictions if p.safety == Safety.SAFE),
            "warning": sum(1 for p in result.predictions if p.safety == Safety.WARNING),
            "destructive": sum(1 for p in result.predictions if p.safety == Safety.DESTRUCTIVE),
        },
        "models": models,
        "parse_failures": result.parse_failures,
        "skipped_models": result.skipped_models,
    }
    return json.dumps(output, indent=2)
