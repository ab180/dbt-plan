"""DDL prediction rules based on materialization and on_schema_change."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Safety(Enum):
    SAFE = "safe"
    WARNING = "warning"
    DESTRUCTIVE = "destructive"


@dataclass(frozen=True)
class DDLOperation:
    """A single DDL operation that will be executed."""

    operation: str  # "CREATE OR REPLACE TABLE", "ADD COLUMN", "DROP COLUMN", etc.
    column: str | None = None  # column name for ADD/DROP


@dataclass(frozen=True)
class DDLPrediction:
    """Complete DDL prediction for a model."""

    model_name: str
    materialization: str
    on_schema_change: str | None
    safety: Safety
    operations: list[DDLOperation] = field(default_factory=list)
    columns_added: list[str] = field(default_factory=list)
    columns_removed: list[str] = field(default_factory=list)


def predict_ddl(
    model_name: str,
    materialization: str,
    on_schema_change: str | None,
    base_columns: list[str] | None,
    current_columns: list[str] | None,
    status: str = "modified",
) -> DDLPrediction:
    """Predict DDL operations from materialization config and column diff.

    Args:
        model_name: dbt model name.
        materialization: "table", "view", "incremental", "ephemeral".
        on_schema_change: "ignore", "fail", "append_new_columns",
                          "sync_all_columns", or None.
        base_columns: Columns from base (None if parse failed).
        current_columns: Columns from current (None if parse failed).
        status: "added", "modified", or "removed" from diff.

    Returns:
        DDLPrediction with safety level and predicted operations.
    """
    # Removed model → destructive (physical table/view will be orphaned)
    # Exception: ephemeral models have no physical object in Snowflake
    if status == "removed":
        if materialization == "ephemeral":
            return DDLPrediction(
                model_name=model_name,
                materialization=materialization,
                on_schema_change=on_schema_change,
                safety=Safety.SAFE,
            )
        return DDLPrediction(
            model_name=model_name,
            materialization=materialization,
            on_schema_change=on_schema_change,
            safety=Safety.DESTRUCTIVE,
            operations=[DDLOperation("MODEL REMOVED")],
        )

    if materialization == "table":
        return DDLPrediction(
            model_name=model_name,
            materialization=materialization,
            on_schema_change=on_schema_change,
            safety=Safety.SAFE,
            operations=[DDLOperation("CREATE OR REPLACE TABLE")],
        )

    if materialization == "view":
        return DDLPrediction(
            model_name=model_name,
            materialization=materialization,
            on_schema_change=on_schema_change,
            safety=Safety.SAFE,
            operations=[DDLOperation("CREATE OR REPLACE VIEW")],
        )

    if materialization == "ephemeral":
        return DDLPrediction(
            model_name=model_name,
            materialization=materialization,
            on_schema_change=on_schema_change,
            safety=Safety.SAFE,
        )

    if materialization == "snapshot":
        # dbt snapshots use CREATE TABLE IF NOT EXISTS + MERGE
        # Schema changes are not auto-managed, so warn for review
        return DDLPrediction(
            model_name=model_name,
            materialization=materialization,
            on_schema_change=on_schema_change,
            safety=Safety.WARNING,
            operations=[DDLOperation("REVIEW REQUIRED (snapshot)")],
        )

    # Incremental: depends on on_schema_change
    osc = on_schema_change or "ignore"

    if osc == "ignore":
        return DDLPrediction(
            model_name=model_name,
            materialization=materialization,
            on_schema_change=osc,
            safety=Safety.SAFE,
            operations=[DDLOperation("NO DDL")],
        )

    # New model (status=added, no base) → safe, no existing table to alter
    if status == "added":
        return DDLPrediction(
            model_name=model_name,
            materialization=materialization,
            on_schema_change=osc,
            safety=Safety.SAFE,
        )

    # From here: status == "modified"
    # Parse failure on either side → WARNING (절대 safe 반환 금지)
    if base_columns is None or current_columns is None:
        return DDLPrediction(
            model_name=model_name,
            materialization=materialization,
            on_schema_change=osc,
            safety=Safety.WARNING,
            operations=[DDLOperation("REVIEW REQUIRED")],
        )

    # SELECT * on either side → cannot determine column diff
    if base_columns == ["*"] or current_columns == ["*"]:
        return DDLPrediction(
            model_name=model_name,
            materialization=materialization,
            on_schema_change=osc,
            safety=Safety.WARNING,
            operations=[DDLOperation("REVIEW REQUIRED (SELECT *)")],
        )

    # Column diff
    base_set = set(base_columns)
    current_set = set(current_columns)
    added = sorted(current_set - base_set)
    removed = sorted(base_set - current_set)

    if osc == "fail":
        if added or removed:
            return DDLPrediction(
                model_name=model_name,
                materialization=materialization,
                on_schema_change=osc,
                safety=Safety.WARNING,
                operations=[DDLOperation("BUILD FAILURE")],
                columns_added=added,
                columns_removed=removed,
            )
        return DDLPrediction(
            model_name=model_name,
            materialization=materialization,
            on_schema_change=osc,
            safety=Safety.SAFE,
        )

    if osc == "append_new_columns":
        ops = [DDLOperation("ADD COLUMN", col) for col in added]
        # Columns removed from SQL will remain in the physical table as stale data
        safety = Safety.WARNING if removed else Safety.SAFE
        if removed:
            ops.append(DDLOperation("STALE COLUMNS (not populated)"))
        return DDLPrediction(
            model_name=model_name,
            materialization=materialization,
            on_schema_change=osc,
            safety=safety,
            operations=ops,
            columns_added=added,
            columns_removed=removed,
        )

    if osc == "sync_all_columns":
        ops = [DDLOperation("ADD COLUMN", col) for col in added]
        ops += [DDLOperation("DROP COLUMN", col) for col in removed]
        safety = Safety.DESTRUCTIVE if removed else Safety.SAFE

        # Detect column reordering: sync_all_columns drops + re-adds
        # columns to match the new order even when the column set is unchanged
        if not added and not removed and base_columns != current_columns:
            ops = [DDLOperation("COLUMNS REORDERED")]
            safety = Safety.WARNING

        return DDLPrediction(
            model_name=model_name,
            materialization=materialization,
            on_schema_change=osc,
            safety=safety,
            operations=ops,
            columns_added=added,
            columns_removed=removed,
        )

    # Unknown on_schema_change
    return DDLPrediction(
        model_name=model_name,
        materialization=materialization,
        on_schema_change=osc,
        safety=Safety.WARNING,
    )
