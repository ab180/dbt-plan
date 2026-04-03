"""Tests for predict_ddl — DDL prediction rules."""

from dbt_plan.predictor import DDLPrediction, DDLOperation, Safety, predict_ddl


class TestTableAndView:
    def test_table_always_safe(self):
        """table materialization → CREATE OR REPLACE TABLE, always safe."""
        result = predict_ddl(
            model_name="dim_device",
            materialization="table",
            on_schema_change=None,
            base_columns=["a", "b"],
            current_columns=["a", "c"],
        )
        assert result.safety == Safety.SAFE
        assert any(op.operation == "CREATE OR REPLACE TABLE" for op in result.operations)

    def test_view_always_safe(self):
        """view materialization → CREATE OR REPLACE VIEW, always safe."""
        result = predict_ddl(
            model_name="vw_report",
            materialization="view",
            on_schema_change=None,
            base_columns=["x"],
            current_columns=["x", "y"],
        )
        assert result.safety == Safety.SAFE
        assert any(op.operation == "CREATE OR REPLACE VIEW" for op in result.operations)


class TestIncrementalIgnoreAndFail:
    def test_incremental_ignore_safe(self):
        """incremental + ignore → NO DDL, safe."""
        result = predict_ddl(
            model_name="fct_events",
            materialization="incremental",
            on_schema_change="ignore",
            base_columns=["a", "b"],
            current_columns=["a", "c"],
        )
        assert result.safety == Safety.SAFE
        assert any(op.operation == "NO DDL" for op in result.operations)

    def test_incremental_fail_warning(self):
        """incremental + fail + columns changed → WARNING, BUILD FAILURE."""
        result = predict_ddl(
            model_name="fct_strict",
            materialization="incremental",
            on_schema_change="fail",
            base_columns=["a", "b"],
            current_columns=["a", "c"],
        )
        assert result.safety == Safety.WARNING
        assert any(op.operation == "BUILD FAILURE" for op in result.operations)


class TestIncrementalAppendAndSync:
    def test_append_add_only(self):
        """incremental + append_new_columns → ADD COLUMN only, safe."""
        result = predict_ddl(
            model_name="fct_metrics",
            materialization="incremental",
            on_schema_change="append_new_columns",
            base_columns=["a", "b"],
            current_columns=["a", "b", "c"],
        )
        assert result.safety == Safety.SAFE
        assert result.columns_added == ["c"]
        assert result.columns_removed == []
        add_ops = [op for op in result.operations if op.operation == "ADD COLUMN"]
        assert len(add_ops) == 1
        assert add_ops[0].column == "c"

    def test_sync_destructive_when_columns_removed(self):
        """incremental + sync_all_columns + columns removed → DESTRUCTIVE."""
        result = predict_ddl(
            model_name="int_unified",
            materialization="incremental",
            on_schema_change="sync_all_columns",
            base_columns=["a", "b", "c"],
            current_columns=["a", "d"],
        )
        assert result.safety == Safety.DESTRUCTIVE
        assert sorted(result.columns_removed) == ["b", "c"]
        assert result.columns_added == ["d"]
        drop_ops = [op for op in result.operations if op.operation == "DROP COLUMN"]
        assert len(drop_ops) == 2

    def test_sync_safe_when_no_changes(self):
        """incremental + sync_all_columns + same columns → SAFE, no ops."""
        result = predict_ddl(
            model_name="int_unified",
            materialization="incremental",
            on_schema_change="sync_all_columns",
            base_columns=["a", "b"],
            current_columns=["a", "b"],
        )
        assert result.safety == Safety.SAFE
        assert result.operations == []
        assert result.columns_added == []
        assert result.columns_removed == []


class TestNewModel:
    def test_new_model_added(self):
        """New model (status=added) → SAFE (no existing table to alter)."""
        result = predict_ddl(
            model_name="new_model",
            materialization="incremental",
            on_schema_change="sync_all_columns",
            base_columns=None,
            current_columns=["a", "b"],
            status="added",
        )
        assert result.safety == Safety.SAFE


class TestRemovedModel:
    def test_removed_model_destructive(self):
        """Removed model → DESTRUCTIVE regardless of materialization."""
        result = predict_ddl(
            model_name="old_model",
            materialization="table",
            on_schema_change=None,
            base_columns=["a", "b"],
            current_columns=None,
            status="removed",
        )
        assert result.safety == Safety.DESTRUCTIVE
        assert any(op.operation == "MODEL REMOVED" for op in result.operations)

    def test_removed_incremental_destructive(self):
        """Removed incremental model → DESTRUCTIVE."""
        result = predict_ddl(
            model_name="old_inc",
            materialization="incremental",
            on_schema_change="sync_all_columns",
            base_columns=["a"],
            current_columns=None,
            status="removed",
        )
        assert result.safety == Safety.DESTRUCTIVE


class TestParseFailure:
    def test_parse_failure_base_never_safe(self):
        """Modified model with base parse failure → WARNING (never safe)."""
        result = predict_ddl(
            model_name="m",
            materialization="incremental",
            on_schema_change="sync_all_columns",
            base_columns=None,
            current_columns=["a", "b"],
            status="modified",
        )
        assert result.safety == Safety.WARNING
        assert any("REVIEW" in op.operation for op in result.operations)

    def test_parse_failure_current_never_safe(self):
        """Modified model with current parse failure → WARNING (never safe)."""
        result = predict_ddl(
            model_name="m",
            materialization="incremental",
            on_schema_change="append_new_columns",
            base_columns=["a", "b"],
            current_columns=None,
            status="modified",
        )
        assert result.safety == Safety.WARNING

    def test_parse_failure_both_never_safe(self):
        """Modified model with both parse failures → WARNING."""
        result = predict_ddl(
            model_name="m",
            materialization="incremental",
            on_schema_change="sync_all_columns",
            base_columns=None,
            current_columns=None,
            status="modified",
        )
        assert result.safety == Safety.WARNING


class TestSelectStarWildcard:
    def test_select_star_base_warning(self):
        """SELECT * in base → WARNING (cannot diff columns)."""
        result = predict_ddl(
            model_name="m",
            materialization="incremental",
            on_schema_change="sync_all_columns",
            base_columns=["*"],
            current_columns=["a", "b"],
        )
        assert result.safety == Safety.WARNING
        assert any("SELECT *" in op.operation for op in result.operations)

    def test_select_star_current_warning(self):
        """SELECT * in current → WARNING."""
        result = predict_ddl(
            model_name="m",
            materialization="incremental",
            on_schema_change="sync_all_columns",
            base_columns=["a", "b"],
            current_columns=["*"],
        )
        assert result.safety == Safety.WARNING


class TestEphemeralAndUnknown:
    def test_ephemeral_safe(self):
        """Ephemeral model → SAFE, no operations."""
        result = predict_ddl(
            model_name="eph",
            materialization="ephemeral",
            on_schema_change=None,
            base_columns=None,
            current_columns=None,
            status="added",
        )
        assert result.safety == Safety.SAFE
        assert result.operations == []

    def test_unknown_on_schema_change_warning(self):
        """Unknown on_schema_change value → WARNING."""
        result = predict_ddl(
            model_name="m",
            materialization="incremental",
            on_schema_change="unknown_value",
            base_columns=["a"],
            current_columns=["a", "b"],
        )
        assert result.safety == Safety.WARNING
