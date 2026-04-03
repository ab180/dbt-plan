"""Tests for text and github formatters."""

from dbt_plan.formatter import CheckResult, format_github, format_text
from dbt_plan.predictor import DDLOperation, DDLPrediction, Safety


class TestFormatText:
    def test_destructive_output(self):
        """Destructive prediction shows DROP COLUMN in terminal output."""
        result = CheckResult(
            predictions=[
                DDLPrediction(
                    model_name="int_unified",
                    materialization="incremental",
                    on_schema_change="sync_all_columns",
                    safety=Safety.DESTRUCTIVE,
                    operations=[
                        DDLOperation("DROP COLUMN", "data__device"),
                        DDLOperation("ADD COLUMN", "data__device__uuid"),
                    ],
                    columns_added=["data__device__uuid"],
                    columns_removed=["data__device"],
                ),
            ],
            downstream_map={"int_unified": ["dim_device"]},
            parse_failures=[],
        )
        output = format_text(result)
        assert "DESTRUCTIVE" in output
        assert "int_unified" in output
        assert "DROP COLUMN" in output
        assert "data__device" in output
        assert "dim_device" in output

    def test_empty_no_changes(self):
        """No predictions → 'no model changes detected'."""
        result = CheckResult(predictions=[], downstream_map={}, parse_failures=[])
        output = format_text(result)
        assert "no model changes detected" in output


class TestFormatGithub:
    def test_safe_markdown(self):
        """Safe prediction uses markdown formatting."""
        result = CheckResult(
            predictions=[
                DDLPrediction(
                    model_name="dim_device",
                    materialization="table",
                    on_schema_change=None,
                    safety=Safety.SAFE,
                    operations=[DDLOperation("CREATE OR REPLACE TABLE")],
                ),
            ],
            downstream_map={},
            parse_failures=[],
        )
        output = format_github(result)
        assert "SAFE" in output
        assert "dim_device" in output
        assert "CREATE OR REPLACE TABLE" in output
