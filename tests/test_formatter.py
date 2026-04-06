"""Tests for text, github, and json formatters."""

import json

from dbt_plan.formatter import CheckResult, format_github, format_json, format_text
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


class TestFormatTextColor:
    def test_color_enabled_includes_ansi_codes(self):
        """color=True wraps safety labels with ANSI escape codes."""
        result = CheckResult(
            predictions=[
                DDLPrediction(
                    model_name="m",
                    materialization="table",
                    on_schema_change=None,
                    safety=Safety.SAFE,
                    operations=[DDLOperation("CREATE OR REPLACE TABLE")],
                ),
            ],
        )
        output = format_text(result, color=True)
        assert "\033[32m" in output  # green
        assert "\033[1m" in output  # bold
        assert "\033[0m" in output  # reset

    def test_color_disabled_no_ansi_codes(self):
        """color=False produces plain text without ANSI escape codes."""
        result = CheckResult(
            predictions=[
                DDLPrediction(
                    model_name="m",
                    materialization="table",
                    on_schema_change=None,
                    safety=Safety.DESTRUCTIVE,
                    operations=[DDLOperation("MODEL REMOVED")],
                ),
            ],
        )
        output = format_text(result, color=False)
        assert "\033[" not in output
        assert "DESTRUCTIVE" in output


class TestFormatTextSkippedModels:
    def test_skipped_models_warning_shown(self):
        """Skipped models produce a WARNING line in text output."""
        result = CheckResult(
            predictions=[
                DDLPrediction(
                    model_name="m",
                    materialization="table",
                    on_schema_change=None,
                    safety=Safety.SAFE,
                    operations=[DDLOperation("CREATE OR REPLACE TABLE")],
                ),
            ],
            skipped_models=["orphan_model", "unknown_model"],
        )
        output = format_text(result, color=False)
        assert "WARNING" in output
        assert "Skipped 2 model(s)" in output
        assert "orphan_model" in output
        assert "unknown_model" in output


class TestFormatTextParseFailuresColor:
    def test_parse_failures_warning_with_color(self):
        """Parse failures WARNING is colored when color=True."""
        result = CheckResult(
            predictions=[
                DDLPrediction(
                    model_name="m",
                    materialization="incremental",
                    on_schema_change="sync_all_columns",
                    safety=Safety.WARNING,
                    operations=[DDLOperation("REVIEW REQUIRED")],
                ),
            ],
            parse_failures=["broken_model"],
        )
        output = format_text(result, color=True)
        assert "\033[33m" in output  # yellow for WARNING
        assert "broken_model" in output
        assert "manual review required" in output


class TestFormatGithubSkippedModels:
    def test_skipped_models_warning_in_github(self):
        """Skipped models produce a blockquote WARNING in github output."""
        result = CheckResult(
            predictions=[
                DDLPrediction(
                    model_name="m",
                    materialization="table",
                    on_schema_change=None,
                    safety=Safety.SAFE,
                    operations=[DDLOperation("CREATE OR REPLACE TABLE")],
                ),
            ],
            skipped_models=["orphan_model"],
        )
        output = format_github(result)
        assert "> **WARNING**: Skipped 1 model(s)" in output
        assert "orphan_model" in output


class TestFormatJson:
    def test_json_output_structure(self):
        """JSON output has summary, models, parse_failures, skipped_models."""
        result = CheckResult(
            predictions=[
                DDLPrediction(
                    model_name="int_unified",
                    materialization="incremental",
                    on_schema_change="sync_all_columns",
                    safety=Safety.DESTRUCTIVE,
                    operations=[
                        DDLOperation("DROP COLUMN", "old_col"),
                        DDLOperation("ADD COLUMN", "new_col"),
                    ],
                    columns_added=["new_col"],
                    columns_removed=["old_col"],
                ),
                DDLPrediction(
                    model_name="dim_device",
                    materialization="table",
                    on_schema_change=None,
                    safety=Safety.SAFE,
                    operations=[DDLOperation("CREATE OR REPLACE TABLE")],
                ),
            ],
            downstream_map={"int_unified": ["dim_device"]},
            parse_failures=["broken_model"],
            skipped_models=["unknown_model"],
        )
        raw = format_json(result)
        data = json.loads(raw)

        assert data["summary"]["total"] == 2
        assert data["summary"]["destructive"] == 1
        assert data["summary"]["safe"] == 1
        assert len(data["models"]) == 2
        assert data["models"][0]["model_name"] == "int_unified"
        assert data["models"][0]["safety"] == "destructive"
        assert data["models"][0]["downstream"] == ["dim_device"]
        assert data["parse_failures"] == ["broken_model"]
        assert data["skipped_models"] == ["unknown_model"]

    def test_json_empty_result(self):
        """Empty result produces valid JSON with zero counts."""
        result = CheckResult()
        raw = format_json(result)
        data = json.loads(raw)
        assert data["summary"]["total"] == 0
        assert data["models"] == []
