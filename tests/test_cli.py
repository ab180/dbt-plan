"""Tests for CLI entry points — snapshot and check subcommands."""

import json
from unittest.mock import patch

import pytest

from dbt_plan.cli import _do_check, _do_snapshot, main


def _make_project(tmp_path, *, models_sql=None, manifest=None, base_sql=None, base_manifest=None):
    """Helper to set up a fake dbt project structure for CLI tests.

    Returns:
        project_dir Path
    """
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    # target/compiled/{proj}/models/
    target = project_dir / "target"
    compiled = target / "compiled" / "my_project" / "models"
    compiled.mkdir(parents=True)
    if models_sql:
        for name, sql in models_sql.items():
            (compiled / f"{name}.sql").write_text(sql)

    # manifest.json
    if manifest is not None:
        (target / "manifest.json").write_text(json.dumps(manifest))

    # base snapshot
    if base_sql is not None:
        base_compiled = project_dir / ".dbt-plan" / "base" / "compiled"
        base_compiled.mkdir(parents=True)
        for name, sql in base_sql.items():
            (base_compiled / f"{name}.sql").write_text(sql)

    if base_manifest is not None:
        base_dir = project_dir / ".dbt-plan" / "base"
        base_dir.mkdir(parents=True, exist_ok=True)
        (base_dir / "manifest.json").write_text(json.dumps(base_manifest))

    return project_dir


def _make_check_args(project_dir, fmt="text", no_color=True, manifest=None, select=None):
    """Build an argparse.Namespace for _do_check."""
    import argparse

    return argparse.Namespace(
        project_dir=str(project_dir),
        target_dir="target",
        base_dir=".dbt-plan/base",
        manifest=manifest,
        format=fmt,
        no_color=no_color,
        select=select,
        verbose=False,
        dialect=None,
    )


def _make_snapshot_args(project_dir):
    import argparse

    return argparse.Namespace(
        project_dir=str(project_dir),
        target_dir="target",
    )


class TestSnapshotPathValidation:
    def test_snapshot_rejects_base_dir_escaping_project(self, tmp_path):
        """Snapshot aborts if base_dir resolves outside project directory."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()

        # Create compiled SQL so we get past the first check
        compiled = project_dir / "target" / "compiled" / "proj" / "models"
        compiled.mkdir(parents=True)
        (compiled / "m.sql").write_text("SELECT 1")

        # Create a symlink that escapes the project dir
        escape_target = tmp_path / "outside"
        escape_target.mkdir()
        base_dir = project_dir / ".dbt-plan" / "base"
        base_dir.parent.mkdir(parents=True, exist_ok=True)
        base_dir.symlink_to(escape_target)

        args = _make_snapshot_args(project_dir)
        with pytest.raises(SystemExit) as exc_info:
            _do_snapshot(args)
        assert exc_info.value.code == 2

    def test_snapshot_missing_manifest_warns(self, tmp_path, capsys):
        """Snapshot prints warning when manifest.json is missing from target/."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()

        compiled = project_dir / "target" / "compiled" / "proj" / "models"
        compiled.mkdir(parents=True)
        (compiled / "m.sql").write_text("SELECT 1")
        # No manifest.json created

        args = _make_snapshot_args(project_dir)
        _do_snapshot(args)

        captured = capsys.readouterr()
        assert "Warning: manifest.json not found" in captured.err
        assert "Snapshot saved to" in captured.out


class TestCheckValueErrorCatch:
    def test_check_catches_duplicate_model_valueerror(self, tmp_path, capsys):
        """check returns 2 when diff_compiled_dirs raises ValueError (duplicate models)."""
        manifest = {
            "nodes": {"model.p.m": {"name": "m", "config": {"materialized": "table"}}},
            "child_map": {},
        }
        project_dir = _make_project(
            tmp_path,
            models_sql={"m": "SELECT 1"},
            manifest=manifest,
            base_sql={"m": "SELECT 1"},
            base_manifest=manifest,
        )

        # Create duplicate in current compiled dir
        compiled = project_dir / "target" / "compiled" / "my_project" / "models"
        sub = compiled / "subdir"
        sub.mkdir()
        (sub / "m.sql").write_text("SELECT 2")

        args = _make_check_args(project_dir)
        exit_code = _do_check(args)
        assert exit_code == 2
        captured = capsys.readouterr()
        assert "Duplicate model name" in captured.err


class TestCheckJsonFormatRouting:
    def test_empty_diff_json_format(self, tmp_path, capsys):
        """When no diffs exist, --format=json outputs valid JSON."""
        manifest = {
            "nodes": {"model.p.m": {"name": "m", "config": {"materialized": "table"}}},
            "child_map": {},
        }
        project_dir = _make_project(
            tmp_path,
            models_sql={"m": "SELECT 1"},
            manifest=manifest,
            base_sql={"m": "SELECT 1"},
            base_manifest=manifest,
        )

        args = _make_check_args(project_dir, fmt="json")
        exit_code = _do_check(args)
        assert exit_code == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["summary"]["total"] == 0

    def test_empty_diff_github_format(self, tmp_path, capsys):
        """When no diffs exist, --format=github outputs markdown header."""
        manifest = {
            "nodes": {"model.p.m": {"name": "m", "config": {"materialized": "table"}}},
            "child_map": {},
        }
        project_dir = _make_project(
            tmp_path,
            models_sql={"m": "SELECT 1"},
            manifest=manifest,
            base_sql={"m": "SELECT 1"},
            base_manifest=manifest,
        )

        args = _make_check_args(project_dir, fmt="github")
        exit_code = _do_check(args)
        assert exit_code == 0
        captured = capsys.readouterr()
        assert "no model changes detected" in captured.out

    def test_empty_diff_text_no_color(self, tmp_path, capsys):
        """When no diffs exist, --format=text --no-color outputs plain text."""
        manifest = {
            "nodes": {"model.p.m": {"name": "m", "config": {"materialized": "table"}}},
            "child_map": {},
        }
        project_dir = _make_project(
            tmp_path,
            models_sql={"m": "SELECT 1"},
            manifest=manifest,
            base_sql={"m": "SELECT 1"},
            base_manifest=manifest,
        )

        args = _make_check_args(project_dir, fmt="text", no_color=True)
        exit_code = _do_check(args)
        assert exit_code == 0
        captured = capsys.readouterr()
        assert "no model changes detected" in captured.out
        assert "\033[" not in captured.out


class TestCheckSkippedModels:
    def test_model_not_in_manifest_is_skipped(self, tmp_path, capsys):
        """Model in diff but not in any manifest → appears in skipped_models."""
        manifest = {
            "nodes": {},
            "child_map": {},
        }
        project_dir = _make_project(
            tmp_path,
            models_sql={"orphan": "SELECT 1"},
            manifest=manifest,
            base_sql={},
            base_manifest=manifest,
        )

        args = _make_check_args(project_dir, fmt="json")
        _do_check(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert "orphan" in data["skipped_models"]


class TestCheckJsonFormatWithPredictions:
    def test_check_json_with_changes(self, tmp_path, capsys):
        """check --format=json with actual model changes produces valid JSON."""
        manifest = {
            "nodes": {
                "model.p.m": {
                    "name": "m",
                    "config": {"materialized": "table"},
                },
            },
            "child_map": {},
        }
        project_dir = _make_project(
            tmp_path,
            models_sql={"m": "SELECT a, b, c FROM t"},
            manifest=manifest,
            base_sql={"m": "SELECT a, b FROM t"},
            base_manifest=manifest,
        )

        args = _make_check_args(project_dir, fmt="json")
        exit_code = _do_check(args)
        assert exit_code == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["summary"]["total"] == 1
        assert data["models"][0]["model_name"] == "m"
        assert data["models"][0]["safety"] == "safe"


class TestSelectFilter:
    def test_select_single_model(self, tmp_path, capsys):
        """--select filters results to only the specified model."""
        manifest = {
            "nodes": {
                "model.p.m1": {
                    "name": "m1",
                    "config": {"materialized": "table"},
                },
                "model.p.m2": {
                    "name": "m2",
                    "config": {"materialized": "table"},
                },
            },
            "child_map": {},
        }
        project_dir = _make_project(
            tmp_path,
            models_sql={"m1": "SELECT a, b FROM t", "m2": "SELECT x, y FROM t"},
            manifest=manifest,
            base_sql={"m1": "SELECT a FROM t", "m2": "SELECT x FROM t"},
            base_manifest=manifest,
        )

        args = _make_check_args(project_dir, fmt="json", select="m1")
        exit_code = _do_check(args)
        assert exit_code == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["summary"]["total"] == 1
        assert data["models"][0]["model_name"] == "m1"

    def test_select_multiple_models(self, tmp_path, capsys):
        """--select with comma-separated models includes all specified."""
        manifest = {
            "nodes": {
                "model.p.m1": {
                    "name": "m1",
                    "config": {"materialized": "table"},
                },
                "model.p.m2": {
                    "name": "m2",
                    "config": {"materialized": "table"},
                },
                "model.p.m3": {
                    "name": "m3",
                    "config": {"materialized": "table"},
                },
            },
            "child_map": {},
        }
        project_dir = _make_project(
            tmp_path,
            models_sql={
                "m1": "SELECT a, b FROM t",
                "m2": "SELECT x, y FROM t",
                "m3": "SELECT p, q FROM t",
            },
            manifest=manifest,
            base_sql={
                "m1": "SELECT a FROM t",
                "m2": "SELECT x FROM t",
                "m3": "SELECT p FROM t",
            },
            base_manifest=manifest,
        )

        args = _make_check_args(project_dir, fmt="json", select="m1,m3")
        exit_code = _do_check(args)
        assert exit_code == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        model_names = sorted(m["model_name"] for m in data["models"])
        assert model_names == ["m1", "m3"]

    def test_select_no_match_returns_empty(self, tmp_path, capsys):
        """--select with non-existent model name results in empty output."""
        manifest = {
            "nodes": {
                "model.p.m1": {
                    "name": "m1",
                    "config": {"materialized": "table"},
                },
            },
            "child_map": {},
        }
        project_dir = _make_project(
            tmp_path,
            models_sql={"m1": "SELECT a, b FROM t"},
            manifest=manifest,
            base_sql={"m1": "SELECT a FROM t"},
            base_manifest=manifest,
        )

        args = _make_check_args(project_dir, fmt="json", select="nonexistent")
        exit_code = _do_check(args)
        assert exit_code == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["summary"]["total"] == 0


class TestManifestColumnFallback:
    def test_current_cols_fallback_from_manifest(self, tmp_path, capsys):
        """When extract_columns returns ['*'], use node.columns from manifest."""
        manifest = {
            "nodes": {
                "model.p.m": {
                    "name": "m",
                    "config": {
                        "materialized": "incremental",
                        "on_schema_change": "sync_all_columns",
                    },
                    "columns": {"id": {}, "name": {}, "email": {}},
                },
            },
            "child_map": {},
        }
        project_dir = _make_project(
            tmp_path,
            # Current SQL uses SELECT * which triggers fallback
            models_sql={"m": "SELECT * FROM t"},
            manifest=manifest,
            base_sql={"m": "SELECT id, name FROM t"},
            base_manifest=manifest,
        )

        args = _make_check_args(project_dir, fmt="json")
        _do_check(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["summary"]["total"] == 1
        pred = data["models"][0]
        assert pred["model_name"] == "m"
        # With manifest fallback, columns are resolved so we get a real prediction
        # (ADD COLUMN email) instead of "REVIEW REQUIRED (SELECT *)"
        op_texts = [op["operation"] for op in pred["operations"]]
        assert all("REVIEW REQUIRED" not in op for op in op_texts)
        assert "email" in pred["columns_added"]

    def test_base_cols_fallback_from_base_manifest(self, tmp_path, capsys):
        """When base extract_columns returns ['*'], use base_node.columns from base manifest."""
        manifest = {
            "nodes": {
                "model.p.m": {
                    "name": "m",
                    "config": {
                        "materialized": "incremental",
                        "on_schema_change": "sync_all_columns",
                    },
                    "columns": {"id": {}, "name": {}, "email": {}},
                },
            },
            "child_map": {},
        }
        base_manifest = {
            "nodes": {
                "model.p.m": {
                    "name": "m",
                    "config": {
                        "materialized": "incremental",
                        "on_schema_change": "sync_all_columns",
                    },
                    "columns": {"id": {}, "name": {}},
                },
            },
            "child_map": {},
        }
        project_dir = _make_project(
            tmp_path,
            models_sql={"m": "SELECT id, name, email FROM t"},
            manifest=manifest,
            # Base SQL uses SELECT * which triggers fallback
            base_sql={"m": "SELECT * FROM t"},
            base_manifest=base_manifest,
        )

        args = _make_check_args(project_dir, fmt="json")
        _do_check(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["summary"]["total"] == 1
        pred = data["models"][0]
        assert pred["model_name"] == "m"
        # With base manifest fallback, we get a real column diff (ADD COLUMN email)
        op_texts = [op["operation"] for op in pred["operations"]]
        assert all("REVIEW REQUIRED" not in op for op in op_texts)
        assert "email" in pred["columns_added"]

    def test_no_fallback_without_manifest_columns(self, tmp_path, capsys):
        """When manifest has no columns and SQL is SELECT *, no fallback occurs."""
        manifest = {
            "nodes": {
                "model.p.m": {
                    "name": "m",
                    "config": {
                        "materialized": "incremental",
                        "on_schema_change": "sync_all_columns",
                    },
                    # No columns defined in manifest
                },
            },
            "child_map": {},
        }
        project_dir = _make_project(
            tmp_path,
            models_sql={"m": "SELECT * FROM t"},
            manifest=manifest,
            base_sql={"m": "SELECT id, name FROM t"},
            base_manifest=manifest,
        )

        args = _make_check_args(project_dir, fmt="json")
        _do_check(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["summary"]["total"] == 1
        pred = data["models"][0]
        # Without fallback columns, the prediction should indicate review needed
        op_texts = [op["operation"] for op in pred["operations"]]
        assert any("REVIEW REQUIRED" in op for op in op_texts)


class TestMainNoCommand:
    def test_no_subcommand_exits_zero(self):
        """Running dbt-plan with no subcommand exits with code 0."""
        with patch("sys.argv", ["dbt-plan"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
