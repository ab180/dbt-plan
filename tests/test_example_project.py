"""Test that the sample project produces expected predictions."""

from pathlib import Path

import pytest

from dbt_plan.cli import main

EXAMPLE_DIR = Path(__file__).parent.parent / "examples" / "sample-project"


@pytest.fixture
def example_project():
    """Verify example project exists."""
    assert EXAMPLE_DIR.exists(), f"Example project not found: {EXAMPLE_DIR}"
    return EXAMPLE_DIR


class TestExampleProject:
    def test_detects_destructive_change(self, example_project, monkeypatch, capsys):
        """int_unified DROP COLUMN → exit 1."""
        monkeypatch.setattr(
            "sys.argv",
            [
                "dbt-plan",
                "check",
                "--base-dir",
                str(example_project / "base"),
                "--project-dir",
                str(example_project / "current"),
                "--format",
                "text",
            ],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

        output = capsys.readouterr().out
        assert "DESTRUCTIVE" in output
        assert "int_unified" in output
        assert "DROP COLUMN" in output
        assert "data__device" in output
        assert "data__user" in output

    def test_detects_safe_table_rebuild(self, example_project, monkeypatch, capsys):
        """dim_device (table) → SAFE CREATE OR REPLACE."""
        monkeypatch.setattr(
            "sys.argv",
            [
                "dbt-plan",
                "check",
                "--base-dir",
                str(example_project / "base"),
                "--project-dir",
                str(example_project / "current"),
                "--format",
                "text",
            ],
        )
        with pytest.raises(SystemExit):
            main()

        output = capsys.readouterr().out
        assert "dim_device" in output
        assert "CREATE OR REPLACE TABLE" in output

    def test_detects_append_column(self, example_project, monkeypatch, capsys):
        """fct_daily_metrics (append_new_columns) → SAFE ADD COLUMN."""
        monkeypatch.setattr(
            "sys.argv",
            [
                "dbt-plan",
                "check",
                "--base-dir",
                str(example_project / "base"),
                "--project-dir",
                str(example_project / "current"),
                "--format",
                "text",
            ],
        )
        with pytest.raises(SystemExit):
            main()

        output = capsys.readouterr().out
        assert "fct_daily_metrics" in output
        assert "ADD COLUMN" in output
        assert "total_revenue" in output

    def test_detects_new_model(self, example_project, monkeypatch, capsys):
        """dim_campaign (new) → SAFE."""
        monkeypatch.setattr(
            "sys.argv",
            [
                "dbt-plan",
                "check",
                "--base-dir",
                str(example_project / "base"),
                "--project-dir",
                str(example_project / "current"),
                "--format",
                "text",
            ],
        )
        with pytest.raises(SystemExit):
            main()

        output = capsys.readouterr().out
        assert "dim_campaign" in output

    def test_downstream_impact(self, example_project, monkeypatch, capsys):
        """int_unified downstream → dim_device, fct_daily_metrics."""
        monkeypatch.setattr(
            "sys.argv",
            [
                "dbt-plan",
                "check",
                "--base-dir",
                str(example_project / "base"),
                "--project-dir",
                str(example_project / "current"),
                "--format",
                "text",
            ],
        )
        with pytest.raises(SystemExit):
            main()

        output = capsys.readouterr().out
        assert "Downstream:" in output
        assert "dim_device" in output
        assert "fct_daily_metrics" in output

    def test_github_format(self, example_project, monkeypatch, capsys):
        """--format github produces markdown."""
        monkeypatch.setattr(
            "sys.argv",
            [
                "dbt-plan",
                "check",
                "--base-dir",
                str(example_project / "base"),
                "--project-dir",
                str(example_project / "current"),
                "--format",
                "github",
            ],
        )
        with pytest.raises(SystemExit):
            main()

        output = capsys.readouterr().out
        assert "###" in output
        assert "**DESTRUCTIVE**" in output
        assert "`int_unified`" in output

    def test_model_count(self, example_project, monkeypatch, capsys):
        """4 models changed."""
        monkeypatch.setattr(
            "sys.argv",
            [
                "dbt-plan",
                "check",
                "--base-dir",
                str(example_project / "base"),
                "--project-dir",
                str(example_project / "current"),
                "--format",
                "text",
            ],
        )
        with pytest.raises(SystemExit):
            main()

        output = capsys.readouterr().out
        assert "4 model(s) changed" in output
