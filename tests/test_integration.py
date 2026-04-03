"""Integration test — full DDL prediction pipeline."""

import json
import shutil

import pytest

from dbt_plan.cli import main
from dbt_plan.columns import extract_columns
from dbt_plan.diff import diff_compiled_dirs
from dbt_plan.formatter import CheckResult, format_github, format_text
from dbt_plan.manifest import find_downstream, find_node_by_name, load_manifest
from dbt_plan.predictor import Safety, predict_ddl


@pytest.fixture
def project(tmp_path):
    """Create a complete test project: base, current compiled SQL, manifest."""
    # Base compiled SQL (snapshot)
    base_dir = tmp_path / "base"
    base_dir.mkdir()
    (base_dir / "int_unified.sql").write_text(
        "SELECT\n    event_id,\n    app_id,\n    data__device,\n"
        "    data__user,\n    event_date\nFROM all_events\n"
    )
    (base_dir / "dim_device.sql").write_text(
        "SELECT device_id, country FROM raw_devices\n"
    )

    # Current compiled SQL (modified)
    current_dir = tmp_path / "current"
    current_dir.mkdir()
    (current_dir / "int_unified.sql").write_text(
        "SELECT\n    event_id,\n    app_id,\n    data__device__uuid,\n"
        "    data__user__id,\n    event_date\nFROM all_events\n"
    )
    (current_dir / "dim_device.sql").write_text(
        "SELECT device_id, country, platform FROM raw_devices\n"
    )
    (current_dir / "new_model.sql").write_text(
        "SELECT id, name FROM new_table\n"
    )

    # Manifest
    manifest_data = {
        "nodes": {
            "model.test.int_unified": {
                "name": "int_unified",
                "config": {
                    "materialized": "incremental",
                    "on_schema_change": "sync_all_columns",
                },
            },
            "model.test.dim_device": {
                "name": "dim_device",
                "config": {"materialized": "table"},
            },
            "model.test.new_model": {
                "name": "new_model",
                "config": {"materialized": "table"},
            },
        },
        "child_map": {
            "model.test.int_unified": [
                "model.test.dim_device",
                "test.test.not_null.abc",
            ],
            "model.test.dim_device": [],
            "model.test.new_model": [],
        },
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_data))

    return tmp_path, base_dir, current_dir, manifest_path


class TestFullPipeline:
    def test_diff_extract_predict_format(self, project):
        """E2E: diff → extract → predict → downstream → format."""
        _, base_dir, current_dir, manifest_path = project

        # 1. Diff
        diffs = diff_compiled_dirs(base_dir, current_dir)
        assert len(diffs) == 3
        modified = [d for d in diffs if d.status == "modified"]
        added = [d for d in diffs if d.status == "added"]
        assert len(modified) == 2
        assert len(added) == 1
        assert added[0].model_name == "new_model"

        # 2. Load manifest
        manifest = load_manifest(manifest_path)

        # 3. Predict int_unified (sync_all_columns + columns changed)
        unified_diff = next(d for d in diffs if d.model_name == "int_unified")
        base_cols = extract_columns(unified_diff.base_path.read_text())
        current_cols = extract_columns(unified_diff.current_path.read_text())
        assert base_cols is not None
        assert current_cols is not None

        node = find_node_by_name("int_unified", manifest)
        assert node is not None
        assert node.materialization == "incremental"
        assert node.on_schema_change == "sync_all_columns"

        prediction = predict_ddl(
            "int_unified",
            node.materialization,
            node.on_schema_change,
            base_cols,
            current_cols,
            status="modified",
        )
        assert prediction.safety == Safety.DESTRUCTIVE
        assert "data__device" in prediction.columns_removed
        assert "data__user" in prediction.columns_removed
        assert "data__device__uuid" in prediction.columns_added
        assert "data__user__id" in prediction.columns_added

        # 4. Downstream
        child_map = manifest.get("child_map", {})
        downstream = find_downstream(node.node_id, child_map)
        assert "model.test.dim_device" in downstream

        # 5. Format (smoke test)
        check_result = CheckResult(
            predictions=[prediction],
            downstream_map={"int_unified": ["dim_device"]},
            parse_failures=[],
        )
        text_output = format_text(check_result)
        assert "DESTRUCTIVE" in text_output
        assert "int_unified" in text_output
        assert "DROP COLUMN" in text_output

        github_output = format_github(check_result)
        assert "DESTRUCTIVE" in github_output
        assert "int_unified" in github_output


class TestCLIExitCode:
    def test_destructive_exits_1(self, project, monkeypatch):
        """CLI exits 1 when destructive DDL detected."""
        tmp_path, base_dir, current_dir, manifest_path = project

        # Create target structure matching _find_compiled_dir expectation
        target_models = tmp_path / "target" / "compiled" / "test" / "models"
        target_models.mkdir(parents=True)
        for f in current_dir.iterdir():
            shutil.copy(f, target_models / f.name)
        shutil.copy(manifest_path, tmp_path / "target" / "manifest.json")

        monkeypatch.setattr(
            "sys.argv",
            [
                "dbt-plan",
                "check",
                "--project-dir", str(tmp_path),
                "--base-dir", str(base_dir),
                "--format", "text",
            ],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
