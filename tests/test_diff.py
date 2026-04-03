"""Tests for diff_compiled_dirs — compiled SQL directory comparison."""

from dbt_plan.diff import ModelDiff, diff_compiled_dirs


class TestDiffCompiledDirs:
    def test_modified_model(self, tmp_path):
        """Same filename, different content → status='modified'."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "my_model.sql").write_text("SELECT a, b FROM t")

        current = tmp_path / "current"
        current.mkdir()
        (current / "my_model.sql").write_text("SELECT a, c FROM t")

        result = diff_compiled_dirs(base, current)
        assert len(result) == 1
        assert result[0].model_name == "my_model"
        assert result[0].status == "modified"
        assert result[0].base_path is not None
        assert result[0].current_path is not None

    def test_added_model(self, tmp_path):
        """File in current only → status='added'."""
        base = tmp_path / "base"
        base.mkdir()

        current = tmp_path / "current"
        current.mkdir()
        (current / "new_model.sql").write_text("SELECT id FROM t")

        result = diff_compiled_dirs(base, current)
        assert len(result) == 1
        assert result[0].model_name == "new_model"
        assert result[0].status == "added"
        assert result[0].base_path is None
        assert result[0].current_path is not None

    def test_removed_model(self, tmp_path):
        """File in base only → status='removed'."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "old_model.sql").write_text("SELECT id FROM t")

        current = tmp_path / "current"
        current.mkdir()

        result = diff_compiled_dirs(base, current)
        assert len(result) == 1
        assert result[0].model_name == "old_model"
        assert result[0].status == "removed"
        assert result[0].base_path is not None
        assert result[0].current_path is None

    def test_unchanged_model_excluded(self, tmp_path):
        """Same filename, same content → not in result."""
        base = tmp_path / "base"
        base.mkdir()
        (base / "stable.sql").write_text("SELECT id FROM t")

        current = tmp_path / "current"
        current.mkdir()
        (current / "stable.sql").write_text("SELECT id FROM t")

        result = diff_compiled_dirs(base, current)
        assert result == []
