"""Tests for config loading."""

from dbt_plan.config import Config


class TestConfigDefaults:
    def test_defaults(self):
        """No config file → sensible defaults."""
        config = Config.load("/nonexistent")
        assert config.dbt_cmd == "dbt"
        assert config.target_dir == "target"
        assert config.base_dir == ".dbt-plan/base"
        assert config.profiles_dir is None


class TestConfigFromFile:
    def test_load_from_file(self, tmp_path):
        """Reads .dbt-plan.yml."""
        (tmp_path / ".dbt-plan.yml").write_text(
            "dbt_cmd: uv run dbt\n"
            "target_dir: build\n"
            "profiles_dir: ./profiles\n"
        )
        config = Config.load(tmp_path)
        assert config.dbt_cmd == "uv run dbt"
        assert config.target_dir == "build"
        assert config.profiles_dir == "./profiles"

    def test_partial_config(self, tmp_path):
        """Missing keys use defaults."""
        (tmp_path / ".dbt-plan.yml").write_text("dbt_cmd: poetry run dbt\n")
        config = Config.load(tmp_path)
        assert config.dbt_cmd == "poetry run dbt"
        assert config.target_dir == "target"
        assert config.base_dir == ".dbt-plan/base"

    def test_empty_file(self, tmp_path):
        """Empty config file → defaults."""
        (tmp_path / ".dbt-plan.yml").write_text("")
        config = Config.load(tmp_path)
        assert config.dbt_cmd == "dbt"

    def test_comments_ignored(self, tmp_path):
        """Comments and blank lines are ignored."""
        (tmp_path / ".dbt-plan.yml").write_text(
            "# dbt execution config\n"
            "\n"
            "dbt_cmd: uv run dbt\n"
            "# target_dir: custom\n"
        )
        config = Config.load(tmp_path)
        assert config.dbt_cmd == "uv run dbt"
        assert config.target_dir == "target"
