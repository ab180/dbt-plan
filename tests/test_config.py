"""Tests for configuration loading from .dbt-plan.yml and env vars."""


from dbt_plan.config import Config


class TestConfigDefaults:
    def test_default_values(self, tmp_path):
        """Config without file or env vars returns defaults."""
        config = Config.load(tmp_path)
        assert config.ignore_models == []
        assert config.warning_exit_code == 2
        assert config.format == "text"
        assert config.no_color is False
        assert config.verbose is False
        assert config.dialect == "snowflake"


class TestConfigFile:
    def test_load_ignore_models(self, tmp_path):
        """Parses ignore_models list from config file."""
        (tmp_path / ".dbt-plan.yml").write_text(
            "ignore_models: [test_model, staging_temp]\n"
        )
        config = Config.load(tmp_path)
        assert config.ignore_models == ["test_model", "staging_temp"]

    def test_load_warning_exit_code(self, tmp_path):
        """Parses warning_exit_code as integer."""
        (tmp_path / ".dbt-plan.yml").write_text("warning_exit_code: 0\n")
        config = Config.load(tmp_path)
        assert config.warning_exit_code == 0

    def test_load_format(self, tmp_path):
        """Parses format option."""
        (tmp_path / ".dbt-plan.yml").write_text("format: github\n")
        config = Config.load(tmp_path)
        assert config.format == "github"

    def test_load_dialect(self, tmp_path):
        """Parses dialect option."""
        (tmp_path / ".dbt-plan.yml").write_text("dialect: bigquery\n")
        config = Config.load(tmp_path)
        assert config.dialect == "bigquery"

    def test_ignores_comments_and_blanks(self, tmp_path):
        """Comments and blank lines are skipped."""
        (tmp_path / ".dbt-plan.yml").write_text(
            "# This is a comment\n"
            "\n"
            "warning_exit_code: 0\n"
            "# Another comment\n"
        )
        config = Config.load(tmp_path)
        assert config.warning_exit_code == 0

    def test_invalid_format_ignored(self, tmp_path):
        """Invalid format value is ignored, keeps default."""
        (tmp_path / ".dbt-plan.yml").write_text("format: xml\n")
        config = Config.load(tmp_path)
        assert config.format == "text"

    def test_malicious_dialect_rejected(self, tmp_path):
        """Non-alphanumeric dialect values are rejected."""
        (tmp_path / ".dbt-plan.yml").write_text("dialect: ; rm -rf /\n")
        config = Config.load(tmp_path)
        assert config.dialect == "snowflake"  # stays default

    def test_missing_file_uses_defaults(self, tmp_path):
        """No config file → all defaults."""
        config = Config.load(tmp_path)
        assert config.dialect == "snowflake"


class TestEnvVars:
    def test_env_overrides_format(self, tmp_path, monkeypatch):
        """DBT_PLAN_FORMAT env var overrides config file."""
        (tmp_path / ".dbt-plan.yml").write_text("format: text\n")
        monkeypatch.setenv("DBT_PLAN_FORMAT", "json")
        config = Config.load(tmp_path)
        assert config.format == "json"

    def test_env_no_color(self, tmp_path, monkeypatch):
        """DBT_PLAN_NO_COLOR=true enables no_color."""
        monkeypatch.setenv("DBT_PLAN_NO_COLOR", "true")
        config = Config.load(tmp_path)
        assert config.no_color is True

    def test_env_verbose(self, tmp_path, monkeypatch):
        """DBT_PLAN_VERBOSE=1 enables verbose."""
        monkeypatch.setenv("DBT_PLAN_VERBOSE", "1")
        config = Config.load(tmp_path)
        assert config.verbose is True

    def test_env_dialect(self, tmp_path, monkeypatch):
        """DBT_PLAN_DIALECT overrides config."""
        (tmp_path / ".dbt-plan.yml").write_text("dialect: snowflake\n")
        monkeypatch.setenv("DBT_PLAN_DIALECT", "postgres")
        config = Config.load(tmp_path)
        assert config.dialect == "postgres"

    def test_env_ignore_models(self, tmp_path, monkeypatch):
        """DBT_PLAN_IGNORE_MODELS as comma-separated list."""
        monkeypatch.setenv("DBT_PLAN_IGNORE_MODELS", "model_a, model_b")
        config = Config.load(tmp_path)
        assert config.ignore_models == ["model_a", "model_b"]

    def test_env_warning_exit_code(self, tmp_path, monkeypatch):
        """DBT_PLAN_WARNING_EXIT_CODE overrides config."""
        monkeypatch.setenv("DBT_PLAN_WARNING_EXIT_CODE", "0")
        config = Config.load(tmp_path)
        assert config.warning_exit_code == 0
