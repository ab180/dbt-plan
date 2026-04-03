"""Tests for warehouse module (INFORMATION_SCHEMA query building and parsing)."""

from dbt_plan.warehouse import WarehouseConfig, build_columns_query, parse_columns_result


class TestBuildColumnsQuery:
    def test_single_model(self):
        """Single model → valid SQL with uppercased name."""
        query = build_columns_query("MY_DB", "DBT", ["int_unified"])
        assert "MY_DB.INFORMATION_SCHEMA.COLUMNS" in query
        assert "'INT_UNIFIED'" in query
        assert "TABLE_SCHEMA = 'DBT'" in query

    def test_multiple_models(self):
        """Multiple models → IN clause with all names."""
        query = build_columns_query("DB", "PUBLIC", ["model_a", "model_b"])
        assert "'MODEL_A'" in query
        assert "'MODEL_B'" in query
        assert "IN (" in query

    def test_empty_models(self):
        """Empty model list → empty string."""
        assert build_columns_query("DB", "S", []) == ""

    def test_schema_uppercased(self):
        """Schema name is uppercased."""
        query = build_columns_query("DB", "my_schema", ["m"])
        assert "TABLE_SCHEMA = 'MY_SCHEMA'" in query


class TestParseColumnsResult:
    def test_basic_parsing(self):
        """Rows are grouped by table, lowercased."""
        rows = [
            ("INT_UNIFIED", "EVENT_ID"),
            ("INT_UNIFIED", "APP_ID"),
            ("DIM_DEVICE", "DEVICE_ID"),
        ]
        result = parse_columns_result(rows)
        assert result == {
            "int_unified": ["event_id", "app_id"],
            "dim_device": ["device_id"],
        }

    def test_empty_rows(self):
        """Empty rows → empty dict."""
        assert parse_columns_result([]) == {}

    def test_preserves_order(self):
        """Column order matches input order."""
        rows = [
            ("M", "C"),
            ("M", "A"),
            ("M", "B"),
        ]
        result = parse_columns_result(rows)
        assert result["m"] == ["c", "a", "b"]


class TestWarehouseConfig:
    def test_defaults(self):
        """Default schema is DBT."""
        config = WarehouseConfig(account="a", user="u", database="d")
        assert config.schema == "DBT"
        assert config.private_key_path is None
        assert config.password is None
