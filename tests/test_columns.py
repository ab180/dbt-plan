"""Tests for extract_columns — SQLGlot-based column extraction."""

from pathlib import Path

from dbt_plan.columns import extract_columns


class TestExplicitColumns:
    def test_cte_with_explicit_final_select(self, explicit_columns_sql):
        """CTE chain → explicit final SELECT extracts all column names."""
        result = extract_columns(explicit_columns_sql)
        assert result == [
            "app_id",
            "event_date",
            "device_id",
            "total_revenue",
            "event_count",
            "is_active",
            "day_n",
        ]

    def test_simple_select(self):
        """Plain SELECT without CTE extracts column names."""
        sql = "SELECT id, name, email FROM users"
        result = extract_columns(sql)
        assert result == ["id", "name", "email"]

    def test_cte_with_union_all(self):
        """CTE wrapping UNION ALL → extracts from final outer SELECT."""
        sql = """
        WITH combined AS (
            SELECT a, b FROM t1
            UNION ALL
            SELECT a, b FROM t2
        )
        SELECT a, b, 'combined' AS source FROM combined
        """
        result = extract_columns(sql)
        assert result == ["a", "b", "source"]


class TestSelectStar:
    def test_select_star_returns_star_list(self, select_star_sql):
        """SELECT * → returns ["*"]."""
        result = extract_columns(select_star_sql)
        assert result == ["*"]


class TestVariantAccess:
    def test_variant_with_qualify(self, variant_access_sql):
        """VARIANT col:path::TYPE + QUALIFY → extracts aliased names."""
        result = extract_columns(variant_access_sql)
        assert result == [
            "write_date",
            "data__device__airbridgegenerateddeviceuuid",
            "data__device__deviceuuid",
            "data__device__country",
            "app_id",
            "event_date",
        ]


class TestQualifiedStar:
    def test_qualified_star_returns_star_list(self):
        """SELECT t.* → returns ["*"] (same as unqualified star)."""
        sql = "SELECT t.* FROM my_table t"
        result = extract_columns(sql)
        assert result == ["*"]

    def test_qualified_star_mixed_with_columns(self):
        """SELECT t.*, extra_col → returns ["*"] (cannot resolve)."""
        sql = "SELECT t.*, extra_col FROM my_table t JOIN other o ON t.id = o.id"
        result = extract_columns(sql)
        assert result == ["*"]


class TestUnaliasedExpressions:
    def test_unaliased_case_returns_none(self):
        """CASE without alias → None (ambiguity, cannot determine column name)."""
        sql = "SELECT id, CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END FROM users"
        result = extract_columns(sql)
        assert result is None

    def test_all_aliased_returns_columns(self):
        """All expressions aliased → returns column list normally."""
        sql = "SELECT id, CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END AS status_label FROM users"
        result = extract_columns(sql)
        assert result == ["id", "status_label"]


class TestDialects:
    def test_bigquery_dialect(self):
        """BigQuery dialect parses ARRAY_AGG correctly."""
        sql = "SELECT user_id, ARRAY_AGG(item ORDER BY ts) AS items FROM events GROUP BY 1"
        result = extract_columns(sql, dialect="bigquery")
        assert result == ["user_id", "items"]

    def test_postgres_dialect(self):
        """Postgres dialect parses JSONB operator correctly."""
        sql = """SELECT id, data->>'email' AS email FROM users"""
        result = extract_columns(sql, dialect="postgres")
        assert result == ["id", "email"]

    def test_default_dialect_is_snowflake(self):
        """Default dialect is snowflake (backward compat)."""
        sql = "SELECT id FROM t"
        assert extract_columns(sql) == extract_columns(sql, dialect="snowflake")


class TestEdgeCases:
    def test_parse_failure_returns_none(self):
        """Invalid SQL → returns None (never raises, never returns 'safe')."""
        result = extract_columns("NOT VALID SQL !!!")
        assert result is None

    def test_lateral_flatten_no_crash(self):
        """LATERAL FLATTEN → returns None or list, never crashes."""
        sql = """
        SELECT
            f.value::STRING AS tag
        FROM events,
            LATERAL FLATTEN(input => events.tags) AS f
        """
        result = extract_columns(sql)
        assert result is None or isinstance(result, list)


class TestColumnExtractionEdgeCases:
    def test_non_select_sql_returns_none(self):
        """CREATE TABLE or INSERT → None (no SELECT to extract from)."""
        assert extract_columns("CREATE TABLE foo (id INT)") is None

    def test_empty_sql_returns_none(self):
        """Empty string → None."""
        assert extract_columns("") is None

    def test_comment_only_returns_none(self):
        """SQL with only comments → None."""
        assert extract_columns("-- just a comment") is None

    def test_bare_union_all_returns_first_branch(self):
        """UNION ALL returns first branch's columns (SQL standard)."""
        sql = "SELECT a, b FROM t1 UNION ALL SELECT x, y FROM t2"
        result = extract_columns(sql)
        assert result == ["a", "b"]


class TestRealWorldFixtures:
    """Tests using real-world dbt SQL patterns from fixtures."""

    FIXTURES = Path(__file__).parent / "fixtures"

    def test_window_functions(self):
        """Window functions with QUALIFY — common in incremental models."""
        sql = (self.FIXTURES / "window_functions.sql").read_text()
        result = extract_columns(sql)
        assert result is not None
        assert "user_id" in result
        assert "event_date" in result
        assert "row_num" in result
        assert "prev_event_date" in result
        assert "lifetime_revenue" in result

    def test_cte_chain_with_select_star(self):
        """Multi-CTE chain ending in SELECT * FROM final — returns ["*"]."""
        sql = (self.FIXTURES / "cte_chain.sql").read_text()
        result = extract_columns(sql)
        # SELECT * FROM final → ["*"]
        assert result == ["*"]

    def test_union_staging_pattern(self):
        """UNION ALL multi-source pattern — returns first branch columns."""
        sql = (self.FIXTURES / "union_staging.sql").read_text()
        result = extract_columns(sql)
        assert result is not None
        assert "platform" in result
        assert "device_id" in result
        assert "event_name" in result
        assert "event_timestamp" in result

    def test_explicit_columns_fixture(self):
        """Existing fixture: explicit column SELECT."""
        sql = (self.FIXTURES / "explicit_columns.sql").read_text()
        result = extract_columns(sql)
        assert result is not None
        assert len(result) > 0

    def test_variant_access_fixture(self):
        """Existing fixture: Snowflake VARIANT access."""
        sql = (self.FIXTURES / "variant_access.sql").read_text()
        result = extract_columns(sql)
        # VARIANT access may or may not parse depending on dialect
        assert result is None or isinstance(result, list)
