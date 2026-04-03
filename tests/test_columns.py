"""Tests for extract_columns — SQLGlot-based column extraction."""

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
