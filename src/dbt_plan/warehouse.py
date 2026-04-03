"""Warehouse column query via INFORMATION_SCHEMA (Snowflake)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WarehouseConfig:
    """Snowflake connection parameters for INFORMATION_SCHEMA queries."""

    account: str
    user: str
    database: str
    schema: str = "DBT"
    private_key_path: str | None = None
    password: str | None = None
    role: str | None = None
    warehouse: str | None = None


def build_columns_query(
    database: str,
    schema: str,
    model_names: list[str],
) -> str:
    """Build INFORMATION_SCHEMA.COLUMNS query for given models.

    Model names are uppercased (Snowflake default identifier handling).
    """
    if not model_names:
        return ""

    quoted_names = ", ".join(f"'{name.upper()}'" for name in model_names)
    return (
        f"SELECT TABLE_NAME, COLUMN_NAME\n"
        f"FROM {database}.INFORMATION_SCHEMA.COLUMNS\n"
        f"WHERE TABLE_SCHEMA = '{schema.upper()}'\n"
        f"  AND TABLE_NAME IN ({quoted_names})\n"
        f"ORDER BY TABLE_NAME, ORDINAL_POSITION"
    )


def parse_columns_result(
    rows: list[tuple[str, str]],
) -> dict[str, list[str]]:
    """Parse INFORMATION_SCHEMA query result into model -> columns mapping.

    Args:
        rows: List of (TABLE_NAME, COLUMN_NAME) tuples from Snowflake.

    Returns:
        Dict mapping lowercased model name to lowercased column names.
    """
    result: dict[str, list[str]] = {}
    for table_name, column_name in rows:
        key = table_name.lower()
        if key not in result:
            result[key] = []
        result[key].append(column_name.lower())
    return result


def query_warehouse_columns(
    config: WarehouseConfig,
    model_names: list[str],
) -> dict[str, list[str]]:
    """Query Snowflake INFORMATION_SCHEMA for actual table columns.

    Requires snowflake-connector-python to be installed.

    Args:
        config: Snowflake connection parameters.
        model_names: Model names to query columns for.

    Returns:
        Dict mapping model name to list of column names (lowercased).

    Raises:
        ImportError: If snowflake-connector-python is not installed.
        Exception: On Snowflake connection or query errors.
    """
    if not model_names:
        return {}

    import snowflake.connector  # type: ignore[import-untyped]

    connect_params: dict = {
        "account": config.account,
        "user": config.user,
        "database": config.database,
    }
    if config.password:
        connect_params["password"] = config.password
    if config.role:
        connect_params["role"] = config.role
    if config.warehouse:
        connect_params["warehouse"] = config.warehouse
    if config.private_key_path:
        from pathlib import Path
        import cryptography.hazmat.primitives.serialization as serialization

        key_data = Path(config.private_key_path).read_bytes()
        private_key = serialization.load_pem_private_key(key_data, password=None)
        connect_params["private_key"] = private_key

    query = build_columns_query(config.database, config.schema, model_names)

    with snowflake.connector.connect(**connect_params) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

    return parse_columns_result(rows)
