"""SQLGlot-based column extraction from compiled SQL."""

import sqlglot
from sqlglot import exp


def extract_columns(sql: str) -> list[str] | None:
    """Extract column names from compiled SQL's final SELECT.

    Parses with Snowflake dialect. Returns lowercased column names
    using alias if available, otherwise output_name.

    Returns:
        list[str]: Column names (lowercased).
        ["*"]: If final SELECT uses SELECT *.
        None: If parsing fails or no SELECT found.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="snowflake")
    except sqlglot.errors.ParseError:
        return None

    select = tree.find(exp.Select)
    if select is None:
        return None

    columns = []
    for expr in select.expressions:
        if isinstance(expr, exp.Star):
            return ["*"]
        name = expr.alias or expr.output_name
        if name:
            columns.append(name.lower())

    return columns if columns else None
