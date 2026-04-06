"""SQLGlot-based column extraction from compiled SQL."""

import sqlglot
from sqlglot import exp


def extract_columns(sql: str, *, dialect: str = "snowflake") -> list[str] | None:
    """Extract column names from compiled SQL's final SELECT.

    Parses with the given SQL dialect. Returns lowercased column names
    using alias if available, otherwise output_name.

    Args:
        sql: Compiled SQL string.
        dialect: sqlglot dialect name (default: "snowflake").

    Returns:
        list[str]: Column names (lowercased).
        ["*"]: If final SELECT uses SELECT *.
        None: If parsing fails or no SELECT found.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.ParseError:
        return None

    select = tree.find(exp.Select)
    if select is None:
        return None

    columns = []
    expr_count = 0
    for expr in select.expressions:
        expr_count += 1
        if isinstance(expr, exp.Star):
            return ["*"]
        name = expr.alias or expr.output_name
        # Qualified star (e.g. t1.*) produces a Column with name='*'
        if name == "*":
            return ["*"]
        if name:
            columns.append(name.lower())

    # If some expressions had no extractable name (e.g. CASE without AS),
    # we have ambiguity — return None so the caller treats as REVIEW REQUIRED
    if columns and len(columns) < expr_count:
        return None

    return columns if columns else None
