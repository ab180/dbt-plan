# Contributing to dbt-plan

## Development Setup

```bash
git clone https://github.com/ab180/dbt-plan
cd dbt-plan
pip install -e ".[test]"
pytest -v
```

## Adding Features

1. Write failing test first (TDD)
2. Implement the minimal code to pass
3. Run `pytest -v` to verify
4. Commit with conventional commits (`feat:`, `fix:`, `docs:`, `test:`)

## Architecture

See `CLAUDE.md` for module responsibilities and design rules.

## Key Rules

- No runtime dependencies beyond sqlglot
- Parse failure must never return "safe"
- Every feature needs tests
- Snowflake dialect only (for now)
