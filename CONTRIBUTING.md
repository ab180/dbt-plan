# Contributing to dbt-plan

## Development Setup

```bash
git clone https://github.com/ab180/dbt-plan
cd dbt-plan
pip install -e ".[test]"
pytest -v                  # 42 tests should pass
dbt-plan --version         # dbt-plan 0.1.0
```

## Adding Features

1. Write failing test first (TDD)
2. Implement the minimal code to pass
3. Run `pytest -v` to verify
4. Commit with conventional commits (`feat:`, `fix:`, `docs:`, `test:`)

## Architecture

```
src/dbt_plan/
├── columns.py      # SQLGlot column extraction from compiled SQL
├── predictor.py    # DDL prediction rules (materialization x on_schema_change)
├── manifest.py     # manifest.json parsing + downstream BFS discovery
├── diff.py         # compiled SQL directory comparison
├── formatter.py    # text / GitHub markdown output formatting
└── cli.py          # CLI entry point (snapshot, check subcommands)
```

Data flow: `diff_compiled_dirs` -> `extract_columns` -> `predict_ddl` -> `find_downstream` -> `format_text/format_github`

## Key Rules

- No runtime dependencies beyond sqlglot
- Parse failure must never return "safe" (return None, caller handles as WARNING)
- SELECT * must return `["*"]` (not an empty list, not None)
- Every feature needs tests before implementation
- Snowflake dialect only (for now)

## Good First Issues

- Add compiled SQL fixtures in `tests/fixtures/` for more edge cases
- Improve error messages for common mistakes (e.g., running check without snapshot)
- Add `--verbose` flag for debugging column extraction

## Testing

```bash
pytest -v                        # all tests
pytest tests/test_columns.py     # specific module
pytest -k "test_sync"            # by name pattern
```

Test fixtures in `tests/fixtures/` contain real-world compiled SQL patterns from production dbt projects.

## Design Decisions

See [docs/architecture-decisions.md](docs/architecture-decisions.md) for the rationale behind key decisions:
- Why SQLGlot (ADR-2)
- Why INFORMATION_SCHEMA is needed (ADR-3)
- Why PR merge is the only interception point (ADR-4)
- DDL prediction rules from dbt-snowflake source (ADR-5)
