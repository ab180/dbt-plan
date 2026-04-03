# dbt-plan

Preview what DDL changes `dbt run` will execute on Snowflake — before you run it.

Like `terraform plan` for dbt.

## Why

An `ALTER TABLE DROP COLUMN` via `sync_all_columns` cascaded to destroy 10+ downstream models in production. There was no way to detect this before merge. dbt-plan solves this by predicting DDL impact at PR time.

## Quick Start

```bash
pip install git+https://github.com/ab180/dbt-plan@v0.1.0

# In your dbt project directory:
dbt compile
dbt-plan snapshot          # Save baseline (compiled SQL + manifest)
# ... make model changes ...
dbt compile
dbt-plan check             # See what will change
dbt-plan check --format github   # GitHub markdown output
```

## Output Example

```
$ dbt-plan check

dbt-plan -- 2 model(s) changed

DESTRUCTIVE  int_unified (incremental, sync_all_columns)
  DROP COLUMN  data__device
  DROP COLUMN  data__user
  ADD COLUMN   data__device__uuid
  Downstream: dim_device, fct_events (2 model(s))

SAFE  dim_device (table)
  CREATE OR REPLACE TABLE
```

## What Works (v0.1.0)

| Feature | Status | Details |
|---------|--------|---------|
| Column extraction (SQLGlot) | **Done** | Snowflake dialect, CTE, VARIANT, QUALIFY, UNION ALL |
| DDL prediction | **Done** | All materialization x on_schema_change combinations |
| Downstream impact | **Done** | BFS via manifest child_map, cycle protection |
| Removed model detection | **Done** | Always DESTRUCTIVE, base manifest fallback |
| Parse failure safety | **Done** | Never returns SAFE when columns unknown |
| SELECT * detection | **Done** | Returns WARNING (cannot diff columns) |
| Text + GitHub output | **Done** | `--format text` (default) / `--format github` |
| Snapshot (baseline) | **Done** | Saves compiled SQL + manifest.json |
| Exit codes | **Done** | 0=safe, 1=destructive, 2=warning/error |
| CI (package) | **Done** | pytest on Python 3.10-3.13, release on tag |

## What Doesn't Work Yet

| Feature | Phase | Why It Matters |
|---------|-------|----------------|
| INFORMATION_SCHEMA query | 1b | 49/228 models use `SELECT *` — can't extract columns without querying warehouse |
| CI workflow template | 1c | Reusable GitHub Actions workflow for dbt projects |
| PR comment posting | 1c | Auto-post DDL impact as PR comment with `<!-- dbt-plan -->` marker |
| `ddl-reviewed` label override | 1d | Escape hatch for intentional destructive changes |
| Slack notifications | 2a | Alert on destructive DDL |
| `dbt-plan run` wrapper | 2b | Interactive confirmation before `dbt run` |
| Column type detection | 2c | `ALTER TYPE` predictions (only 5.2% of columns have type info) |

## DDL Prediction Rules

| Materialization | on_schema_change | Predicted DDL | Safety |
|-----------------|------------------|---------------|--------|
| table | any | `CREATE OR REPLACE TABLE` | SAFE |
| view | any | `CREATE OR REPLACE VIEW` | SAFE |
| incremental | ignore | no DDL | SAFE |
| incremental | fail | build failure | WARNING |
| incremental | append_new_columns | `ADD COLUMN` only | SAFE |
| incremental | sync_all_columns | `ADD + DROP COLUMN` | DESTRUCTIVE if columns removed |
| any | (model removed) | `MODEL REMOVED` | DESTRUCTIVE |

## CI Integration (GitHub Actions)

```yaml
name: dbt-plan
on:
  pull_request:
    paths: ['models/**', 'macros/**']

jobs:
  plan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }

      - run: pip install uv && uv sync
      - run: pip install git+https://github.com/ab180/dbt-plan@v0.1.0

      # Compile and snapshot base branch
      - run: |
          git checkout ${{ github.event.pull_request.base.sha }}
          dbt compile
          dbt-plan snapshot

      # Compile current and check
      - run: |
          git checkout ${{ github.event.pull_request.head.sha }}
          dbt compile
          dbt-plan check --format github >> $GITHUB_STEP_SUMMARY

      # Block destructive changes (exit 1)
      - run: dbt-plan check
```

## How It Works

```
1. dbt-plan snapshot
   Save compiled SQL + manifest.json as baseline

2. dbt-plan check
   compiled SQL diff → column extraction (SQLGlot) → DDL prediction → downstream impact

   base compiled/*.sql  vs  current compiled/*.sql
        ↓                        ↓
   extract_columns()        extract_columns()
        ↓                        ↓
   base columns    ──diff──  current columns
                      ↓
              predict_ddl() + manifest config
                      ↓
              DDL prediction + downstream
                      ↓
              format_text() / format_github()
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, TDD workflow, and coding rules.

### Architecture

```
src/dbt_plan/
├── columns.py      # SQLGlot column extraction
├── predictor.py    # DDL prediction rules
├── manifest.py     # manifest.json parsing + downstream BFS
├── diff.py         # compiled SQL directory comparison
├── formatter.py    # text / GitHub markdown output
└── cli.py          # CLI entry point (snapshot, check)
```

### How to Contribute

**Good first issues:**
- Add more compiled SQL fixtures in `tests/fixtures/` for edge cases
- Improve error messages for common mistakes
- Add `--verbose` flag for debugging

**Medium issues:**
- INFORMATION_SCHEMA integration (Phase 1b) — query warehouse for actual columns
- CI workflow template — reusable GitHub Actions for dbt projects
- PR comment posting with `<!-- dbt-plan -->` marker

**Design decisions:** See [docs/architecture-decisions.md](docs/architecture-decisions.md) and [docs/design-spec.md](docs/design-spec.md).

## Supported

- dbt-core 1.7+ with dbt-snowflake
- Python 3.10+
- Snowflake VARIANT (`col:path::TYPE`), CTE, QUALIFY, window functions

## License

Apache-2.0
