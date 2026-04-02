# dbt-plan

Preview what DDL changes `dbt run` will execute on Snowflake — before you run it.

Like `terraform plan` for dbt.

## Quick Start

```bash
pip install git+https://github.com/ab180/dbt-plan

# In your dbt project directory:
dbt compile
dbt-plan snapshot          # Save baseline
# ... make model changes ...
dbt compile
dbt-plan check             # See what will change
```

## CI Integration (GitHub Actions)

```yaml
- name: Install dbt-plan
  run: pip install git+https://github.com/ab180/dbt-plan@v0.1.0

- name: Run DDL check
  run: dbt-plan check --format github >> $GITHUB_STEP_SUMMARY
```

## Output Example

```
$ dbt-plan check

dbt-plan — 2 models changed

🔴 DESTRUCTIVE  int_unified (incremental, sync_all_columns)
  DROP COLUMN  data__device
  DROP COLUMN  data__user
  ADD COLUMN   data__device__uuid
  Downstream: dim_device, fct_events, vw_client_events (8 models)

✅ SAFE  dim_device (table)
  CREATE OR REPLACE TABLE (full rebuild)
```

## How It Works

1. Parses compiled SQL with [SQLGlot](https://github.com/tobymao/sqlglot) (Snowflake dialect)
2. Extracts SELECT column lists from base and current compiled SQL
3. Reads manifest config (`materialized`, `on_schema_change`)
4. Predicts DDL: `CREATE OR REPLACE`, `ALTER TABLE ADD/DROP COLUMN`, etc.
5. Reports impact with downstream model list

## Supported

- dbt-core 1.7+ with dbt-snowflake
- Python 3.10+
- Snowflake VARIANT (`col:path::TYPE`), CTE, QUALIFY, window functions

## License

Apache-2.0
