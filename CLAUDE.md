# dbt-plan

## Overview

dbt compiled SQL을 파싱하여 DDL 영향을 예측하는 CLI 도구.
`terraform plan`의 dbt 버전. 모든 warehouse 지원 (Snowflake, BigQuery, Redshift, Postgres 등).

## Architecture

```
src/dbt_plan/
├── columns.py      # SQLGlot 기반 컬럼 추출 (multi-dialect)
├── config.py       # .dbt-plan.yml + env var 설정
├── predictor.py    # DDL 예측 규칙 (materialization × on_schema_change)
├── manifest.py     # manifest.json 파싱, node index, downstream BFS
├── diff.py         # compiled SQL 디렉토리 비교 (캐싱)
├── formatter.py    # text (color) / github / json 출력
└── cli.py          # CLI: snapshot, check, init, stats
```

## Development

```bash
uv sync --extra test
make test
```

## Rules

- sqlglot 외 런타임 의존성 추가 금지
- 파싱 실패 시 safe 반환 절대 금지 — None 반환 → 호출자가 review로 처리
- 테스트 없는 기능 추가 금지 (TDD)
- SELECT \* → ["*"] 반환 (manifest column fallback 지원)
- Multi-dialect 지원 via --dialect (기본값: snowflake)

## DDL Prediction Rules

| materialization | on_schema_change | DDL |
|---|---|---|
| table | * | CREATE OR REPLACE TABLE (safe) |
| view | * | CREATE OR REPLACE VIEW (safe) |
| incremental | ignore | no DDL (safe) |
| incremental | fail | build failure (warning) |
| incremental | append_new_columns | ADD COLUMN only (safe) |
| incremental | sync_all_columns | ADD + DROP COLUMN (destructive if removed) |

## Testing

```bash
make test                    # all tests
make test-cov                # with coverage
make lint                    # ruff check
pytest tests/test_columns.py # specific module
```

Test fixtures in `tests/fixtures/` contain real-world compiled SQL patterns.
