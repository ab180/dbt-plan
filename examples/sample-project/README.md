# Sample Project

dbt-plan을 체험해볼 수 있는 예제 프로젝트입니다. Snowflake 접속 없이 로컬에서 바로 실행됩니다.

## 시나리오

| 모델 | 변경 | 예상 결과 |
|------|------|-----------|
| `int_unified` | `data__device`, `data__user` 삭제, `data__device__uuid`, `data__user__id` 추가 | **DESTRUCTIVE** (sync_all_columns + DROP COLUMN) |
| `fct_daily_metrics` | `total_revenue` 컬럼 추가, `data__device` 참조 유지 | **SAFE** (append_new_columns) + **BROKEN_REF** cascade |
| `dim_device` | `platform` 컬럼 추가 | **SAFE** (table = CREATE OR REPLACE) |
| `dim_campaign` | 새 모델 | **SAFE** (신규 테이블) |

## 실행

```bash
pip install dbt-plan  # or: pip install git+https://github.com/ab180/dbt-plan@v0.2.0
cd examples/sample-project
bash run-example.sh
```

## 기대 출력

```text
dbt-plan -- 4 model(s) changed

DESTRUCTIVE  int_unified (incremental, sync_all_columns)
  ADD COLUMN  data__device__uuid
  ADD COLUMN  data__user__id
  DROP COLUMN  data__device
  DROP COLUMN  data__user
  Downstream: dim_device, fct_daily_metrics (2 model(s))
  >> BROKEN_REF  fct_daily_metrics: references dropped column(s): data__device

SAFE  dim_campaign (table)
  CREATE OR REPLACE TABLE

SAFE  dim_device (table)
  CREATE OR REPLACE TABLE

SAFE  fct_daily_metrics (incremental, append_new_columns)
  ADD COLUMN  total_revenue

dbt-plan: 4 checked, 3 safe, 0 warning, 1 destructive, 1 cascade risk(s)
```

Exit code: **1** (destructive — int_unified에 DROP COLUMN + fct_daily_metrics cascade broken ref)

## 구조

```text
base/                              # snapshot (변경 전)
├── compiled/*.sql
└── manifest.json

current/                           # 현재 상태 (변경 후)
└── target/
    ├── compiled/sample/models/*.sql
    └── manifest.json
```
