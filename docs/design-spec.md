# dbt DDL Impact Predictor — Design Spec

## 한 줄 요약

PR merge 전에 "이 변경으로 Snowflake에서 어떤 DDL이 실행되는가"를 자동 예측하고, 파괴적 DDL(DROP COLUMN) 시 merge를 차단한다.

---

## Problem

2026년 PII 대응 작업에서 `int_unified`의 VARIANT 컬럼 제거 → `sync_all_columns` 설정으로 `ALTER TABLE DROP COLUMN` 실행 → downstream 10+ 모델 연쇄 파괴. **merge 전에 이를 감지할 수단이 없었다.**

### 왜 이 문제가 반복 가능한가

- dbt incremental + `sync_all_columns`는 SELECT 컬럼이 바뀌면 **자동으로 ALTER TABLE**을 실행한다
- 이 프로젝트에 sync_all_columns 모델이 **14개** 있다
- 프로덕션 dbt run은 Snowflake Tasks(17개)가 자동 실행 — merge하면 바로 프로덕션에 반영
- 현재 방어선: sqlfluff 린트(영향 분석 없음) + dbt SQL test(사후 검증)

### 왜 PR merge 시점이 유일한 interception point인가

| 대안 | 왜 안 되는가 |
|------|-------------|
| dbt run 래핑 (shell wrapper) | Snowflake Tasks가 `EXECUTE dbt project`로 실행 — shell 접근 불가 |
| on-run-start hook | SQL만 실행 가능 (Python/local file 접근 불가) |
| dbt --empty | 실제 DDL 실행됨 (dry-run 아님) |
| Recce CI | PR 코멘트 없음(OSS), catalog.json 의존(같은 warehouse), 29 packages |

---

## Solution

### 핵심 로직 (3단계)

```
1. COMPILED SQL DIFF
   base compiled/*.sql vs current compiled/*.sql
   → 변경/추가/삭제된 모델 목록

2. COLUMN DIFF
   sync_all_columns 모델만:
   - SQLGlot: PR의 compiled SQL 파싱 → SELECT 컬럼 추출
   - INFORMATION_SCHEMA: warehouse 실제 테이블 컬럼 조회
   - diff: warehouse 컬럼 vs PR 컬럼 = 실제로 ADD/DROP 될 컬럼

3. DDL PREDICTION
   materialization + on_schema_change + column diff → 결정론적 DDL 예측
```

### DDL 예측 규칙 (dbt-snowflake 소스코드 확인)

| materialization | on_schema_change | DDL | 위험도 |
|---|---|---|---|
| table | 무관 | `CREATE OR REPLACE TABLE AS SELECT` | ✅ safe (전체 재생성) |
| view | 무관 | `CREATE OR REPLACE VIEW AS SELECT` | ✅ safe |
| incremental | ignore | DDL 없음 | ✅ safe (스키마 불일치 무시) |
| incremental | fail | DDL 없음, 빌드 실패 | ⚠️ review |
| incremental | append_new_columns | `ALTER TABLE ADD COLUMN` only | ✅ safe |
| incremental | sync_all_columns | `ALTER TABLE ADD COLUMN` + `DROP COLUMN` + `ALTER TYPE` | 🔴 destructive |

### 왜 INFORMATION_SCHEMA가 필요한가

compiled SQL 비교만으로는 불충분:
- compiled SQL은 "PR 브랜치의 SELECT가 어떤 컬럼을 생산하는가"만 알려줌
- 실제 DDL은 "현재 warehouse 테이블의 컬럼"과 "PR SELECT 컬럼"의 차이로 결정됨
- warehouse 상태가 base branch와 다를 수 있음 (부분 실행, manual DDL, 실패한 run)
- catalog.json은 사용 불가 (dbt docs generate가 같은 warehouse를 읽으므로 base/current 동일)

**INFORMATION_SCHEMA 접근 검증 결과:**
- 비용: ~$0.001 (메타데이터만 읽음, compute 무시 가능)
- 속도: sub-second (14개 테이블 단일 IN 쿼리)
- 이미 검증됨: `assert_no_pii_columns_in_dbt_schema.sql`에서 동일 패턴 사용 중
- 신규 모델: 빈 결과 반환 (에러 없음 → drop 위험 없음)

### 컬럼 추출 전략 (Gap 검증 완료)

**compiled/ 디렉토리 검증 결과:**
- `dbt compile` 출력 = **순수 SELECT만** (MERGE/INSERT 아님, Jinja 완전 해소)
- 99개 모델: 명시적 컬럼 리스트 → SQLGlot으로 추출 가능
- **36개 모델: `SELECT *` 사용** → 컬럼 추출 불가 → INFORMATION_SCHEMA fallback 필요

**LATERAL FLATTEN vs sync_all_columns overlap:**
- 11개 파싱 실패 모델 중 sync_all_columns는 **`fct_sdk_v1_event_validation` 1개만**
- 나머지 10개: table/view/ignore/append → 파싱 실패해도 위험 없음

**SQLGlot 검증 결과:**
- 128/128 모델 파싱 성공 (100%)
- qualify_columns: 117/128 (91.4%) — 실패 11개는 LATERAL FLATTEN
- 이미 venv에 설치됨, 0 dependency, 2.8MB
- VARIANT `col:path::TYPE`, CTE, QUALIFY, 윈도우 함수 모두 정상

### 컬럼 추출 우선순위

**핵심 발견: sync_all_columns 14개 중 6개 이상이 SELECT * 사용 → SQLGlot만으로는 가장 위험한 모델의 컬럼을 추출할 수 없음. INFORMATION_SCHEMA가 필수 경로.**

```
sync_all_columns 모델 (14개) — DDL 예측이 필요한 대상:
  → INFORMATION_SCHEMA.COLUMNS로 warehouse 실제 컬럼 조회 (PRIMARY)
  → SQLGlot으로 PR compiled SQL 컬럼 추출 (가능한 경우)
  → SELECT * 모델: PR 컬럼 추출 불가 → warehouse 컬럼 vs base compiled SQL 비교로 대체
  → 파싱 실패 + INFORMATION_SCHEMA 실패: ⚠️ review (절대 safe 아님)

기타 모델 (214개) — 정보 제공용:
  → table/view: CREATE OR REPLACE (항상 safe)
  → incremental + ignore: DDL 없음 (항상 safe)
  → incremental + append: ADD COLUMN only (safe)
  → 컬럼 추출은 best-effort (SQLGlot 성공 시 표시, 실패 시 생략)
```

### 파싱 실패 처리

**절대 "safe" 반환하지 않는다.** sync_all_columns 모델의 파싱 실패 = ⚠️ review.

| 상황 | 처리 |
|------|------|
| SQLGlot 파싱 성공 (명시적 컬럼) | 컬럼 추출 → DDL 예측 |
| SELECT * + sync_all_columns | INFORMATION_SCHEMA 조회 → DDL 예측 |
| SELECT * + table/view | ✅ safe (CREATE OR REPLACE, 위험 없음) |
| LATERAL FLATTEN 파싱 실패 + sync_all_columns | INFORMATION_SCHEMA 조회 → DDL 예측 |
| INFORMATION_SCHEMA 조회 실패 | ⚠️ review ("컬럼 확인 불가 — 수동 확인 필요") |
| compiled SQL 파일 없음 (ephemeral) | 스킵 (물리 테이블 없음) |

---

## PR 코멘트 형태

```markdown
### dbt DDL Impact Check — 3 models changed

🔴 **int_unified** (incremental, sync_all_columns)
  Warehouse columns: event_id, app_id, data__device, data__user, event_date
  PR SELECT columns: event_id, app_id, data__device__uuid, data__user__id, event_date

  Predicted DDL:
    ALTER TABLE DROP COLUMN data__device
    ALTER TABLE DROP COLUMN data__user
    ALTER TABLE ADD COLUMN data__device__uuid VARCHAR
    ALTER TABLE ADD COLUMN data__user__id VARCHAR

  Downstream impact: dim_device, fct_events, vw_client_events... (8 models)

✅ **dim_device** (table)
  Predicted DDL: CREATE OR REPLACE TABLE (full rebuild, safe)

✅ **agg_daily** (incremental, append_new_columns)
  Predicted DDL: ALTER TABLE ADD COLUMN new_metric (add only)

<details><summary>📄 int_unified — compiled SQL diff</summary>

(unified diff here)

</details>
```

---

## 판정 + 동작

| DDL | 판정 | CI 동작 |
|-----|------|---------|
| `ALTER TABLE DROP COLUMN` | 🔴 destructive | merge 차단 |
| 모델 삭제 | 🔴 destructive | merge 차단 |
| 파싱 실패 (sync_all_columns 모델) | ⚠️ review | 경고만 (차단 안 함) |
| `ALTER TABLE ADD COLUMN` | ✅ safe | 정보만 |
| `CREATE OR REPLACE TABLE/VIEW` | ✅ safe | 정보만 |
| on_schema_change/materialization 변경 | ⚠️ review | 경고만 |

### Override 메커니즘

의도적인 파괴적 변경(컬럼 제거가 올바른 경우)을 위한 escape hatch:
- PR에 `ddl-reviewed` 라벨 추가 시 CI 차단 해제
- PR 코멘트에는 여전히 DDL 예측 표시 (정보 제공)

---

## 파일 구조

| File | 역할 | 추정 크기 |
|------|------|----------|
| `airflux/scripts/dbt_ddl_check.py` | DDL predictor core | ~250줄 |
| `tests/test_dbt_ddl_check.py` | Unit + integration tests | ~250줄 |
| `tests/fixtures/` | 실제 compiled SQL 샘플 3-5개 | — |
| `.github/workflows/dbt-pr-check.yml` | CI workflow (기존 수정) | ~120줄 |
| `pyproject.toml` | sqlglot dependency 추가 | 1줄 |

### 모듈 구조 (단일 파일, 함수 분리)

```python
# dbt_ddl_check.py

# 1. 컬럼 추출
def extract_columns(sql: str) -> list[str] | None
    # SQLGlot Snowflake dialect, 실패 시 None 반환

# 2. Warehouse 컬럼 조회
def query_warehouse_columns(models: list[str], connection_params) -> dict[str, list[str]]
    # INFORMATION_SCHEMA.COLUMNS, sync_all_columns 모델만

# 3. DDL 예측
def predict_ddl(warehouse_cols, pr_cols, materialized, on_schema_change) -> list[DDLPrediction]

# 4. Downstream impact
def find_downstream(node_id, child_map) -> list[str]

# 5. 통합 실행
def run_check(base_dir, current_dir, manifest_path, connection_params) -> CheckResult

# 6. 출력
def format_github(result) -> str
def format_terminal(result) -> str

# 7. CLI
def main()  # snapshot, check 서브커맨드
```

---

## CI Workflow

```yaml
# .github/workflows/dbt-pr-check.yml (수정)
steps:
  - Org Action checkout
  - Install (uv sync --group dbt)
  - Snowflake credentials (AWS Secrets Manager)
  - Compile base (git checkout base SHA → dbt compile)
  - Compile current (git checkout PR SHA → dbt compile)
  - Run DDL check (dbt_ddl_check.py — INFORMATION_SCHEMA 포함)
  - Post PR comment (github-script, env var로 injection 방지)
  - Fail on destructive (exit code 1 → merge 차단)
  - Skip on ddl-reviewed label
```

### CI 안정성 (Eng Review 반영)

- model 감지: shell `diff -rq` 대신 Python 내부에서 디렉토리 비교
- git checkout: base/current 각각 clean compile (profiles.yml 일관성)
- 동시 실행: `concurrency` 설정으로 PR당 직렬화
- PR 코멘트: marker(`<!-- dbt-ddl-check -->`)로 기존 코멘트 업데이트 (누적 방지)
- 실패 구분: 파싱 에러(exit 2) vs destructive DDL(exit 1) vs safe(exit 0)

---

## Phase 구분

| Phase | 범위 | 산출물 |
|-------|------|--------|
| **1a** | DDL predictor core (extract_columns + predict_ddl + tests) | dbt_ddl_check.py, test_dbt_ddl_check.py |
| **1b** | INFORMATION_SCHEMA 연동 + 통합 테스트 | query_warehouse_columns, fixtures/ |
| **1c** | CI workflow 수정 + PR 코멘트 | dbt-pr-check.yml 수정 |
| **1d** | Override 메커니즘 (ddl-reviewed 라벨) | workflow 조건 추가 |
| **2a** | Slack 알림 (destructive 시) | webhook 연동 |
| **2b** | Recce 로컬 분석 도입 | 별도 평가 |
| **2c** | 컬럼 타입 변경 감지 (ALTER TYPE) | extract_columns 확장 |

---

## 검증 계획

1. **Unit test**: extract_columns — VARIANT, CTE, QUALIFY, LATERAL FLATTEN fallback
2. **Unit test**: predict_ddl — DDL 테이블의 모든 조합
3. **Integration test**: 실제 compiled SQL 파일 3-5개로 E2E
4. **PII 시뮬레이션**: VARIANT 컬럼 제거 → DROP COLUMN 예측 확인
5. **Safe 변경 시뮬레이션**: 컬럼 추가만 → ADD COLUMN 예측 확인
6. **파싱 실패**: LATERAL FLATTEN 모델 → ⚠️ review 판정 확인
7. **Backtest**: 최근 20개 merged PR에 대해 실행 → false positive rate 측정

---

## Out of Scope

- Snowflake Task 자동 실행 관리
- dbt Cloud 마이그레이션
- Recce CI 통합 (로컬만)
- dbt-core upstream 기여
- 컬럼 타입 변경 감지 (Phase 2c)

---

## 근거 출처

| 사항 | 근거 |
|------|------|
| Snowflake DDL 동작 | dbt-snowflake `snowflake__alter_relation_add_remove_columns` 매크로 소스코드 |
| SQLGlot 파싱 성공률 | 128/128 실제 compiled SQL 테스트 |
| INFORMATION_SCHEMA 실용성 | assert_no_pii_columns_in_dbt_schema.sql (기존 사용), Snowflake 문서 |
| deploy-time gate 불가 | Snowflake Tasks `EXECUTE dbt project` 방식, on-run-start SQL 전용 |
| catalog.json 불가 | dbt docs generate = warehouse introspection → base/current 동일 |
| Recce CI 부적합 | PR 코멘트 없음(OSS), catalog 의존, 29 packages |
