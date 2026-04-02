# Architecture Decision Records

이 문서는 dbt-plan 설계 과정에서 내린 결정과 그 근거를 기록합니다.
새 워크스페이스에서 작업을 이어받을 때 "왜 이렇게 했는가"를 이해하기 위한 문서입니다.

---

## ADR-1: 왜 별도 OSS 패키지인가

### 결정
`ab180/dbt-plan`을 독립 public OSS 패키지로 만든다. airflux-dbt 내부 스크립트가 아님.

### 이유
- ab180에 dbt 프로젝트가 여러 개 있어서 재사용 필요
- CI에서 `pip install git+https://github.com/ab180/dbt-plan@v0.1.0`으로 설치
- 로컬에서 `pipx install dbt-plan`으로 CLI 사용
- 프로젝트별 하드코딩 없음 — 환경변수로 설정

### 거부된 대안
- airflux-dbt 내부 스크립트: 한 프로젝트에서만 사용 가능, 재사용 불가

---

## ADR-2: 왜 SQLGlot으로 컬럼 추출하는가

### 결정
compiled SQL을 SQLGlot (Snowflake dialect)으로 파싱하여 SELECT 컬럼을 추출한다.

### 이유
- compiled SQL은 `dbt compile` 후 `target/compiled/`에 순수 SELECT문으로 생성됨 (MERGE 아님, Jinja 완전 해소)
- 128/128 모델 파싱 성공 (100%) — 실제 compiled SQL 파일로 테스트 완료
- SQLGlot은 zero-dependency, 2.8MB, 이미 venv에 설치되어 있음
- VARIANT `col:path::TYPE`, CTE, QUALIFY, 윈도우 함수 모두 정상 파싱

### 거부된 대안
| 대안 | 거부 이유 |
|------|----------|
| **text diff (difflib)** | 컬럼 자동 추출 불가 — 사람이 봐야 함. "사람이 분석하지 않는다"는 요구사항 위반 |
| **regex** | CTE/QUALIFY/UNION ALL 처리 불가, 정확도 낮음 |
| **Recce** | 29 packages dependency, PR 코멘트 없음(OSS), 커스텀 규칙 불가, catalog.json 의존 |
| **catalog.json** | `dbt docs generate`가 warehouse를 introspect하므로 base/current가 동일 (아래 ADR-3 참조) |

### 한계
- 49/228 모델이 `SELECT *` 사용 → 컬럼 추출 불가 → INFORMATION_SCHEMA fallback 필요 (ADR-3)
- 11/128 모델 LATERAL FLATTEN → `qualify_columns` 실패 → 기본 파싱으로 fallback
- 컬럼 타입 추출: 1845개 컬럼 중 5.2%만 가능 → Phase 2로 이동

### SQLGlot 사용 코드 패턴

```python
import sqlglot
from sqlglot import exp

def extract_columns(sql: str) -> list[str] | None:
    """Extract column names from compiled SQL's final SELECT.

    Returns:
        list[str]: Column names (lowercased aliases)
        ["*"]: If final SELECT is SELECT *
        None: If parsing fails (LATERAL FLATTEN, invalid SQL, etc.)
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
```

실제 compiled SQL 패턴 (airflux-dbt 기준):

```sql
-- Pattern A: 명시적 컬럼 + VARIANT (int_unified)
SELECT
    e.write_date,
    e.data__device:airbridgeGeneratedDeviceUUID::STRING AS data__device__airbridgegenerateddeviceuuid,
    e.data__device:deviceUUID::STRING AS data__device__deviceuuid,
    e.event_date
FROM all_events
QUALIFY ROW_NUMBER() OVER (PARTITION BY log_uuid ORDER BY received_timestamp DESC) = 1

-- Pattern B: SELECT * (fct_client_events)
WITH base AS (SELECT * FROM source_table)
SELECT * FROM base

-- Pattern C: CTE + UNION ALL
WITH combined AS (
    SELECT a, b FROM t1
    UNION ALL
    SELECT a, b FROM t2
)
SELECT a, b, 'combined' AS source FROM combined
```

---

## ADR-3: 왜 INFORMATION_SCHEMA가 필수인가

### 결정
sync_all_columns 모델에 대해 INFORMATION_SCHEMA.COLUMNS로 warehouse 실제 컬럼을 조회한다.
이것은 "optional fallback"이 아니라 **필수 경로**이다.

### 이유
- sync_all_columns 14개 모델 중 **6개 이상이 SELECT *** 사용 → SQLGlot으로 컬럼 추출 불가
- DDL 예측은 "현재 warehouse 테이블 컬럼" vs "PR의 SELECT 컬럼"의 차이로 결정됨
- catalog.json은 사용 불가 (ADR-2 거부 대안 참조)
- INFORMATION_SCHEMA 조회 비용: ~$0.001, sub-second (메타데이터만 읽음)
- 이미 airflux-dbt에서 `assert_no_pii_columns_in_dbt_schema.sql` 테스트로 동일 패턴 사용 중

### 정확한 쿼리

```sql
SELECT
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    ORDINAL_POSITION
FROM {database}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{schema}'
  AND TABLE_NAME IN (
      'MODEL_NAME_1',  -- 대문자 (Snowflake 기본)
      'MODEL_NAME_2',
      ...
  )
ORDER BY TABLE_NAME, ORDINAL_POSITION;
```

- 14개 테이블을 단일 IN 쿼리로 배치 가능
- 신규 모델: 빈 결과 반환 (에러 없음 → "drop 위험 없음" 신호)
- warehouse가 warm 상태 (dbt compile이 먼저 실행되므로)

### catalog.json이 안 되는 이유

catalog.json은 `dbt docs generate`가 Snowflake warehouse를 introspect해서 생성한다.
PR 브랜치에서 `dbt docs generate`를 해도 **dbt run을 안 했으므로 warehouse 테이블은 그대로**이다.
즉, base catalog.json과 current catalog.json은 **동일한 warehouse 상태를 반영**한다.
컬럼 변경을 감지할 수 없다.

---

## ADR-4: 왜 PR merge가 유일한 interception point인가

### 결정
DDL 체크를 PR merge 전 CI에서만 실행한다. dbt run 시점 체크는 하지 않는다.

### 이유
airflux-dbt 프로젝트의 실제 dbt 실행 방식:
- **17개 Snowflake Tasks**가 `EXECUTE dbt project AIRFLUX_DBT`로 실행
- 로컬 CLI `dbt run`은 프로덕션에서 사용하지 않음
- Snowflake Tasks 내부에 shell wrapper를 끼워넣을 수 없음

### 거부된 대안
| 대안 | 거부 이유 |
|------|----------|
| dbt run shell wrapper | Snowflake Tasks가 `EXECUTE dbt project`로 실행 — shell 접근 불가 |
| on-run-start hook | SQL만 실행 가능 (Python/local file 접근 불가) — dbt 소스코드 확인 |
| dbt --empty | 실제 DDL 실행됨 (dry-run 아님) |
| dbt --defer + --state | 변경 모델 목록만 알려줌, DDL 예측 안 함 |

### 근거
- dbt-core 소스코드 `dbt/task/run.py`에서 `on-run-start`는 `safe_run_hooks(adapter, RunHookType.Start)`로 실행 — SQL 전용
- `dbt/cli/params.py`에서 `--empty`는 `WHERE 1=0`으로 감싸지만 DDL은 그대로 실행됨
- `snowflake/task/` 디렉토리에 17개 Task 파일 확인

---

## ADR-5: DDL 예측 규칙 (dbt-snowflake 소스코드 확인)

### 결정
materialization + on_schema_change 조합으로 결정론적 DDL 예측.

### DDL 규칙 테이블

| materialization | on_schema_change | DDL | 판정 |
|---|---|---|---|
| table | * | `CREATE OR REPLACE TABLE AS SELECT` | safe |
| view | * | `CREATE OR REPLACE VIEW AS SELECT` | safe |
| incremental | ignore | 없음 (스키마 불일치 무시) | safe |
| incremental | fail | 없음 (빌드 실패) | warning |
| incremental | append_new_columns | `ALTER TABLE ADD COLUMN` only | safe |
| incremental | sync_all_columns | `ALTER TABLE ADD COLUMN` + `ALTER TABLE DROP COLUMN` | destructive (if columns removed) |

### 근거
dbt-snowflake 소스코드:
- `snowflake__create_table_as` 매크로: table → CREATE OR REPLACE
- `snowflake__alter_relation_add_remove_columns` 매크로: sync_all_columns → ADD + DROP
- `process_schema_changes` 함수: ignore → no DDL, fail → raise error
- `alter_relation_add_remove_columns(remove=none)`: append → ADD only

### 파싱 실패 시 처리

**절대 "safe" 반환하지 않는다.**

| 상황 | 처리 |
|------|------|
| SQLGlot 파싱 성공 (명시적 컬럼) | 컬럼 추출 → DDL 예측 |
| SELECT * + sync_all_columns | INFORMATION_SCHEMA 조회 → DDL 예측 |
| SELECT * + table/view | safe (CREATE OR REPLACE) |
| LATERAL FLATTEN 파싱 실패 + sync_all_columns | INFORMATION_SCHEMA 조회 → DDL 예측 |
| INFORMATION_SCHEMA 조회 실패 | ⚠️ review ("컬럼 확인 불가 — 수동 확인 필요") |
| compiled SQL 파일 없음 (ephemeral) | 스킵 (물리 테이블 없음) |

---

## ADR-6: 왜 Recce를 CI에 쓰지 않는가

### 결정
Recce는 CI가 아닌 로컬 분석 도구로만 별도 도입한다 (Phase 2b).

### 이유
| 요구사항 | Recce 지원 여부 |
|----------|----------------|
| compiled SQL diff | summary에 미포함 |
| 커스텀 위험 규칙 | 지원 안 함 (check type 고정) |
| PR 코멘트 (OSS) | 불가 (Cloud만) |
| Slack 알림 | 없음 |
| schema_diff | catalog.json 의존 (같은 warehouse 문제) |

### 추가 문제
- 설치: 29개 신규 패키지, 60-120초
- `pip install recce` → boto3, fastapi, sentry-sdk 등 불필요한 dependency
- profiles.yml 필수 (adapter 초기화 필요)

### Recce가 여전히 가치 있는 이유
- `recce server --review` → lineage 시각화, Column-Level Lineage
- SQLGlot VARIANT 파싱 이슈는 2024-02에 해결됨 (Issue #2957)
- 로컬에서 PR 전 개인 검토 용도로 사용 가능

---

## ADR-7: 사용자 핵심 요구사항

### "사람이 직접 분석하는 일을 거의 없애고"
- 시스템이 자동으로 감지 → 보고 → 판정
- "sync_all_columns이면 위험" 같은 binary flag가 아니라, "ALTER TABLE DROP COLUMN data__device가 실행됩니다"를 보여줌
- PR 코멘트 + Slack으로 push (사람이 문서를 찾아보는 방식 아님)

### "보고받는 방식"
- DDL 예측 결과가 PR 코멘트로 자동 게시
- destructive DDL 시 merge 차단
- (Phase 2a) Slack으로 알림

### "유지보수할 수 있는 형태"
- 독립 패키지, 의존성 최소 (sqlglot만)
- 환경변수 기반 설정 (프로젝트별 하드코딩 없음)
- TDD, CI, 릴리즈 자동화

### "dbt 프로젝트 여러 개에서 사용"
- `pip install git+https://github.com/ab180/dbt-plan@v0.1.0`
- CI workflow template 제공 (docs/ci-integration.md)
- 프로젝트별 `--project-dir` 플래그만 변경

---

## ADR-8: CEO/Eng Review 핵심 발견

### CEO Review

1. **CRITICAL: "컬럼 추출 ≠ DDL 예측"** → INFORMATION_SCHEMA로 해결 (ADR-3)
2. **HIGH: deploy-time gate가 더 높은 레버리지** → 불가 (ADR-4 — Snowflake Tasks)
3. **HIGH: alert fatigue** → DDL 예측 방식으로 전환 (binary flag 아님)
4. **MEDIUM: dbt state:modified 미활용** → 구현 시 검토 (shell diff 대체)
5. **HIGH: scope 과대** → v1은 core + CI만, Slack/포매터는 v2

### Eng Review

1. **CRITICAL: 파싱 실패 시 safe 반환 금지** → None → review 처리
2. **HIGH: 컬럼 타입 변경 감지 누락** → Phase 2c (5.2%만 추출 가능)
3. **HIGH: 실제 compiled SQL 통합 테스트 필요** → tests/fixtures/
4. **HIGH: shell diff 파이프라인 취약** → Python 내부 디렉토리 비교
5. **HIGH: git checkout CI 비일관** → concurrency + clean compile
6. **MEDIUM: PR 코멘트 누적** → `<!-- dbt-plan -->` marker
7. **MEDIUM: 150줄 추정 과소** → 실제 ~250줄

---

## sync_all_columns 모델 목록 (14개, 검증 완료)

모두 model-level `{{ config() }}` override. dbt_project.yml 상속 0개.

| # | 모델 | SELECT * 여부 |
|---|------|:---:|
| 1 | int_airbridge__experiments_app_events_unified | * |
| 2 | int_airbridge__experiments_parsed_app_events | * |
| 3 | dim_app | * |
| 4 | dim_device | * |
| 5 | dim_device_install_date | - |
| 6 | dim_api_user | - |
| 7 | fct_client_events | * |
| 8 | fct_client_events_by_ip_country | * |
| 9 | fct_sdk_v1_event_validation | - (LATERAL FLATTEN) |
| 10 | fct_airflux_active_experiment_events | - |
| 11 | fct_airflux_inference_api_log | - |
| 12 | fct_internal_metrics_daily | - |
| 13 | fct_internal_metrics_cohort | - |
| 14 | int_internal_daily_base | - |

**6개가 SELECT * 사용 → INFORMATION_SCHEMA 필수.**
**1개(#9)가 LATERAL FLATTEN → SQLGlot qualify_columns 실패 → 기본 파싱 fallback.**
