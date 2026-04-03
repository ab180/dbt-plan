# dbt-plan

`dbt run` 실행 시 Snowflake에서 어떤 DDL이 실행될지 미리 예측합니다.

dbt 버전의 `terraform plan`.

## 목표

파괴적 DDL 변경(`DROP COLUMN` 등)이 프로덕션에 도달하기 전에 잡아냅니다. dbt의 `incremental` + `sync_all_columns`는 SELECT 컬럼이 바뀌면 자동으로 `ALTER TABLE`을 실행합니다. dbt-plan은 PR 시점에 이를 감지하고 merge를 차단합니다.

## 빠른 시작

```bash
pip install git+https://github.com/ab180/dbt-plan@v0.1.0

# dbt 프로젝트 디렉토리에서:
dbt compile
dbt-plan snapshot          # 기준선 저장 (compiled SQL + manifest)
# ... 모델 변경 ...
dbt compile
dbt-plan check             # DDL 영향 확인
dbt-plan check --format github   # GitHub 마크다운 출력
```

## 출력 예시

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

## 현재 되는 것 (v0.1.0)

| 기능 | 상태 | 설명 |
|------|------|------|
| 컬럼 추출 (SQLGlot) | **완료** | Snowflake 방언, CTE, VARIANT, QUALIFY, UNION ALL |
| DDL 예측 | **완료** | materialization x on_schema_change 전체 조합 |
| 하류 영향 분석 | **완료** | manifest child_map으로 BFS, 순환 참조 보호 |
| 삭제된 모델 감지 | **완료** | materialization 무관 DESTRUCTIVE, base manifest fallback |
| 파싱 실패 안전 | **완료** | 컬럼 추출 실패 시 절대 SAFE 반환 안 함 |
| SELECT * 감지 | **완료** | WARNING 반환 (컬럼 비교 불가) |
| 출력 포맷 | **완료** | `--format text` (기본) / `--format github` |
| 스냅샷 | **완료** | compiled SQL + manifest.json 저장 |
| 종료 코드 | **완료** | 0=안전, 1=파괴적, 2=경고/오류 |
| CI (패키지) | **완료** | Python 3.10-3.13 테스트, 태그 릴리즈 |

## 아직 안 되는 것

| 기능 | 단계 | 왜 필요한가 |
|------|------|-------------|
| INFORMATION_SCHEMA 조회 | 1b | 228개 모델 중 49개가 `SELECT *` 사용 — warehouse 조회 없이 컬럼 추출 불가 |
| CI 워크플로우 템플릿 | 1c | dbt 프로젝트에서 복사해서 바로 쓸 수 있는 GitHub Actions |
| PR 코멘트 자동 게시 | 1c | `<!-- dbt-plan -->` 마커로 중복 없이 DDL 예측 결과 PR에 게시 |
| `ddl-reviewed` 라벨 | 1d | 의도적 파괴적 변경을 위한 CI 차단 해제 |
| Slack 알림 | 2a | 파괴적 DDL 발생 시 알림 |
| `dbt-plan run` 래퍼 | 2b | `dbt run` 전 대화형 확인 |
| 컬럼 타입 감지 | 2c | `ALTER TYPE` 예측 (현재 5.2%만 타입 추출 가능) |

## DDL 예측 규칙

| materialization | on_schema_change | 예측 DDL | 판정 |
|-----------------|------------------|----------|------|
| table | 무관 | `CREATE OR REPLACE TABLE` | SAFE |
| view | 무관 | `CREATE OR REPLACE VIEW` | SAFE |
| incremental | ignore | DDL 없음 | SAFE |
| incremental | fail | 빌드 실패 | WARNING |
| incremental | append_new_columns | `ADD COLUMN`만 | SAFE |
| incremental | sync_all_columns | `ADD + DROP COLUMN` | 컬럼 삭제 시 DESTRUCTIVE |
| 전체 | (모델 삭제) | `MODEL REMOVED` | DESTRUCTIVE |

## CI 연동 (GitHub Actions)

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

      # Base 브랜치 컴파일 + 스냅샷
      - run: |
          git checkout ${{ github.event.pull_request.base.sha }}
          dbt compile
          dbt-plan snapshot

      # Current 브랜치 컴파일 + 체크
      - run: |
          git checkout ${{ github.event.pull_request.head.sha }}
          dbt compile
          dbt-plan check --format github >> $GITHUB_STEP_SUMMARY

      # 파괴적 변경 시 merge 차단 (exit 1)
      - run: dbt-plan check
```

## 동작 원리

```
1. dbt-plan snapshot
   compiled SQL + manifest.json을 기준선으로 저장

2. dbt-plan check
   compiled SQL 비교 → 컬럼 추출 (SQLGlot) → DDL 예측 → 하류 영향 분석

   base compiled/*.sql  vs  current compiled/*.sql
        ↓                        ↓
   extract_columns()        extract_columns()
        ↓                        ↓
   base 컬럼        ──diff──  current 컬럼
                      ↓
              predict_ddl() + manifest 설정
                      ↓
              DDL 예측 + downstream 모델
                      ↓
              format_text() / format_github()
```

## 기여하기

### 개발 환경 세팅

```bash
git clone https://github.com/ab180/dbt-plan
cd dbt-plan
pip install -e ".[test]"
pytest -v                  # 42개 테스트 전체 실행
dbt-plan --version         # dbt-plan 0.1.0
```

### 개발 규칙

- **TDD**: 테스트 먼저 작성 → 실패 확인 → 구현 → 통과 확인
- **sqlglot 외 런타임 의존성 추가 금지**
- **파싱 실패 시 SAFE 반환 절대 금지** — None 반환 → 호출자가 WARNING 처리
- **커밋 메시지**: conventional commits (`feat:`, `fix:`, `test:`, `docs:`)

### 아키텍처

```
src/dbt_plan/
├── columns.py      # SQLGlot 기반 컬럼 추출
├── predictor.py    # DDL 예측 규칙 엔진
├── manifest.py     # manifest.json 파싱 + downstream BFS 탐색
├── diff.py         # compiled SQL 디렉토리 비교
├── formatter.py    # 터미널 / GitHub 마크다운 출력
└── cli.py          # CLI 진입점 (snapshot, check)
```

### 기여 가이드

**쉬운 이슈 (처음 기여하기 좋은 것):**
- `tests/fixtures/`에 실제 compiled SQL 패턴 추가
- 에러 메시지 개선 (흔한 실수에 대한 안내)
- `--verbose` 플래그 추가 (디버깅용)

**중간 이슈:**
- INFORMATION_SCHEMA 연동 (Phase 1b) — warehouse에서 실제 컬럼 조회
- CI 워크플로우 템플릿 — dbt 프로젝트용 재사용 가능한 GitHub Actions
- PR 코멘트 게시 (`<!-- dbt-plan -->` 마커로 중복 방지)

**설계 문서:** [docs/architecture-decisions.md](docs/architecture-decisions.md), [docs/design-spec.md](docs/design-spec.md) 참조.

## 지원 환경

- dbt-core 1.7+ with dbt-snowflake
- Python 3.10+
- Snowflake VARIANT (`col:path::TYPE`), CTE, QUALIFY, 윈도우 함수

## 라이선스

Apache-2.0
