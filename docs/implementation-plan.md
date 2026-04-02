# dbt-plan — DDL Impact Predictor

## Context

PII 대응 작업에서 `int_unified`의 VARIANT 컬럼 제거 → `sync_all_columns`로 `ALTER TABLE DROP COLUMN` 실행 → 10+ 모델 연쇄 파괴. PR merge 전에 DDL 영향을 자동 예측하는 시스템이 필요하다.

**`ab180/dbt-plan`**: 독립 OSS 패키지. `terraform plan`처럼 "dbt run 하면 뭐가 바뀌는지" 미리 보여준다.

## 패키지 정보

- **레포**: `ab180/dbt-plan` (public)
- **CLI**: `dbt-plan check`, `dbt-plan run`, `dbt-plan snapshot`
- **설치**: `pip install git+https://github.com/ab180/dbt-plan` 또는 `pipx install dbt-plan`
- **의존성**: `sqlglot>=26.0.0` (zero-dep, 2.8MB)
- **Python**: `>=3.10`

## CLI 사용법

```bash
dbt-plan check                            # standalone: diff + predict (CI용)
dbt-plan check --format github            # CI: PR 코멘트 마크다운 출력
dbt-plan run --select int_unified         # wrapper: plan → confirm → dbt run (로컬용)
dbt-plan snapshot                         # baseline 저장 (.dbt-plan/base/)
```

## 이 플랜의 scope

Phase 1a — DDL Predictor Core (check 서브커맨드). 후속:
- 1b: INFORMATION_SCHEMA 연동 (warehouse 실제 컬럼)
- 1c: CI workflow (GitHub Actions)
- 1d: Override (ddl-reviewed 라벨)
- 2a: Slack 알림
- 2b: run 서브커맨드 (dbt run 래핑)

## 핵심 설계 원칙

- **파싱 실패 ≠ safe** — None 반환 → 호출자가 ⚠️ review로 처리
- **SELECT * → `["*"]` 반환** — Phase 1b에서 INFORMATION_SCHEMA로 처리
- **환경변수로 설정** — 프로젝트별 하드코딩 없음
- **dbt 아티팩트 자동 감지** — `target/manifest.json`, `target/compiled/`

## 파일 구조

```
ab180/dbt-plan/
├── src/dbt_plan/
│   ├── __init__.py
│   ├── columns.py          # extract_columns (SQLGlot)
│   ├── predictor.py        # predict_ddl (DDL 규칙)
│   ├── manifest.py         # manifest 파싱, child_map, find_node
│   ├── diff.py             # compiled SQL 디렉토리 비교
│   ├── formatter.py        # github / terminal 출력
│   └── cli.py              # CLI entry point (check, snapshot)
├── tests/
│   ├── fixtures/
│   │   ├── explicit_columns.sql
│   │   ├── select_star.sql
│   │   └── variant_access.sql
│   └── test_*.py
├── pyproject.toml
├── .gitignore
├── LICENSE                  # Apache-2.0
├── README.md
└── CONTRIBUTING.md
```

## 검증된 사실 (근거 기반)

- SQLGlot: 128/128 파싱 성공, VARIANT/CTE/QUALIFY 정상, zero-dep
- compiled SQL: 순수 SELECT (MERGE 아님), 179개 명시적 컬럼, 49개 SELECT *
- sync_all_columns 14개 중 6개가 SELECT * → INFORMATION_SCHEMA 필수 (Phase 1b)
- LATERAL FLATTEN 파싱 실패 11개 중 sync_all_columns는 1개만
- INFORMATION_SCHEMA: ~$0.001, sub-second, 기존 dbt test에서 사용 중

---

## Task 0: 레포 초기화 + pyproject.toml

```bash
# 레포 생성 (수동 또는 gh repo create)
gh repo create ab180/dbt-plan --public --clone
cd dbt-plan
```

**pyproject.toml:**
```toml
[project]
name = "dbt-plan"
version = "0.1.0"
description = "Preview what DDL changes dbt run will execute on Snowflake"
requires-python = ">=3.10"
license = "Apache-2.0"
dependencies = [
    "sqlglot>=26.0.0",
]

[project.scripts]
dbt-plan = "dbt_plan.cli:main"

[project.optional-dependencies]
test = ["pytest>=8.0.0"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/dbt_plan"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

**.gitignore:**
```
__pycache__/
*.egg-info/
dist/
build/
.dbt-plan/
.pytest_cache/
*.pyc
.venv/
```

커밋: `chore: init dbt-plan package`

---

## Task 1: 테스트 fixture 생성

**파일:** `tests/fixtures/` (3개)

동일한 fixture 파일 (경로만 `tests/fixtures/`로 변경 없음).

커밋: `test: add compiled SQL fixtures`

---

## Task 2: extract_columns 테스트 (red)

**파일:** `tests/test_dbt_ddl_check.py`

7개 테스트:
- `test_explicit_columns` — fixture에서 alias 추출
- `test_select_star` — `["*"]` 반환
- `test_variant_access_with_qualify` — VARIANT + QUALIFY
- `test_lateral_flatten_no_crash` — 크래시 없이 None 또는 list
- `test_cte_with_union_all` — 최종 SELECT 컬럼만
- `test_parse_failure_returns_none` — 잘못된 SQL
- `test_simple_select` — 단순 SELECT

검증: `uv run pytest tests/test_dbt_ddl_check.py -x` → ModuleNotFoundError

커밋: `test: add extract_columns tests (red phase)`

---

## Task 3: extract_columns 구현 (green)

**파일:** `src/dbt_plan/columns.py`

```python
import sqlglot
from sqlglot import exp

def extract_columns(sql: str) -> list[str] | None:
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

검증: `pytest tests/test_columns.py -v` → 7 pass

커밋: `feat: implement extract_columns with SQLGlot Snowflake parser`

---

## Task 4: predict_ddl 테스트 (red)

8개 테스트: table(safe), view(safe), incremental×ignore(no DDL), ×fail(build failure), ×append(ADD only), ×sync(ADD+DROP), sync+no changes(empty), empty warehouse(new model).

커밋: `test: add predict_ddl tests (red phase)`

---

## Task 5: predict_ddl 구현 (green)

**파일:** `src/dbt_plan/predictor.py`

DDLPrediction dataclass + predict_ddl 함수. 모든 materialization × on_schema_change 조합.

커밋: `feat: implement predict_ddl with Snowflake DDL rules`

---

## Task 6: find_downstream 테스트 + 구현

5개 테스트: direct children, recursive, cycle protection, test node filter, no children.

커밋: `feat: implement find_downstream with cycle protection`

---

## Task 7: diff_compiled_dirs 테스트 + 구현

4개 테스트: modified, added, removed, unchanged.

커밋: `feat: implement diff_compiled_dirs`

---

## Task 8: manifest + formatters + CLI

**파일:**
- `src/dbt_plan/manifest.py` — find_node_by_name (node_id, node) 반환
- `src/dbt_plan/formatter.py` — format_github / format_terminal
- `src/dbt_plan/cli.py` — argparse, check/snapshot 서브커맨드, `[project.scripts]`로 `dbt-plan` 명령어 등록

커밋: `feat: implement manifest, formatters, and CLI entry point`

---

## Task 9: 통합 테스트

full pipeline: diff → extract → predict → downstream → format

커밋: `test: add integration test for DDL prediction pipeline`

---

## 검증

1. `pytest -v` → 전체 pass (extract_columns, predict_ddl, downstream, diff, manifest, formatter, integration)
2. `pip install -e .` → `dbt-plan --help` 동작
3. `dbt-plan check --help` → 서브커맨드 동작
4. 다른 dbt 프로젝트에서 `pip install git+https://github.com/ab180/dbt-plan` → 설치 + 실행 확인

---

## Task 10: README.md

OSS로서 5분 내 시작 가능한 문서.

```markdown
# dbt-plan

Preview what DDL changes `dbt run` will execute — before you run it.

Like `terraform plan` for dbt. Parses compiled SQL to predict `ALTER TABLE`,
`DROP COLUMN`, and other DDL operations that Snowflake will execute.

## Quick Start

pip install git+https://github.com/ab180/dbt-plan

# In your dbt project directory:
dbt compile
dbt-plan snapshot          # Save baseline
# ... make changes ...
dbt compile
dbt-plan check             # See what will change

## CI Integration (GitHub Actions)

- name: Install dbt-plan
  run: pip install git+https://github.com/ab180/dbt-plan@v0.1.0

- name: Run DDL check
  run: dbt-plan check --format github >> $GITHUB_STEP_SUMMARY

## Output Example

$ dbt-plan check

dbt-plan — 2 models changed

🔴 DESTRUCTIVE  int_unified (incremental, sync_all_columns)
  DROP COLUMN  data__device
  DROP COLUMN  data__user
  ADD COLUMN   data__device__uuid
  Downstream: dim_device, fct_events, vw_client_events (8 models)

✅ SAFE  dim_device (table)
  CREATE OR REPLACE TABLE (full rebuild)

## How It Works

1. Parses compiled SQL with SQLGlot (Snowflake dialect)
2. Extracts SELECT column lists from base and current
3. Compares columns + reads manifest config (materialized, on_schema_change)
4. Predicts DDL: CREATE OR REPLACE, ALTER TABLE ADD/DROP COLUMN, etc.
5. Reports impact with downstream model list

## Supported

- dbt-core 1.7+ with dbt-snowflake
- Python 3.10+
- Snowflake VARIANT, CTE, QUALIFY, window functions
```

커밋: `docs: add README with quick start and CI guide`

---

## Task 11: CI integration guide (docs/ci-integration.md)

각 dbt 프로젝트에서 `dbt-plan`을 CI에 붙이는 구체적 레시피.

```markdown
# CI Integration Guide

## GitHub Actions (recommended)

### Minimal setup

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

      # Compile base
      - run: |
          git checkout ${{ github.event.pull_request.base.sha }}
          dbt compile
          dbt-plan snapshot

      # Compile current
      - run: |
          git checkout ${{ github.event.pull_request.head.sha }}
          dbt compile

      # Check + post
      - run: dbt-plan check --format github >> $GITHUB_STEP_SUMMARY

      # Block on destructive
      - run: dbt-plan check  # exit 1 if destructive

### With INFORMATION_SCHEMA (Phase 1b)

dbt-plan check --live  # queries warehouse for actual columns

### With PR comment

- uses: actions/github-script@v7
  env:
    RESULT: ${{ steps.plan.outputs.result }}
  with:
    script: |
      const body = process.env.RESULT;
      // find/update existing comment with marker
      // <!-- dbt-plan -->

### Self-hosted runner notes

- concurrent PR: set `concurrency: { group: dbt-plan-${{ github.ref }} }`
- credentials: AWS Secrets Manager or GitHub Secrets
```

커밋: `docs: add CI integration guide`

---

## Task 12: 패키지 자체 CI (.github/workflows/)

`dbt-plan` 레포 자체의 CI — PR마다 lint + test.

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.11", "3.12"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: ${{ matrix.python-version }} }
      - run: pip install -e ".[test]"
      - run: pytest -v
```

```yaml
# .github/workflows/release.yml
name: Release
on:
  push:
    tags: ['v*']

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install build
      - run: python -m build
      - uses: softprops/action-gh-release@v2
        with:
          files: dist/*
```

커밋: `ci: add test and release workflows`

---

## Task 13: CLAUDE.md

AI 에이전트가 이 레포에서 작업할 때의 지침.

```markdown
# dbt-plan

## 프로젝트 개요
dbt compiled SQL을 파싱하여 Snowflake DDL 영향을 예측하는 CLI 도구.
terraform plan의 dbt 버전.

## 아키텍처
- src/dbt_plan/columns.py: SQLGlot 기반 컬럼 추출
- src/dbt_plan/predictor.py: DDL 예측 규칙
- src/dbt_plan/manifest.py: manifest.json 파싱
- src/dbt_plan/diff.py: compiled SQL 디렉토리 비교
- src/dbt_plan/formatter.py: 출력 포매팅
- src/dbt_plan/cli.py: CLI entry point

## 개발
pip install -e ".[test]"
pytest -v

## 규칙
- Snowflake dialect만 지원 (현재)
- sqlglot 외 런타임 의존성 추가 금지
- 파싱 실패 시 safe 반환 절대 금지
- 테스트 없는 기능 추가 금지
```

커밋: `docs: add CLAUDE.md for AI agent guidelines`

---

## Task 14: 환경변수 + 설정 문서

`dbt-plan`이 사용하는 환경변수 목록과 설정 방법.

```markdown
# docs/configuration.md

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| DBT_PROJECT_DIR | No | `.` | dbt 프로젝트 루트 |
| DBT_TARGET_DIR | No | `target` | dbt compile 출력 |
| DBT_PLAN_BASE_DIR | No | `.dbt-plan/base` | snapshot 저장 경로 |
| SNOWFLAKE_ACCOUNT | Phase 1b | - | INFORMATION_SCHEMA 조회용 |
| SNOWFLAKE_USER | Phase 1b | - | Snowflake 인증 |
| SNOWFLAKE_PRIVATE_KEY | Phase 1b | - | Snowflake 인증 |
| SNOWFLAKE_DATABASE | Phase 1b | - | 대상 데이터베이스 |
| SNOWFLAKE_SCHEMA | Phase 1b | `DBT` | 대상 스키마 |

## CLI Flags (환경변수 override)

dbt-plan check --project-dir ./my-dbt --target-dir ./target --format github
```

커밋: `docs: add configuration reference`

---

## Task 15: GitHub Issues + Milestones 생성

레포에서 로드맵 관리.

```bash
# Milestone 생성
gh api repos/ab180/dbt-plan/milestones -f title="v0.1.0 - Core DDL Prediction" -f description="Phase 1a: SQLGlot column extraction + DDL prediction"
gh api repos/ab180/dbt-plan/milestones -f title="v0.2.0 - Warehouse Integration" -f description="Phase 1b: INFORMATION_SCHEMA + Phase 1c: CI workflow"
gh api repos/ab180/dbt-plan/milestones -f title="v0.3.0 - Production Ready" -f description="Phase 1d: Override + Phase 2a: Slack"

# Issue 생성 (Phase별)
gh issue create --title "[1b] INFORMATION_SCHEMA 연동" --body "..."  --milestone "v0.2.0"
gh issue create --title "[1c] CI workflow template" --body "..."      --milestone "v0.2.0"
gh issue create --title "[1d] ddl-reviewed override" --body "..."     --milestone "v0.3.0"
gh issue create --title "[2a] Slack 알림" --body "..."                --milestone "v0.3.0"
gh issue create --title "[2b] dbt-plan run wrapper" --body "..."      --milestone "v0.3.0"
gh issue create --title "[2c] 컬럼 타입 변경 감지" --body "..."       --milestone "v0.3.0"
```

커밋: (GitHub API로 생성, 코드 커밋 없음)

---

## Task 16: 릴리즈 전략 문서

```markdown
# docs/releasing.md

## Versioning (SemVer)

- v0.x.y: 초기 개발. API 변경 가능.
- v1.0.0: stable API 확정 후.

## Release Process

1. `pyproject.toml`에서 version bump
2. `git tag v0.1.0 && git push --tags`
3. GitHub Actions가 자동으로 GitHub Release 생성 + 빌드 아티팩트 첨부

## Consuming Projects

# 최신 릴리즈 pin
pip install git+https://github.com/ab180/dbt-plan@v0.1.0

# latest (unstable)
pip install git+https://github.com/ab180/dbt-plan
```

커밋: `docs: add release strategy`

---

## Task 17: CONTRIBUTING.md

```markdown
# Contributing to dbt-plan

## Development Setup

git clone https://github.com/ab180/dbt-plan
cd dbt-plan
pip install -e ".[test]"
pytest -v

## Adding Features

1. Write failing test first (TDD)
2. Implement the minimal code to pass
3. Run `pytest -v` to verify
4. Commit with conventional commits (feat:, fix:, docs:, test:)

## Architecture

See CLAUDE.md for module responsibilities.

## Key Rules

- No runtime dependencies beyond sqlglot
- Parse failure must never return "safe"
- Every feature needs tests
- Snowflake dialect only (for now)
```

커밋: `docs: add CONTRIBUTING.md`

---

## 내부 인프라 통합 (airflux-dbt 기준)

### Phase 1c에서 할 일 (별도 workspace)

1. `airflux-dbt/.github/workflows/dbt-pr-check.yml` 수정:
   - `pip install git+https://github.com/ab180/dbt-plan@v0.1.0` 추가
   - 기존 `dbt_risk_check.py` 호출 → `dbt-plan check --format github` 교체
   - PR 코멘트: `<!-- dbt-plan -->` marker로 업데이트 (누적 방지)

2. 기존 v1 정리:
   - `airflux/scripts/dbt_risk_check.py` 삭제
   - `tests/test_dbt_risk_check.py` 삭제
   - `pyproject.toml`에서 sqlglot 제거 (dbt-plan이 가져옴)

3. 다른 dbt 프로젝트 적용:
   - CI workflow를 `docs/ci-integration.md`의 template에서 복사
   - 프로젝트별 `--project-dir` 플래그만 조정

---

## 전체 실행 순서

| 순서 | Task | 산출물 |
|------|------|--------|
| 1 | Task 0: 레포 초기화 | pyproject.toml, .gitignore, LICENSE, 빈 패키지 구조 |
| 2 | Task 13: CLAUDE.md | AI 작업 지침 |
| 3 | Task 17: CONTRIBUTING.md | 기여 가이드 |
| 4 | Task 1-9: 코어 구현 (TDD) | src/dbt_plan/*.py, tests/ |
| 5 | Task 10: README | Quick Start + CI guide + 출력 예시 |
| 6 | Task 11: CI integration guide | docs/ci-integration.md |
| 7 | Task 14: 설정 문서 | docs/configuration.md |
| 8 | Task 16: 릴리즈 전략 | docs/releasing.md |
| 9 | Task 12: 패키지 CI | .github/workflows/ci.yml, release.yml |
| 10 | Task 15: GitHub Issues | Milestones + Issues |
| 11 | v0.1.0 태그 | 첫 릴리즈 |
| 12 | airflux-dbt CI 통합 | Phase 1c (별도 workspace) |

## 검증

1. `pytest -v` → 전체 pass
2. `pip install -e .` → `dbt-plan --help` 동작
3. `dbt-plan check --help` → 서브커맨드 동작
4. 다른 dbt 프로젝트에서 `pip install git+https://github.com/ab180/dbt-plan` → 설치 + 실행 확인
5. GitHub Actions CI green
6. `git tag v0.1.0 && git push --tags` → Release 자동 생성
