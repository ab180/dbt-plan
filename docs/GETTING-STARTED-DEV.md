# Getting Started (개발자/에이전트용)

이 문서는 새 workspace에서 dbt-plan 개발을 이어받을 때 읽어야 할 가이드입니다.

## 현재 상태

**Phase 0 완료.** 레포 초기화 + 패키지 구조 + 문서 세팅 완료.

### 완료된 것

- [x] `ab180/dbt-plan` GitHub 레포 생성 (public)
- [x] `pyproject.toml` (hatchling, sqlglot, pytest, CLI entry point)
- [x] `src/dbt_plan/__init__.py`, `cli.py` (스캐폴딩)
- [x] `.gitignore`, `LICENSE` (Apache-2.0)
- [x] `CLAUDE.md`, `CONTRIBUTING.md`, `README.md`
- [x] `pip install -e ".[test]"` → `dbt-plan --help` 동작 확인
- [x] docs/ (design-spec, architecture-decisions, implementation-plan, research-findings)

### 다음 할 일

**Task 1부터 시작.** `docs/implementation-plan.md`의 Task 1 (테스트 fixture) → Task 9 (통합 테스트)를 순차 실행.

## 시작 방법

```bash
# 1. 레포 clone (이미 되어있으면 skip)
git clone https://github.com/ab180/dbt-plan
cd dbt-plan

# 2. 개발 환경 세팅
python3 -m venv .venv
.venv/bin/pip install -e ".[test]"

# 3. 현재 상태 확인
.venv/bin/dbt-plan --help          # CLI 동작 확인
.venv/bin/pytest -v                # 테스트 (아직 0개)
```

## 필수 읽기 (순서대로)

1. **`CLAUDE.md`** — 프로젝트 규칙 (파싱 실패 시 safe 금지, sqlglot 외 dep 금지 등)
2. **`docs/architecture-decisions.md`** — 왜 이렇게 설계했는지 (ADR 8개)
3. **`docs/implementation-plan.md`** — Task 1-17 구현 계획 (코드 포함)
4. **`docs/design-spec.md`** — 전체 설계 스펙

## 구현 순서 (Task 1부터)

| 순서 | Task | 산출물 | 설명 |
|------|------|--------|------|
| **→ 1** | **테스트 fixture** | `tests/fixtures/*.sql` (3개) | compiled SQL 샘플 |
| 2 | extract_columns 테스트 (red) | `tests/test_columns.py` | 7개 테스트 |
| 3 | extract_columns 구현 (green) | `src/dbt_plan/columns.py` | SQLGlot 파싱 |
| 4 | predict_ddl 테스트 (red) | `tests/test_predictor.py` | 8개 테스트 |
| 5 | predict_ddl 구현 (green) | `src/dbt_plan/predictor.py` | DDL 규칙 |
| 6 | find_downstream 테스트 + 구현 | `src/dbt_plan/manifest.py` | child_map 워킹 |
| 7 | diff_compiled_dirs 테스트 + 구현 | `src/dbt_plan/diff.py` | 디렉토리 비교 |
| 8 | manifest + formatters + CLI | 나머지 모듈 | 통합 |
| 9 | 통합 테스트 | E2E 테스트 | 전체 파이프라인 |
| 10-17 | 문서 + CI + 릴리즈 | README 보강, workflows, Issues | OSS 완성 |

## TDD 사이클

```
각 모듈마다:
1. 테스트 작성 (red) → pytest 실패 확인
2. 구현 (green) → pytest 성공 확인
3. 커밋 (conventional commits: feat:, test:, docs:)
```

## 핵심 설계 원칙 (반드시 준수)

1. **파싱 실패 ≠ safe** — `extract_columns()`가 None 반환하면 호출자가 ⚠️ review로 처리
2. **SELECT * → `["*"]` 반환** — INFORMATION_SCHEMA fallback (Phase 1b)
3. **환경변수로 설정** — 프로젝트별 하드코딩 없음
4. **sqlglot 외 런타임 의존성 추가 금지**
5. **Snowflake dialect만 지원** (현재)

## 관련 프로젝트 (참고용)

- `ab180/airflux_dbt` (mesa-verde) — dbt-plan의 첫 번째 소비자 프로젝트
  - `airflux/scripts/dbt_risk_check.py` — 기존 v1 (manifest-only 체크, dbt-plan 통합 후 제거 예정)
  - `.github/workflows/dbt-pr-check.yml` — 기존 CI (dbt-plan 통합 후 교체 예정)
  - `airflux/target/compiled/` — 실제 compiled SQL 파일 (SQLGlot 테스트에 사용)

## Notion 티켓

메인 PRD: [dbt-plan DDL Impact Predictor](https://www.notion.so/336a69a82507814b99f3ebe6e0297aaa)
전체 티켓 11개가 Airflux Engineering > Tickets 하위에 있음.
