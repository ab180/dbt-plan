# dbt PR Safety Check — 리서치 진행 상황

## 상태: 모든 검증 가능 항목 완료

## 검증 완료 (14개, 모두 실제 테스트/코드 확인)

| # | 항목 | 방법 | 결과 | 신뢰도 |
|---|------|------|------|--------|
| V1 | SQLGlot 파싱 | 128개 실제 compiled SQL 실행 | 100% 성공 | 높음 |
| V2 | SQLGlot 의존성 | pip show 실행 | zero-dep, 2.8MB | 높음 |
| V3 | 파일명→manifest | manifest.json 실제 조회 | 99.2% (125/126) | 높음 |
| V4 | INFORMATION_SCHEMA | Snowflake 문서 + 기존 dbt test | ~$0.001, sub-second | 중간 (실행 안 함) |
| V5 | dbt 실행 방식 | snowflake/task/ 17개 파일 확인 | Snowflake Tasks | 높음 |
| V6 | on-run-start | dbt 소스코드 직접 확인 | SQL only | 높음 |
| V7 | catalog.json | 논리적 확인 | base/current 동일 | 중간 |
| V8 | Recce OSS | 소스코드 분석 | PR 코멘트 없음, 29 pkgs | 중간 |
| V9 | compiled SQL 구조 | 실제 파일 읽기 | 순수 SELECT (MERGE 아님) | 높음 |
| V10 | SELECT * 카운트 | 228개 파일 전수 검사 | **49개** (모델34+테스트13+EXCLUDE2) | 높음 |
| V11 | FLATTEN↔sync overlap | dbt_project.yml + 모델 config | 1개 (fct_sdk_v1_event_validation) | 높음 |
| V12 | sync_all_columns 정확한 목록 | dbt_project.yml + 모든 모델 grep | **14개** 확정, 모두 model-level override | 높음 |
| V13 | sync↔SELECT * overlap | 교차 확인 | **6개 이상** SELECT * 사용 | 높음 |
| V14 | SQLGlot 타입 추출 | 1845개 컬럼 실제 테스트 | **5.2%만 추출 가능**, 실용적이지 않음 | 높음 |

## 이전 리서치 오류 수정

| 이전 주장 | 수정된 값 | 영향 |
|----------|----------|------|
| SELECT * 36개 | **49개** | INFORMATION_SCHEMA 의존도 증가 |
| INFORMATION_SCHEMA는 optional fallback | **sync_all_columns에 필수** | 설계 변경 |
| 컬럼 타입 변경 Phase 2 | Phase 2 확정 (5.2%만 추출 가능) | 변경 없음 |

## 남은 gap (구현 시에만 검증 가능)

| # | 항목 | 위험도 | 이유 |
|---|------|--------|------|
| G1 | INFORMATION_SCHEMA 실제 쿼리 | MEDIUM | Snowflake 접근 필요 |
| G2 | CI 환경 E2E | MEDIUM | self-hosted runner 필요 |
| G3 | Slack webhook 패턴 | LOW | Phase 2, 사용자 확인 |

**G1-G3은 로컬에서 검증 불가. 구현 시 Snowflake/CI 환경에서 검증.**
