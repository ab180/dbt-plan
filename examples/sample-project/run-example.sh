#!/bin/bash
# dbt-plan 체험용 예제
# Snowflake 접속 없이 로컬에서 바로 실행할 수 있습니다.
#
# 사용법:
#   pip install git+https://github.com/ab180/dbt-plan@v0.1.0
#   cd examples/sample-project
#   bash run-example.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== dbt-plan example ==="
echo ""
echo "시나리오:"
echo "  - int_unified: data__device, data__user 삭제 → data__device__uuid, data__user__id 추가 (DESTRUCTIVE)"
echo "  - fct_daily_metrics: total_revenue 컬럼 추가 (SAFE)"
echo "  - dim_device: platform 컬럼 추가 (SAFE, table = CREATE OR REPLACE)"
echo "  - dim_campaign: 새 모델 추가 (SAFE)"
echo ""

echo "--- Text output ---"
echo ""
dbt-plan check \
  --base-dir "$SCRIPT_DIR/base" \
  --project-dir "$SCRIPT_DIR/current" \
  --format text || true

echo ""
echo "--- GitHub markdown output ---"
echo ""
dbt-plan check \
  --base-dir "$SCRIPT_DIR/base" \
  --project-dir "$SCRIPT_DIR/current" \
  --format github || true

echo ""
echo "--- Exit code check ---"
set +e
dbt-plan check \
  --base-dir "$SCRIPT_DIR/base" \
  --project-dir "$SCRIPT_DIR/current" \
  > /dev/null 2>&1
EXIT_CODE=$?
set -e
echo "exit code: $EXIT_CODE (0=safe, 1=destructive, 2=error)"
echo ""
echo "이 예제에서는 int_unified에 DROP COLUMN이 있으므로 exit 1 (destructive) 입니다."
