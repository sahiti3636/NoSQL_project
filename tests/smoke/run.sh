#!/usr/bin/env bash
# Smoke test: Hive and MongoDB must parse ~2,000 real log lines (including every odd one)
# exactly like the Python reference, using only the files in contracts/.
# Needs: docker compose up -d --wait, and the two logs in data/.
set -euo pipefail
cd "$(dirname "$0")/../.."
python3 -I tests/smoke/make_sample.py "$PWD"
python3 -I tests/smoke/make_hive_sql.py "$PWD" > runs/regex_test.hql
rm -rf runs/regex_test_hive
docker compose exec -T hive beeline -u jdbc:hive2://localhost:10000/ --silent=true -f /runs/regex_test.hql > runs/hive_log.txt 2>&1 \
  || { echo "Hive failed, see runs/hive_log.txt"; exit 1; }
docker compose exec -T mongodb mongosh --quiet --file /dev/stdin < tests/smoke/regex_test_mongo.js > runs/regex_test_mongo.jsonl
python3 -I tests/smoke/compare.py "$PWD" && echo "SMOKE TEST PASSED"
