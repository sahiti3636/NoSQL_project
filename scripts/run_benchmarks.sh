#!/usr/bin/env bash
# Benchmark both Phase 1 engines at several batch sizes, on ONE machine.
#
#   ./scripts/run_benchmarks.sh                       3 rounds x {100k, 500k, 1M} x {mongodb, hive}
#   ROUNDS=1 SIZES="500000" ./scripts/run_benchmarks.sh
#
# Every run goes through the normal controller, so its runtime, batch count and results are
# stored in PostgreSQL like any other run. Rounds are interleaved (size by size, engine by
# engine) so that slow drift on the laptop affects every configuration equally.
# Logs: runs/benchmarks/<timestamp>/ (runs/ is git-ignored).
set -euo pipefail
cd "$(dirname "$0")/.."

ROUNDS="${ROUNDS:-3}"
SIZES="${SIZES:-100000 500000 1000000}"
PIPELINES="${PIPELINES:-mongodb hive}"
PY="${PY:-.venv/bin/python}"
OUT="runs/benchmarks/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"

echo "benchmark started $(date '+%F %T')  rounds=$ROUNDS  sizes=$SIZES  pipelines=$PIPELINES" | tee "$OUT/summary.log"
for round in $(seq 1 "$ROUNDS"); do
  for size in $SIZES; do
    for pipeline in $PIPELINES; do
      log="$OUT/r${round}_${pipeline}_${size}.log"
      "$PY" -m etl run --pipeline "$pipeline" --batch-size "$size" > "$log" 2>&1
      echo "round $round  $pipeline  $size  $(grep -E '^\s+runtime' "$log" | awk '{print $2, $3}')  $(head -1 "$log" | awk '{print $2}')" | tee -a "$OUT/summary.log"
    done
  done
done
echo "benchmark finished $(date '+%F %T')" | tee -a "$OUT/summary.log"
