#!/usr/bin/env bash
# Benchmark the four engines at several batch sizes, and without batching, on ONE machine.
#
#   ./scripts/run_benchmarks.sh                    3 rounds x {100k, 500k, 1M, none} x 4 engines
#   ROUNDS=1 SIZES="500000 none" PIPELINES="pig mapreduce" ./scripts/run_benchmarks.sh
#
# SIZES lists batch sizes in lines; "none" runs without batching (the whole input is one batch).
# Every run goes through the normal controller, so its runtime, batch count and results are
# stored in PostgreSQL like any other run. Rounds are interleaved (size by size, engine by
# engine) so that slow drift on the laptop affects every configuration equally. A run that
# fails is logged and the benchmark moves on.
# Logs: runs/benchmarks/<timestamp>/ (runs/ is git-ignored).
set -uo pipefail
cd "$(dirname "$0")/.."

ROUNDS="${ROUNDS:-3}"
SIZES="${SIZES:-100000 500000 1000000 none}"
PIPELINES="${PIPELINES:-mongodb hive mapreduce pig}"
PY="${PY:-.venv/bin/python}"
OUT="runs/benchmarks/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"

echo "benchmark started $(date '+%F %T')  rounds=$ROUNDS  sizes=$SIZES  pipelines=$PIPELINES" | tee "$OUT/summary.log"
for round in $(seq 1 "$ROUNDS"); do
  for size in $SIZES; do
    for pipeline in $PIPELINES; do
      log="$OUT/r${round}_${pipeline}_${size}.log"
      if [ "$size" = "none" ]; then batching=(--no-batch); else batching=(--batch-size "$size"); fi
      if "$PY" -m etl run --pipeline "$pipeline" "${batching[@]}" > "$log" 2>&1; then
        result="$(grep -E '^\s+runtime' "$log" | awk '{print $2, $3}')"
      else
        result="FAILED (see $log)"
      fi
      echo "round $round  $pipeline  $size  $result  $(head -1 "$log" | awk '{print $2}')" | tee -a "$OUT/summary.log"
    done
  done
done
echo "benchmark finished $(date '+%F %T')" | tee -a "$OUT/summary.log"
