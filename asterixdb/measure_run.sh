#!/usr/bin/env bash
# save as measure_multi.sh
set -euo pipefail
PAT=${1:?process-name-regex}     # e.g. 'NCService|hyracks|asterix'
OUT=${2:?out-prefix}
shift 2

OUTDIR="$(dirname "$OUT")"; mkdir -p "$OUTDIR"
HERTZ=$(getconf CLK_TCK)

# discover PIDs matching regex (command line)
discover_pids() {
  ps -e -o pid= -o cmd= | egrep -i "$PAT" | awk '{print $1}'
}

# sum utime+stime over all threads of a PID
pid_ticks() { awk '{u+=$14; s+=$15} END{printf("%.0f\n",u+s)}' /proc/"$1"/task/*/stat 2>/dev/null || echo 0; }

# sum over all matching PIDs
sum_ticks_all() {
  local sum=0
  for p in $(discover_pids); do
    t=$(pid_ticks "$p"); sum=$((sum + t))
  done
  echo "$sum"
}

# background sampler for total live threads (sum Threads: across PIDs)
( while :; do
    PIDS=($(discover_pids))
    [[ ${#PIDS[@]} -eq 0 ]] && { sleep 1; continue; }
    ts=$(date +%s)
    tot=0
    for p in "${PIDS[@]}"; do
      thr=$(awk '/^Threads:/ {print $2}' /proc/"$p"/status 2>/dev/null || echo 0)
      tot=$((tot + thr))
    done
    echo -e "${ts}\t${tot}"
    sleep 1
  done ) > "${OUT}.threads.txt" &
THR_PID=$!

START_MS=$(date +%s%3N)
TICKS_START=$(sum_ticks_all)

# run your client command (python driver etc.)
"$@" | tee "${OUT}.client.log" >/dev/null || true

END_MS=$(date +%s%3N)
TICKS_END=$(sum_ticks_all)
kill "$THR_PID" 2>/dev/null || true

WALL_MS=$((END_MS - START_MS))
CPU_TICKS=$((TICKS_END - TICKS_START)); [[ $CPU_TICKS -lt 0 ]] && CPU_TICKS=0

CPU_SEC=$(python3 - <<PY
ticks=${CPU_TICKS}; hz=${HERTZ}
print(f"{ticks/float(hz):.6f}")
PY
)

AVG_CONC=$(python3 - <<PY
cpu=${CPU_SEC}; wall=${WALL_MS}/1000.0
print(f"{(cpu/wall) if wall>0 else 0.0:.2f}")
PY
)

PEAK_THREADS=$(awk 'max<$2{max=$2}END{print (max?max:0)}' "${OUT}.threads.txt")

printf "WALL(s)=%.3f  CPU-seconds=%.3f  AvgConcurrency≈%s\n" "$(awk "BEGIN{print ${WALL_MS}/1000.0}")" "$(awk "BEGIN{print ${CPU_SEC}}")" "${AVG_CONC}"
echo "PeakThreads=${PEAK_THREADS}"
