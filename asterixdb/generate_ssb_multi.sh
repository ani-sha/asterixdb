#!/usr/bin/env bash
# ================================================================
# Generate SSB data for SF = 10, 30, 50, 100, 300
# Split each SF into 5 chunks (directories) using -C 5 and -S 1..5
# Output root: /scratch/sbb-data/
# Generator dir: /scratch/ssb-kylin/ssb-benchmark
# ================================================================

set -euo pipefail

# ---- FIXED PATHS ----
OUTPUT_ROOT="/scratch/sbb-data"
DBGEN_DIR="/scratch/ssb-kylin/ssb-benchmark"
PARALLEL_CHUNKS=5
SCALE_FACTORS=(10 30 50 100 300)

# Locate the generator binary
if [[ -x "${DBGEN_DIR}/ssb-dbgen" ]]; then
  DBGEN_BIN="${DBGEN_DIR}/ssb-dbgen"
elif [[ -x "${DBGEN_DIR}/dbgen" ]]; then
  DBGEN_BIN="${DBGEN_DIR}/dbgen"
else
  echo "ERROR: Could not find an executable 'ssb-dbgen' or 'dbgen' in ${DBGEN_DIR}"
  echo "       Build it first (e.g., 'make' in ${DBGEN_DIR}) or adjust the path."
  exit 1
fi

mkdir -p "$OUTPUT_ROOT"

echo "=== SSB generation ==="
echo "Output root : $OUTPUT_ROOT"
echo "Generator   : $DBGEN_BIN"
echo "Chunks/SF   : $PARALLEL_CHUNKS"
echo "Scale facts : ${SCALE_FACTORS[*]}"
echo

for SF in "${SCALE_FACTORS[@]}"; do
  SF_DIR="${OUTPUT_ROOT}/SF${SF}"
  echo "---- SF=${SF} -> ${SF_DIR} ----"
  mkdir -p "$SF_DIR"

  # (Re)create 5 chunk directories
  for i in $(seq 1 $PARALLEL_CHUNKS); do
    PART_DIR="${SF_DIR}/ssb_part${i}"
    rm -rf "$PART_DIR"
    mkdir -p "$PART_DIR"

    # Symlink the dictionary file so dbgen can find ./dists.dss
    ln -sf "${DBGEN_DIR}/dists.dss" "${PART_DIR}/dists.dss"
  done

  # Generate chunks in parallel for this SF
  pids=()
  for i in $(seq 1 $PARALLEL_CHUNKS); do
    PART_DIR="${SF_DIR}/ssb_part${i}"
    cd "$PART_DIR"
    SEED=$((1000 + SF*10 + i))
    echo ">>> SF ${SF} | chunk ${i}/${PARALLEL_CHUNKS} | dir: ${PART_DIR}"
    "$DBGEN_BIN" -s "$SF" -C "$PARALLEL_CHUNKS" -S "$i" -r "$SEED" -f > /dev/null
    echo "<<< done: SF ${SF} chunk ${i}"
  done

  # Wait for all chunks of this SF
  for pid in "${pids[@]}"; do wait "$pid"; done
  echo "OK: SF ${SF} completed."
  echo
done

echo "=== All SSB generations finished ==="
echo "Directory layout under $OUTPUT_ROOT:"
for SF in "${SCALE_FACTORS[@]}"; do
  echo "  - $OUTPUT_ROOT/SF${SF}/ssb_part{1..5}"
done
