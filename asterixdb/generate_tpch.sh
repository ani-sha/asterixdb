#!/usr/bin/env bash
# generate_tpch.sh
set -euo pipefail

DBGEN="/scratch/pratyoyd/dbgen"          # <- your dbgen binary
OUTBASE="/scratch/tpch_data"             # change if you want a different root
SCALES=(1 5 10 30 50 100 300)

# sanity check
[[ -x "$DBGEN" ]] || { echo "dbgen not found/executable at $DBGEN"; exit 1; }

mkdir -p "$OUTBASE"

for SF in "${SCALES[@]}"; do
  OUTDIR="${OUTBASE}/sf${SF}"
  echo "=== Generating TPC-H SF=${SF} -> ${OUTDIR}"
  mkdir -p "$OUTDIR"
  (
    cd "$OUTDIR"
    "$DBGEN" -s "$SF" -f
    # optional: compress to save space (uncomment if desired)
    # gzip -f *.tbl
  )
done

echo "Done. Datasets at: ${OUTBASE}/sf{1,5,10,30,50,100,300}"
