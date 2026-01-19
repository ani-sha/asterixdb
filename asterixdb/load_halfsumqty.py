#!/usr/bin/env python3
import subprocess
import json
import sys

ASTERIX_URL = "http://localhost:19002/query/service"

# -------------------------------------------------------------------
# Build HalfSumQty INSERT query for a given scale factor
# -------------------------------------------------------------------
def make_halfsumqty_insert(sf):
    return (
        f"INSERT INTO HalfSumQty_{sf} "
        f"SELECT l.l_partkey AS l_partkey, "
        f"       l.l_suppkey AS l_suppkey, "
        f"       0.5 * SUM(l.l_quantity) AS half_sum_qty "
        f"FROM Lineitem_{sf} l "
        f"WHERE l.l_shipdate >= date(\"1994-01-01\") "
        f"  AND l.l_shipdate <  date(\"1995-01-01\") "
        f"GROUP BY l.l_partkey, l.l_suppkey;"
    )

# -------------------------------------------------------------------
# Run a curl request
# -------------------------------------------------------------------
def run(statement):
    payload = json.dumps({"statement": statement})
    result = subprocess.run(
        ["curl", "-s", "-X", "POST", ASTERIX_URL,
         "-H", "Content-Type: application/json",
         "-d", payload],
        capture_output=True, text=True
    )
    return result.stdout.strip()

# -------------------------------------------------------------------
# Create HalfSumQty table for SF
# -------------------------------------------------------------------
def create_halfsumqty(sf):
    print(f"\n=== Creating HalfSumQty_{sf} ===")

    # Drop dataset and type if they exist (ignore errors)
    run(f"DROP DATASET HalfSumQty_{sf} IF EXISTS;")
    run(f"DROP TYPE HalfSumQty_{sf}_Type IF EXISTS;")

    # Create type
    ddl_type = (
        f"CREATE TYPE HalfSumQty_{sf}_Type AS {{ "
        f"  l_partkey: int32, "
        f"  l_suppkey: int32, "
        f"  half_sum_qty: double "
        f"}};"
    )
    print(run(ddl_type))

    # Create dataset
    ddl_dataset = (
        f"CREATE DATASET HalfSumQty_{sf}(HalfSumQty_{sf}_Type) "
        f"PRIMARY KEY l_partkey, l_suppkey;"
    )
    print(run(ddl_dataset))

    # Insert HalfSumQty values
    insert_stmt = make_halfsumqty_insert(sf)
    print(run(insert_stmt))


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: create_halfsumqty.py <SF> [SF2 SF3 ...]")
        sys.exit(1)

    for arg in sys.argv[1:]:
        try:
            sf = int(arg)
            create_halfsumqty(sf)
        except ValueError:
            print(f"Invalid scale factor: {arg}")

    print("\nDone.")
