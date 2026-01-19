#!/usr/bin/env python3
import subprocess
import json
import sys

ASTERIX_URL = "http://localhost:19002/query/service"

# -------------------------------------------------------------------
# Build MinCost INSERT query for a given scale factor
# -------------------------------------------------------------------
def make_mincost_insert(sf):
    return (
        f"INSERT INTO MinCost_{sf} "
        f"SELECT ps.ps_partkey AS ps_partkey, "
        f"MIN(ps.ps_supplycost) AS min_cost "
        f"FROM Partsupp_{sf} ps "
        f"JOIN Supplier_{sf} s ON ps.ps_suppkey = s.s_suppkey "
        f"JOIN Nation_{sf} n   ON s.s_nationkey = n.n_nationkey "
        f"JOIN Region_{sf} r   ON n.n_regionkey = r.r_regionkey "
        f"WHERE r.r_name = 'EUROPE' "
        f"GROUP BY ps.ps_partkey;"
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
# Create MinCost table for SF
# -------------------------------------------------------------------
def create_min_cost(sf):
    print(f"\n=== Creating MinCost_{sf} ===")

    # Drop dataset and type if they exist (ignore errors)
    run(f"DROP DATASET MinCost_{sf} IF EXISTS;")
    run(f"DROP TYPE MinCost_{sf}_Type IF EXISTS;")

    # Create type
    ddl_type = (
        f"CREATE TYPE MinCost_{sf}_Type AS {{ "
        f"  ps_partkey: int32, "
        f"  min_cost: double "
        f"}};"
    )
    print(run(ddl_type))

    # Create dataset
    ddl_dataset = (
        f"CREATE DATASET MinCost_{sf}(MinCost_{sf}_Type) "
        f"PRIMARY KEY ps_partkey;"
    )
    print(run(ddl_dataset))

    # Insert MinCost values
    insert_stmt = make_mincost_insert(sf)
    print(run(insert_stmt))


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: create_mincost.py <SF> [SF2 SF3 ...]")
        sys.exit(1)

    for arg in sys.argv[1:]:
        try:
            sf = int(arg)
            create_min_cost(sf)
        except ValueError:
            print(f"Invalid scale factor: {arg}")

    print("\nDone.")
