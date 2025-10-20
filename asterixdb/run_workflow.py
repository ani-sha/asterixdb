import subprocess
import argparse
import os

# AsterixDB endpoint
ASTERIX_URL = "http://localhost:19002/query/service"

# Base SmartRabbit command
SMART_RABBIT_CMD = "python3 SmartRabbitExecutor.py"

# Query and index workflow
WORKFLOW = [
    # (query, create_stmt, drop_stmt)
    ("q3", None, None),
    ("q10", None, None),

    ("q1",
     "CREATE INDEX l_returnflag_idx ON Lineitem_{sf}(l_returnflag)",
     "DROP INDEX Lineitem_{sf}.l_returnflag_idx"),

    ("q4",
     "CREATE INDEX o_orderpriority_idx ON Orders_{sf}(o_orderpriority)",
     "DROP INDEX Orders_{sf}.o_orderpriority_idx"),

    ("q9", None, None),

    ("q12",
     "CREATE INDEX l_shipmode_idx ON Lineitem_{sf}(l_shipmode)",
     "DROP INDEX Lineitem_{sf}.l_shipmode_idx"),

    ("q8",
     "CREATE INDEX o_orderdate_idx ON Orders_{sf}(o_orderdate)",
     "DROP INDEX Orders_{sf}.o_orderdate_idx"),

    ("q18",
     "CREATE INDEX o_totalprice_idx ON Orders_{sf}(o_totalprice)",
     "DROP INDEX Orders_{sf}.o_totalprice_idx"),

    ("q16",
     "CREATE INDEX p_brand_idx ON Part_{sf}(p_brand)",
     "DROP INDEX Part_{sf}.p_brand_idx"),
]


def run_cmd(cmd):
    print(f"▶ Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"❌ Command failed: {cmd}")
        exit(1)
def fsync_system():
    run_cmd("sync")

def clear_os_cache():
    """
    Drop the Linux page cache, dentries, and inodes to force cold-cache I/O.
    Requires sudo privileges.
    """
    print("⚙️  Dropping OS caches...")
    run_cmd("sync")  # flush dirty pages
    run_cmd("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'")

def run_curl(stmt):
    curl_cmd = f"""curl -s -X POST {ASTERIX_URL} \\
      -H 'Content-Type: application/json' \\
      -d '{{"statement": "{stmt}"}}'"""
    run_cmd(curl_cmd)

def run_workflow(sfs, runs, queries, number_of_nodes=204, interactive_only=False, blocking_only=False):
    # Convert queries list to a set for quick lookup (if provided)
    queries_to_run = set(queries) if queries else None

    for sf in sfs:
        print(f"\n===== Running Workflow for SF={sf}, runs={runs}, interactive_only={interactive_only}, blocking_only={blocking_only}  =====\n")
        for query, create_stmt, drop_stmt in WORKFLOW:
            # Skip if --queries was specified and this query isn't in it
            if queries_to_run and query not in queries_to_run:
                continue

            # If query needs an index, create first
            if create_stmt:
                stmt = create_stmt.format(sf=sf)
                run_curl(stmt)

            # Blocking run (skip when interactive_only)
            if not interactive_only:
                for r in range(1, runs + 1):
                    print(f"\n--- Blocking Run {r}/{runs} for {query} ---")
                    run_cmd(
                        f"{SMART_RABBIT_CMD} --runs 1 "
                        f"--query {query} --sf {sf} "
                        f"--mode blocking_single --number-of-nodes {number_of_nodes}"
                    )
                    fsync_system()
                    clear_os_cache()

                        # --- Interactive runs ---
            if not blocking_only:
                for r in range(1, runs + 1):
                    print(f"\n--- Interactive Run {r}/{runs} for {query} ---")
                    run_cmd(
                        f"{SMART_RABBIT_CMD} --runs 1 "
                        f"--query {query} --sf {sf} "
                        f"--strategy dynamic --number-of-nodes {number_of_nodes}"
                    )
                    fsync_system()
                    clear_os_cache()

            # If query had index, drop afterwards
            if drop_stmt:
                stmt = drop_stmt.format(sf=sf)
                run_curl(stmt)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SmartRabbit queries + index workflow across scale factors")
    parser.add_argument("--sfs", nargs="+", type=int, default=[10], help="Scale factors to run (e.g., --sfs 10 30 100)")
    parser.add_argument("--runs", type=int, default=5, help="Number of runs per query")
    parser.add_argument("--queries", nargs="+", help="Queries to run (e.g., --queries q3 q10 q1)")
    parser.add_argument("--nodes", type=int, default=204, help="Number of nodes (default 204)")
    parser.add_argument("--interactive-only", action="store_true",
                        help="If set, skip blocking runs and execute only interactive (dynamic) runs")
    parser.add_argument("--blocking-only", action="store_true",
                            help="If set, skip interactive runs and execute only blocking (single) runs")
    args = parser.parse_args()

    run_workflow(args.sfs, args.runs, args.queries, args.nodes, args.interactive_only, args.blocking_only)
