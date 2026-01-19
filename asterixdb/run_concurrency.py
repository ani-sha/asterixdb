#!/usr/bin/env python3
import asyncio
import aiohttp
import argparse
import time
import math
import json
import random
import os

def cleanup_hybrid_execution(signal_dir):
    del_dir = os.path.join(signal_dir, "HybridExecution")
    if not os.path.isdir(del_dir):
        return

    delete_list = [
        "B2ISignal",
        "I2BSignal",
        "InteractiveAnswers",
        "InteractiveAnswersAll",
        "output_blocking.json",
        "output_interactive.json",
        "GroupBarriers"
    ]

    for name in delete_list:
        path = os.path.join(del_dir, name)
        print(f"Checking {path}...", end="")
        if os.path.exists(path):
            try:
                shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)
                print(" deleted")
            except Exception as e:
                print(f" failed: {e}")
        else:
            print(" not found")

# ============================================================
# LONG QUERIES FOR Q9 (parameterized by scale factor)
# ============================================================

def interactive_query_q9_dynamic(sf):
    return (
        'SET `compiler.interactive.mode` "true"; '
        f'SELECT o.o_orderkey, n.n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue '
        f'FROM Orders_{sf} AS o '
        f'JOIN Lineitem_{sf} AS l ON o.o_orderkey /*+ indexnl */ = l.l_orderkey '
        f'JOIN Part_{sf} AS p ON l.l_partkey /*+ indexnl */ = p.p_partkey '
        f'JOIN Supplier_{sf} AS s ON l.l_suppkey /*+ indexnl */ = s.s_suppkey '
        f'JOIN Nation_{sf} AS n ON s.s_nationkey /*+ indexnl */ = n.n_nationkey '
        'WHERE p.p_name LIKE "%green%" '
        'AND o.o_orderdate >= "1993-07-01" '
        'AND o.o_orderdate < "1993-10-01" '
        'GROUP BY o.o_orderkey, n.n_name;'
    )


def blocking_query_q9(sf):
    return (
        f"SELECT o.o_orderkey, n.n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue "
        f"FROM Lineitem_{sf} AS l, Orders_{sf} AS o, Part_{sf} AS p, Supplier_{sf} AS s, Nation_{sf} AS n "
        f"WHERE l.l_orderkey = o.o_orderkey "
        f"AND l.l_partkey = p.p_partkey "
        f"AND l.l_suppkey = s.s_suppkey "
        f"AND s.s_nationkey = n.n_nationkey "
        f"AND p.p_name LIKE \"%green%\" "
        f"AND o.o_orderdate >= \"1993-07-01\" "
        f"AND o.o_orderdate < \"1993-10-01\" "
        f"GROUP BY o.o_orderkey, n.n_name;"
    )


def blocking_query_q9_dynamic(sf):
    return (
        "SET `compiler.blocking.mode` `true`; "
        "SELECT a.* FROM ( "
        f"SELECT o.o_orderkey, n.n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue "
        f"FROM Lineitem_{sf} AS l, Orders_{sf} AS o, Part_{sf} AS p, Supplier_{sf} AS s, Nation_{sf} AS n "
        f"WHERE l.l_orderkey = o.o_orderkey "
        f"AND l.l_partkey = p.p_partkey "
        f"AND l.l_suppkey = s.s_suppkey "
        f"AND s.s_nationkey = n.n_nationkey "
        f"AND p.p_name LIKE \"%green%\" "
        f"AND o.o_orderdate >= \"1993-07-01\" "
        f"AND o.o_orderdate < \"1993-10-01\" "
        f"GROUP BY o.o_orderkey, n.n_name "
        ") a WHERE a.o_orderkey > 10000;"
    )


# ============================================================
# SHORT QUERIES (10 options)
# ============================================================

def make_short_query(sf):
    choice = random.randint(1, 10)

    if choice == 1:
        ck = random.randint(1, 100000 * sf)
        q = f"SELECT c_name, c_acctbal FROM Customer_{sf} WHERE c_custkey = {ck};"
        return q, "S1"

    if choice == 2:
        ok = random.randint(1, 300000 * sf)
        q = f"SELECT o_orderstatus, o_totalprice FROM Orders_{sf} WHERE o_orderkey = {ok};"
        return q, "S2"

    if choice == 3:
        ok = random.randint(1, 300000 * sf)
        ln = random.randint(1, 7)
        q = f"SELECT l_extendedprice, l_discount FROM Lineitem_{sf} WHERE l_orderkey = {ok} AND l_linenumber = {ln};"
        return q, "S3"

    if choice == 4:
        sk = random.randint(1, 10000 * sf)
        q = f"SELECT s_name, s_acctbal FROM Supplier_{sf} WHERE s_suppkey = {sk};"
        return q, "S4"

    if choice == 5:
        nk = random.randint(0, 24)
        q = f"SELECT n_name FROM Nation_{sf} WHERE n_nationkey = {nk};"
        return q, "S5"

    if choice == 6:
        start = random.randint(1, 300000 * sf)
        end   = start + random.randint(10, 200)
        q = f"SELECT o_orderkey, o_orderdate FROM Orders_{sf} WHERE o_orderkey BETWEEN {start} AND {end} LIMIT 50;"
        return q, "S6"

    if choice == 7:
        start = random.randint(1, 100000 * sf)
        end   = start + random.randint(10, 200)
        q = f"SELECT c_custkey, c_name FROM Customer_{sf} WHERE c_custkey BETWEEN {start} AND {end} LIMIT 50;"
        return q, "S7"

    if choice == 8:
        q = f"SELECT COUNT(*) FROM Orders_{sf} WHERE o_orderstatus = 'F';"
        return q, "S8"

    if choice == 9:
        start = random.randint(1, 300000 * sf)
        end   = start + random.randint(10, 200)
        q = f"SELECT SUM(l_quantity) FROM Lineitem_{sf} WHERE l_orderkey BETWEEN {start} AND {end};"
        return q, "S9"

    low  = random.randint(900, 1500)
    high = low + random.randint(10, 200)
    q = f"SELECT p_name, p_retailprice FROM Part_{sf} WHERE p_retailprice BETWEEN {low} AND {high} LIMIT 20;"
    return q, "S10"


# ============================================================
# Helpers
# ============================================================

def percentile(values, p):
    if not values:
        return None
    data = sorted(values)
    k = (len(data) - 1) * (p / 100)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return data[f]
    return data[f] * (c - k) + data[c] * (k - f)

def summarize(latencies):
    if not latencies:
        return {"count": 0, "avg_ms": None, "p50_ms": None, "p90_ms": None, "p99_ms": None}
    avg = sum(latencies) / len(latencies)
    return {
        "count": len(latencies),
        "avg_ms": avg * 1000,
        "p50_ms": percentile(latencies, 50) * 1000,
        "p90_ms": percentile(latencies, 90) * 1000,
        "p99_ms": percentile(latencies, 99) * 1000,
    }

# ============================================================
# Asterix Query Runner
# ============================================================

async def run_query(session, base_url, stmt):
    start = time.monotonic()
    try:
        async with session.post(base_url, json={"statement": stmt}) as resp:
            _ = await resp.text()
            ok = (resp.status == 200)
    except Exception:
        ok = False
    end = time.monotonic()
    return end - start, ok


# ============================================================
# Client Behaviors
# ============================================================

async def short_client(session, base_url, end_time, lat_list, err_list, sf):
    while time.monotonic() < end_time:
        q, _ = make_short_query(sf)
        latency, ok = await run_query(session, base_url, q)
        lat_list.append(latency)
        if not ok:
            err_list.append(1)


async def long_client(session, base_url, end_time, lat_list, err_list, sf, mode):
    while time.monotonic() < end_time:

        if mode == "hybrid":
            cleanup_hybrid_execution("/scratch/asterixdb_eightynode/asterixdb_eightnode/results")
            # Run BOTH dynamic-blocking + dynamic-interactive
            bq = blocking_query_q9_dynamic(sf)
            iq = interactive_query_q9_dynamic(sf)

            # concurrent
            tasks = [
                asyncio.create_task(run_query(session, base_url, bq)),
                asyncio.create_task(run_query(session, base_url, iq))
            ]
            results = await asyncio.gather(*tasks)

            for latency, ok in results:
                lat_list.append(latency)
                if not ok:
                    err_list.append(1)

        else:  # blocking
            q = blocking_query_q9(sf)
            latency, ok = await run_query(session, base_url, q)
            lat_list.append(latency)
            if not ok:
                err_list.append(1)


# ============================================================
# MAIN
# ============================================================

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:19002/query/service")
    parser.add_argument("--sf", type=int, required=True)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--num-short", type=int, default=16)
    parser.add_argument("--num-long", type=int, default=2)
    parser.add_argument("--long-mode", choices=["blocking", "hybrid"], default="blocking")
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    timeout = aiohttp.ClientTimeout(total=None)
    connector = aiohttp.TCPConnector(limit=0)
    session_params = dict(connector=connector, timeout=timeout)

    lat_short = []
    lat_long  = []
    err_short = []
    err_long  = []

    start = time.monotonic()
    end_t = start + args.duration

    async with aiohttp.ClientSession(**session_params) as session:

        tasks = []

        for _ in range(args.num_short):
            tasks.append(
                asyncio.create_task(
                    short_client(session, args.base_url, end_t, lat_short, err_short, args.sf)
                )
            )

        for _ in range(args.num_long):
            tasks.append(
                asyncio.create_task(
                    long_client(session, args.base_url, end_t, lat_long, err_long, args.sf, args.long_mode)
                )
            )

        await asyncio.gather(*tasks, return_exceptions=True)

    total_time = time.monotonic() - start

    result = {
        "config": {
            "sf": args.sf,
            "duration_sec": args.duration,
            "num_short": args.num_short,
            "num_long": args.num_long,
            "long_mode": args.long_mode,
        },
        "short": {
            **summarize(lat_short),
            "errors": len(err_short),
            "throughput_qps": len(lat_short) / total_time,
        },
        "long": {
            **summarize(lat_long),
            "errors": len(err_long),
            "throughput_qps": len(lat_long) / total_time,
        },
        "total_wall_time_sec": total_time,
    }

    text = json.dumps(result, indent=2)
    print(text)

    if args.output:
        with open(args.output, "w") as f:
            f.write(text)


if __name__ == "__main__":
    asyncio.run(main())
