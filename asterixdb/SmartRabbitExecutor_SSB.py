import aiohttp
import asyncio
import argparse
import os
import re
import shutil
import time
from datetime import datetime

# -------------------------------
# Config
# -------------------------------
HEADERS = {"Content-Type": "application/json"}
URL = "http://localhost:19002/query/service"
TIMEOUT = aiohttp.ClientTimeout(total=10000, connect=600, sock_connect=600, sock_read=10000)

# -------------------------------
# Helpers
# -------------------------------
_SSB_TABLES = ["SSB_Date", "SSB_Lineorder", "SSB_Part", "SSB_Supplier", "SSB_Customer"]
_SSB_TABLES_REGEX = re.compile(r"\b(" + "|".join([t.replace('_', r'\_') for t in _SSB_TABLES]) + r")(?:_\d+)?\b")

def _apply_ssb_sf_suffix(sql: str, sf: int | str) -> str:
    sfs = str(sf)
    def repl(m): return f"{m.group(1)}_{sfs}"
    return _SSB_TABLES_REGEX.sub(repl, sql)

def build_payload(statement: str, strategy: str, isInteractive: bool) -> dict:
    payload = {"statement": statement, "profile": "timings"}
    if not (strategy == "dynamic" and isInteractive):
        payload["mode"] = "deferred"
    return payload

def make_filename(base_dir: str, mode: str, part: str, deployment: str, timestamp: str,
                  query_label: str, sf: str, iteration: int, strategy: str,
                  number_of_nodes: str, create_dirs: bool=True) -> str:
    dir_path = os.path.join(base_dir, f"{number_of_nodes}N", f"sf-{sf}", query_label)
    if create_dirs:
        os.makedirs(dir_path, exist_ok=True)
    return os.path.join(dir_path, f"output_{deployment}_{mode}-{strategy}_part-{part}_{timestamp}_run{iteration + 1}.json")

async def run_query(session, payload, filename, delay=0):
    try:
        if delay > 0:
            print(f"Delaying {filename} by {delay}s...")
            await asyncio.sleep(delay)
        async with session.post(URL, headers=HEADERS, json=payload) as response:
            output = await response.text()
            with open(filename, "w") as f:
                f.write(output)
            print(f"Run completed. Output saved to {filename}")
    except asyncio.TimeoutError:
        print(f"Timeout on {filename}")
    except Exception as e:
        print(f"Error for {filename}: {e}")

def resolve_queries(query: str, sf: str, strategy: str, default_blocking_query: str, default_interactive_query: str):
    query_label = f"{query}_{sf}"
    query_map = {
        "SSB_q21": (blocking_query_SSB_q21, interactive_query_SSB_q21, blocking_query_SSB_q21_dynamic, interactive_query_SSB_q21_dynamic),
        "SSB_q22": (blocking_query_SSB_q22, interactive_query_SSB_q22, blocking_query_SSB_q22_dynamic, interactive_query_SSB_q22_dynamic),
        "SSB_q23": (blocking_query_SSB_q23, interactive_query_SSB_q23, blocking_query_SSB_q23_dynamic, interactive_query_SSB_q23_dynamic),
        "SSB_q31": (blocking_query_SSB_q31, interactive_query_SSB_q31, blocking_query_SSB_q31_dynamic, interactive_query_SSB_q31_dynamic),
        "SSB_q32": (blocking_query_SSB_q32, interactive_query_SSB_q32, blocking_query_SSB_q32_dynamic, interactive_query_SSB_q32_dynamic),
        "SSB_q33": (blocking_query_SSB_q33, interactive_query_SSB_q33, blocking_query_SSB_q33_dynamic, interactive_query_SSB_q33_dynamic),
        "SSB_q34": (blocking_query_SSB_q34, interactive_query_SSB_q34, blocking_query_SSB_q34_dynamic, interactive_query_SSB_q34_dynamic),
        "SSB_q41": (blocking_query_SSB_q41, interactive_query_SSB_q41, blocking_query_SSB_q41_dynamic, interactive_query_SSB_q41_dynamic),
        "SSB_q42": (blocking_query_SSB_q42, interactive_query_SSB_q42, blocking_query_SSB_q42_dynamic, interactive_query_SSB_q42_dynamic),
        "SSB_q43": (blocking_query_SSB_q43, interactive_query_SSB_q43, blocking_query_SSB_q43_dynamic, interactive_query_SSB_q43_dynamic),
    }

    if query not in query_map:
        bq, iq = default_blocking_query, default_interactive_query
        bq_dyn, iq_dyn = bq, iq
    else:
        bq, iq, bq_dyn, iq_dyn = query_map[query]

    if strategy == "dynamic":
        blocking, interactive = bq_dyn, iq_dyn
    else:
        blocking, interactive = bq, iq

    blocking = _apply_ssb_sf_suffix(blocking, sf)
    interactive = _apply_ssb_sf_suffix(interactive, sf)
    return blocking, interactive, query_label

# (all SSB query definitions unchanged — omitted for brevity)
# ...
# -------------------------------
# SSB Queries (unsuffixed; we suffix at runtime)
# -------------------------------

# ---- SSB_q21 ----
interactive_query_SSB_q21 = (
    'SET `compiler.interactive.mode` "true"; '
    'SELECT d.d_year, p.p_brand, SUM(l.lo_revenue) AS revenue '
    'FROM SSB_Date d '
    'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
    'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
    'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
    'WHERE d.d_year <= 1995 '
    'AND p.p_category = "MFGR#12" '
    'AND s.s_region = "AMERICA" '
    'GROUP BY d.d_year, p.p_brand;'
)
blocking_query_SSB_q21 = (
    'SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, p.p_brand1 '
    'FROM SSB_Lineorder l, SSB_Date d, SSB_Part p, SSB_Supplier s '
    'WHERE l.lo_orderdate = d.d_datekey '
    'AND d.d_datekey < 19920701 '
    'AND l.lo_partkey = p.p_partkey '
    'AND l.lo_suppkey = s.s_suppkey '
    "AND p.p_category = 'MFGR#12' "
    "AND s.s_region = 'AMERICA' "
    'GROUP BY d.d_datekey, p.p_brand1 '
    'ORDER BY d.d_datekey, p.p_brand1;'
)
blocking_query_SSB_q21_dynamic = (
    "SET `compiler.blocking.mode` `true`; "
    "SELECT a.* FROM( "
    " SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, p.p_brand1 "
    " FROM SSB_Lineorder l, SSB_Date d, SSB_Part p, SSB_Supplier s "
    " WHERE l.lo_orderdate = d.d_datekey "
    " AND l.lo_partkey = p.p_partkey "
    " AND l.lo_suppkey = s.s_suppkey "
    " AND p.p_category = 'MFGR#12' "
    " AND s.s_region = 'AMERICA' "
    " AND d.d_datekey < 19920701 "
    " GROUP BY d.d_datekey, p.p_brand1 "
    " ORDER BY d.d_datekey, p.p_brand1 "
    ") a WHERE a.d_datekey > 19920101;"
)
interactive_query_SSB_q21_dynamic = (
    'SET `compiler.interactive.mode` "true"; '
    'SELECT d.d_datekey, p.p_brand, SUM(l.lo_revenue) AS revenue '
    'FROM SSB_Date d '
    'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
    'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
    'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
    'WHERE d.d_datekey < 19920315 '
    'AND p.p_category = "MFGR#12" '
    'AND s.s_region = "AMERICA" '
    'GROUP BY d.d_datekey, p.p_brand;'
)

# ---- SSB_q22 ----
blocking_query_SSB_q22 = (
    'SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, p.p_brand1 '
    'FROM SSB_Lineorder l, SSB_Date d, SSB_Part p, SSB_Supplier s '
    'WHERE l.lo_orderdate = d.d_datekey '
    'AND d.d_datekey < 19920701 '
    'AND l.lo_partkey = p.p_partkey '
    'AND l.lo_suppkey = s.s_suppkey '
    'AND p.p_brand1 BETWEEN "MFGR#2221" AND "MFGR#2228" '
    'AND s.s_region = "ASIA" '
    'GROUP BY d.d_datekey, p.p_brand1 '
    'ORDER BY d.d_datekey, p.p_brand1;'
)
interactive_query_SSB_q22 = (
    'SET `compiler.interactive.mode` "true"; '
    'SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, p.p_brand1 '
    'FROM SSB_Date d '
    'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
    'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
    'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
    'WHERE d.d_datekey < 19920310 '
    'AND p.p_brand1 BETWEEN "MFGR#22021" AND "MFGR#22028" '
    'AND s.s_region = "ASIA" '
    'GROUP BY d.d_datekey, p.p_brand1;'
)
blocking_query_SSB_q22_dynamic = (
    "SET `compiler.blocking.mode` `true`; "
    "SELECT a.* FROM ( "
    " SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, p.p_brand1 "
    " FROM SSB_Lineorder l, SSB_Date d, SSB_Part p, SSB_Supplier s "
    " WHERE l.lo_orderdate = d.d_datekey "
    " AND l.lo_partkey = p.p_partkey "
    " AND l.lo_suppkey = s.s_suppkey "
    " AND p.p_brand1 BETWEEN 'MFGR#22021' AND 'MFGR#22028' "
    " AND s.s_region = 'ASIA' "
    " AND d.d_yearmonthnum < 199301 "
    " GROUP BY d.d_datekey, p.p_brand1 "
    " ORDER BY d.d_datekey, p.p_brand1 "
    ") a WHERE a.d_datekey > 19920101;"
)
interactive_query_SSB_q22_dynamic = interactive_query_SSB_q22

# ---- SSB_q23 ----
blocking_query_SSB_q23 = (
    'SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, p.p_brand1 '
    'FROM SSB_Lineorder l, SSB_Date d, SSB_Part p, SSB_Supplier s '
    'WHERE l.lo_orderdate = d.d_datekey '
    'AND d.d_datekey < 19920701 '
    'AND l.lo_partkey = p.p_partkey '
    'AND l.lo_suppkey = s.s_suppkey '
    'AND p.p_brand1 = "MFGR#22021" '
    'AND s.s_region = "EUROPE" '
    'GROUP BY d.d_datekey, p.p_brand1 '
    'ORDER BY d.d_datekey, p.p_brand1;'
)
interactive_query_SSB_q23 = (
    'SET `compiler.interactive.mode` "true"; '
    'SELECT d.d_datekey, p.p_brand1, SUM(l.lo_revenue) AS revenue '
    'FROM SSB_Date d '
    'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
    'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
    'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
    'WHERE d.d_datekey < 19920315 '
    'AND p.p_brand1 = "MFGR#22021" '
    'AND s.s_region = "EUROPE" '
    'GROUP BY d.d_datekey, p.p_brand1; '
)
blocking_query_SSB_q23_dynamic = (
    "SET `compiler.blocking.mode` `true`; "
    "SELECT a.* FROM ( "
    " SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, p.p_brand1 "
    " FROM SSB_Lineorder l, SSB_Date d, SSB_Part p, SSB_Supplier s "
    " WHERE l.lo_orderdate = d.d_datekey "
    " AND l.lo_partkey = p.p_partkey "
    " AND l.lo_suppkey = s.s_suppkey "
    " AND p.p_brand1 = 'MFGR#22021' "
    " AND s.s_region = 'EUROPE' "
    " AND d.d_yearmonthnum < 199301 "
    " GROUP BY d.d_datekey, p.p_brand1 "
    " ORDER BY d.d_datekey, p.p_brand1 "
    ") a WHERE a.d_datekey > 19920101;"
)
interactive_query_SSB_q23_dynamic = interactive_query_SSB_q23

# ---- SSB_q31 ----
blocking_query_SSB_q31 = (
    'SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, c.c_nation, s.s_nation '
    'FROM SSB_Lineorder l, SSB_Date d, SSB_Part p, SSB_Supplier s, SSB_Customer c '
    'WHERE l.lo_orderdate = d.d_datekey '
    'AND d.d_datekey < 19920701 '
    'AND l.lo_partkey = p.p_partkey '
    'AND l.lo_suppkey = s.s_suppkey '
    'AND l.lo_custkey = c.c_custkey '
    'AND c.c_region = "ASIA" '
    'AND s.s_region = "ASIA" '
    'GROUP BY d.d_datekey, c.c_nation, s.s_nation '
    'ORDER BY d.d_datekey ASC;'
)
interactive_query_SSB_q31 = (
    'SET `compiler.interactive.mode` "true"; '
    'SELECT c.c_nation, s.s_nation, d.d_datekey, SUM(l.lo_revenue) AS revenue '
    'FROM SSB_Date d '
    'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
    'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
    'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
    'JOIN SSB_Customer c ON l.lo_custkey /*+ indexnl */ = c.c_custkey '
    'WHERE d.d_datekey < 19920325 '
    'AND c.c_region = "ASIA" '
    'AND s.s_region = "ASIA" '
    'GROUP BY d.d_datekey, c.c_nation, s.s_nation '
)
blocking_query_SSB_q31_dynamic = (
    "SET `compiler.blocking.mode` `true`; "
    "SELECT a.* FROM ( "
    " SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, c.c_nation, s.s_nation "
    " FROM SSB_Lineorder l, SSB_Date d, SSB_Part p, SSB_Supplier s, SSB_Customer c "
    " WHERE l.lo_orderdate = d.d_datekey "
    " AND l.lo_partkey = p.p_partkey "
    " AND l.lo_suppkey = s.s_suppkey "
    " AND l.lo_custkey = c.c_custkey "
    " AND c.c_region = 'ASIA' "
    " AND s.s_region = 'ASIA' "
    " AND d.d_yearmonthnum < 199301 "
    " GROUP BY d.d_datekey, c.c_nation, s.s_nation "
    " ORDER BY d.d_datekey ASC "
    ") a WHERE a.d_datekey > 19920101;"
)
interactive_query_SSB_q31_dynamic = interactive_query_SSB_q31

# ---- SSB_q32 ----
blocking_query_SSB_q32 = (
    'SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, c.c_city, s.s_city '
    'FROM SSB_Lineorder l, SSB_Date d, SSB_Part p, SSB_Supplier s, SSB_Customer c '
    'WHERE l.lo_orderdate = d.d_datekey '
    'AND d.d_datekey < 19920701 '
    'AND l.lo_partkey = p.p_partkey '
    'AND l.lo_suppkey = s.s_suppkey '
    'AND l.lo_custkey = c.c_custkey '
    'AND c.c_nation = "UNITED STATES" '
    'AND s.s_nation = "UNITED STATES" '
    'GROUP BY d.d_datekey, c.c_city, s.s_city '
    'ORDER BY d.d_datekey ASC;'
)
interactive_query_SSB_q32 = (
    'SET `compiler.interactive.mode` "true"; '
    'SELECT c.c_city, s.s_city, d.d_datekey, SUM(l.lo_revenue) AS revenue '
    'FROM SSB_Date d '
    'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
    'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
    'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
    'JOIN SSB_Customer c ON l.lo_custkey /*+ indexnl */ = c.c_custkey '
    'WHERE d.d_datekey < 19920328 '
    'AND c.c_nation = "UNITED STATES" '
    'AND s.s_nation = "UNITED STATES" '
    'GROUP BY  d.d_datekey, c.c_city, s.s_city; '
)
blocking_query_SSB_q32_dynamic = (
    "SET `compiler.blocking.mode` `true`; "
    "SELECT a.* FROM ( "
    " SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, c.c_city, s.s_city "
    " FROM SSB_Lineorder l, SSB_Date d, SSB_Part p, SSB_Supplier s, SSB_Customer c "
    " WHERE l.lo_orderdate = d.d_datekey "
    " AND l.lo_partkey = p.p_partkey "
    " AND l.lo_suppkey = s.s_suppkey "
    " AND l.lo_custkey = c.c_custkey "
    " AND c.c_nation = 'UNITED STATES' "
    " AND s.s_nation = 'UNITED STATES' "
    " AND d.d_yearmonthnum < 199301 "
    " GROUP BY d.d_datekey, c.c_city, s.s_city "
    " ORDER BY d.d_datekey ASC "
    ") a WHERE a.d_datekey > 19920101;"
)
interactive_query_SSB_q32_dynamic = interactive_query_SSB_q32

# ---- SSB_q33 ----
blocking_query_SSB_q33 = (
    'SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, c.c_city, s.s_city '
    'FROM SSB_Lineorder l, SSB_Date d, SSB_Part p, SSB_Supplier s, SSB_Customer c '
    'WHERE l.lo_orderdate = d.d_datekey '
    'AND d.d_datekey < 19920701 '
    'AND l.lo_partkey = p.p_partkey '
    'AND l.lo_suppkey = s.s_suppkey '
    'AND l.lo_custkey = c.c_custkey '
    'AND c.c_city = "UNI" '
    'AND s.s_city = "UNI" '
    'GROUP BY d.d_datekey, c.c_city, s.s_city '
    'ORDER BY d.d_datekey ASC, revenue DESC;'
)
interactive_query_SSB_q33 = (
    'SET `compiler.interactive.mode` "true"; '
    'SELECT c.c_city, s.s_city, d.d_datekey, SUM(l.lo_revenue) AS revenue '
    'FROM SSB_Date d '
    'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
    'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
    'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
    'JOIN SSB_Customer c ON l.lo_custkey /*+ indexnl */ = c.c_custkey '
    'WHERE d.d_datekey < 19920328 '
    'AND c.c_city = "UNI" '
    'AND s.s_city = "UNI" '
    'GROUP BY  d.d_datekey, c.c_city, s.s_city; '
)
blocking_query_SSB_q33_dynamic = (
    "SET `compiler.blocking.mode` `true`; "
    "SELECT a.* FROM ( "
    " SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, c.c_city, s.s_city "
    " FROM SSB_Lineorder l, SSB_Date d, SSB_Part p, SSB_Supplier s, SSB_Customer c "
    " WHERE l.lo_orderdate = d.d_datekey "
    " AND l.lo_partkey = p.p_partkey "
    " AND l.lo_suppkey = s.s_suppkey "
    " AND l.lo_custkey = c.c_custkey "
    " AND c.c_city = 'UNI' "
    " AND s.s_city = 'UNI' "
    " AND d.d_yearmonthnum < 199301 "
    " GROUP BY d.d_datekey, c.c_city, s.s_city "
    " ORDER BY d.d_datekey ASC, revenue DESC "
    ") a WHERE a.d_datekey > 19920101;"
)
interactive_query_SSB_q33_dynamic = interactive_query_SSB_q33

# ---- SSB_q34 ----
blocking_query_SSB_q34 = (
    'SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, c.c_city, s.s_city '
    'FROM SSB_Date d '
    'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
    'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
    'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
    'JOIN SSB_Customer c ON l.lo_custkey /*+ indexnl */ = c.c_custkey '
    'AND d.d_yearmonth = "Dec1997" '
    'AND c.c_city = "UNI" '
    'AND s.s_city = "UNI" '
    'GROUP BY d.d_datekey, c.c_city, s.s_city '
    'ORDER BY d.d_datekey;'
)
interactive_query_SSB_q34 = (
    'SET `compiler.interactive.mode` "true"; '
    'SELECT c.c_city, s.s_city, d.d_datekey, SUM(l.lo_revenue) AS revenue '
    'FROM SSB_Date d '
    'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
    'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
    'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
    'JOIN SSB_Customer c ON l.lo_custkey /*+ indexnl */ = c.c_custkey '
    'WHERE d.d_datekey >= 19971201 '
    'AND d.d_yearmonth = "Dec1997" '
    'AND c.c_city = "UNI" '
    'AND s.s_city = "UNI" '
    'GROUP BY d.d_datekey, c.c_city, s.s_city; '
)
blocking_query_SSB_q34_dynamic = (
   'SET `compiler.blocking.mode` `true`; '
   'SELECT a.* FROM ( '
   ' SELECT SUM(l.lo_revenue) AS revenue, d.d_datekey, c.c_city, s.s_city '
   'FROM SSB_Date d '
   'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
   'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
   'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
   'JOIN SSB_Customer c ON l.lo_custkey /*+ indexnl */ = c.c_custkey '
   'AND d.d_yearmonth = "Dec1997" '
   ' AND c.c_city = \'UNI\' '
   ' AND s.s_city = \'UNI\' '
   ' GROUP BY d.d_datekey, c.c_city, s.s_city '
   ' ORDER BY d.d_datekey '
   ') a WHERE a.d_datekey > 19971201;'
)
interactive_query_SSB_q34_dynamic = interactive_query_SSB_q34

# ---- SSB_q41 ----
blocking_query_SSB_q41 = (
    'SELECT SUM(l.lo_revenue - l.lo_supplycost) AS profit, d.d_datekey, c.c_nation '
    'FROM SSB_Lineorder l, SSB_Date d, SSB_Customer c, SSB_Supplier s, SSB_Part p '
    'WHERE l.lo_orderdate = d.d_datekey '
    'AND d.d_datekey < 19920701 '
    'AND l.lo_custkey = c.c_custkey '
    'AND l.lo_suppkey = s.s_suppkey '
    'AND l.lo_partkey = p.p_partkey '
    'AND c.c_region = "AMERICA" '
    'AND s.s_region = "AMERICA" '
    'AND p.p_mfgr = "MFGR#1" '
    'GROUP BY d.d_datekey, c.c_nation;'
)
interactive_query_SSB_q41 = (
    'SET `compiler.interactive.mode` "true"; '
    'SELECT d.d_datekey, c.c_nation, SUM(l.lo_revenue - l.lo_supplycost) AS profit '
    'FROM SSB_Date d '
    'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
    'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
    'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
    'JOIN SSB_Customer c ON l.lo_custkey /*+ indexnl */ = c.c_custkey '
    'WHERE d.d_datekey < 19920330 '
    'AND c.c_region = "AMERICA" '
    'AND s.s_region = "AMERICA" '
    'AND (p.p_mfgr = "MFGR#1" ) '
    'GROUP BY d.d_datekey, c.c_nation; '
)
blocking_query_SSB_q41_dynamic = (
    "SET `compiler.blocking.mode` `true`; "
    "SELECT a.* FROM ( "
    " SELECT SUM(l.lo_revenue - l.lo_supplycost) AS profit, d.d_datekey, c.c_nation "
    " FROM SSB_Lineorder l, SSB_Date d, SSB_Customer c, SSB_Supplier s, SSB_Part p "
    " WHERE l.lo_orderdate = d.d_datekey "
    " AND l.lo_custkey = c.c_custkey "
    " AND l.lo_suppkey = s.s_suppkey "
    " AND l.lo_partkey = p.p_partkey "
    " AND c.c_region = 'AMERICA' "
    " AND s.s_region = 'AMERICA' "
    " AND p.p_mfgr = 'MFGR#1' "
    " AND d.d_yearmonthnum < 199301 "
    " GROUP BY d.d_datekey, c.c_nation "
    " ORDER BY d.d_datekey "
    ") a WHERE a.d_datekey > 19920101;"
)
interactive_query_SSB_q41_dynamic = interactive_query_SSB_q41

# ---- SSB_q42 ----
blocking_query_SSB_q42 = (
    'SELECT SUM(l.lo_revenue - l.lo_supplycost) AS profit, d.d_datekey, s.s_nation, p.p_category '
    'FROM SSB_Lineorder l, SSB_Date d, SSB_Customer c, SSB_Supplier s, SSB_Part p '
    'WHERE l.lo_orderdate = d.d_datekey '
    'AND d.d_datekey < 19920701 '
    'AND l.lo_custkey = c.c_custkey '
    'AND l.lo_suppkey = s.s_suppkey '
    'AND l.lo_partkey = p.p_partkey '
    'AND c.c_region = "AMERICA" '
    'AND s.s_region = "AMERICA" '
    'AND p.p_mfgr = "MFGR#1" '
    'GROUP BY d.d_datekey, s.s_nation, p.p_category '
    'ORDER BY d.d_datekey, s.s_nation, p.p_category;'
)
interactive_query_SSB_q42 = (
    'SET `compiler.interactive.mode` "true"; '
    'SELECT d.d_datekey, s.s_nation, p.p_category, SUM(l.lo_revenue - l.lo_supplycost) AS profit '
    'FROM SSB_Date d '
    'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
    'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
    'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
    'JOIN SSB_Customer c ON l.lo_custkey /*+ indexnl */ = c.c_custkey '
    'WHERE d.d_datekey < 19920323 '
    'AND c.c_region = "AMERICA" '
    'AND s.s_region = "AMERICA" '
    'AND (p.p_mfgr = "MFGR#1" ) '
    'GROUP BY d.d_datekey, s.s_nation, p.p_category;'
)
blocking_query_SSB_q42_dynamic = (
    "SET `compiler.blocking.mode` `true`; "
    "SELECT a.* FROM ( "
    " SELECT SUM(l.lo_revenue - l.lo_supplycost) AS profit, d.d_datekey, s.s_nation, p.p_category "
    " FROM SSB_Lineorder l, SSB_Date d, SSB_Customer c, SSB_Supplier s, SSB_Part p "
    " WHERE l.lo_orderdate = d.d_datekey "
    " AND l.lo_custkey = c.c_custkey "
    " AND l.lo_suppkey = s.s_suppkey "
    " AND l.lo_partkey = p.p_partkey "
    " AND c.c_region = 'AMERICA' "
    " AND s.s_region = 'AMERICA' "
    " AND p.p_mfgr = 'MFGR#1' "
    " AND d.d_yearmonthnum < 199301 "
    " GROUP BY d.d_datekey, s.s_nation, p.p_category "
    " ORDER BY d.d_datekey, s.s_nation, p.p_category "
    ") a WHERE a.d_datekey > 19920101;"
)
interactive_query_SSB_q42_dynamic = interactive_query_SSB_q42

# ---- SSB_q43 ----
blocking_query_SSB_q43 = (
    'SELECT SUM(l.lo_revenue - l.lo_supplycost) AS profit, d.d_datekey, s.s_city, p.p_brand1 '
    'FROM SSB_Lineorder l, SSB_Date d, SSB_Customer c, SSB_Supplier s, SSB_Part p '
    'WHERE l.lo_orderdate = d.d_datekey '
    'AND d.d_datekey < 19920701 '
    'AND l.lo_custkey = c.c_custkey '
    'AND l.lo_suppkey = s.s_suppkey '
    'AND l.lo_partkey = p.p_partkey '
    'AND c.c_region = "AMERICA" '
    'AND s.s_nation = "UNITED STATES" '
    'AND p.p_category = "MFGR#14" '
    'GROUP BY d.d_datekey, s.s_city, p.p_brand1 '
    'ORDER BY d.d_datekey, s.s_city, p.p_brand1;'
)
interactive_query_SSB_q43 = (
    'SET `compiler.interactive.mode` "true"; '
    'SELECT d.d_datekey, s.s_city, p.p_brand1, SUM(l.lo_revenue - l.lo_supplycost) AS profit '
    'FROM SSB_Date d '
    'JOIN SSB_Lineorder l ON d.d_datekey /*+ indexnl */ = l.lo_orderdate '
    'JOIN SSB_Part p ON l.lo_partkey /*+ indexnl */ = p.p_partkey '
    'JOIN SSB_Supplier s ON l.lo_suppkey /*+ indexnl */ = s.s_suppkey '
    'JOIN SSB_Customer c ON l.lo_custkey /*+ indexnl */ = c.c_custkey '
    'WHERE d.d_datekey < 19920330 '
    'AND c.c_region = "AMERICA" '
    'AND s.s_nation = "UNITED STATES" '
    'AND p.p_category = "MFGR#14" '
    'GROUP BY d.d_datekey, s.s_city, p.p_brand1;'
)
blocking_query_SSB_q43_dynamic = (
    "SET `compiler.blocking.mode` `true`; "
    "SELECT a.* FROM ( "
    " SELECT SUM(l.lo_revenue - l.lo_supplycost) AS profit, d.d_datekey, s.s_city, p.p_brand1 "
    " FROM SSB_Lineorder l, SSB_Date d, SSB_Customer c, SSB_Supplier s, SSB_Part p "
    " WHERE l.lo_orderdate = d.d_datekey "
    " AND l.lo_custkey = c.c_custkey "
    " AND l.lo_suppkey = s.s_suppkey "
    " AND l.lo_partkey = p.p_partkey "
    " AND c.c_region = 'AMERICA' "
    " AND s.s_nation = 'UNITED STATES' "
    " AND p.p_category = 'MFGR#14' "
    " AND d.d_yearmonthnum < 199301 "
    " GROUP BY d.d_datekey, s.s_city, p.p_brand1 "
    " ORDER BY d.d_datekey, s.s_city, p.p_brand1 "
    ") a WHERE a.d_datekey > 19920101;"
)
interactive_query_SSB_q43_dynamic = interactive_query_SSB_q43

# -------------------------------
# Main orchestration
# -------------------------------
async def main(mode, deployment, query, base_dir, runs, strategy, sf, number_of_nodes):
    signal_dir = "/scratch/asterixdb_eightynode/asterixdb_eightnode/results"
    del_dir = os.path.join(signal_dir, "HybridExecution")
    if os.path.isdir(del_dir):
        for name in ["B2ISignal", "I2BSignal", "InteractiveAnswers", "InteractiveAnswersAll",
                     "output_blocking.json", "output_interactive.json", "GroupBarriers"]:
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

    default_blocking_query = blocking_query_SSB_q21
    default_interactive_query = interactive_query_SSB_q21

    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
        for iteration in range(int(runs)):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            print(f"\n--- Run {iteration + 1} ---")
            time.sleep(1)

            # Resolve static and dynamic separately for hybrid
            static_blocking, _, query_label = resolve_queries(
                query, sf, "static", default_blocking_query, default_interactive_query)
            dynamic_blocking, dynamic_interactive, _ = resolve_queries(
                query, sf, "dynamic", default_blocking_query, default_interactive_query)

            if mode == "hybrid":
                print("\n[Phase 1] Running static blocking-only...")
                out_b1 = make_filename(base_dir, "blocking", "solo", deployment, timestamp,
                                       query_label, sf, iteration, "static", number_of_nodes)
                payload_b1 = build_payload(static_blocking, strategy="static", isInteractive=False)
                await run_query(session, payload_b1, out_b1)

                await asyncio.sleep(2)
                print("[Phase 2] Running dynamic blocking + interactive together...")
                out_b2 = make_filename(base_dir, "blocking", "hybrid", deployment, timestamp,
                                       query_label, sf, iteration, "dynamic", number_of_nodes)
                out_i2 = make_filename(base_dir, "interactive", "hybrid", deployment, timestamp,
                                       query_label, sf, iteration, "dynamic", number_of_nodes)
                payload_b2 = build_payload(dynamic_blocking, "dynamic", isInteractive=False)
                payload_i2 = build_payload(dynamic_interactive, "dynamic", isInteractive=True)
                await asyncio.gather(
                    run_query(session, payload_b2, out_b2),
                    run_query(session, payload_i2, out_i2)
                )

            else:
                blocking_query, interactive_query, query_label = resolve_queries(
                    query, sf, strategy, default_blocking_query, default_interactive_query)
                tasks = []
                if mode in ["blocking", "both"]:
                    out_b = make_filename(base_dir, "blocking", "1", deployment, timestamp,
                                          query_label, sf, iteration, strategy, number_of_nodes)
                    payload_b = build_payload(blocking_query, strategy, isInteractive=False)
                    tasks.append(run_query(session, payload_b, out_b))
                if mode in ["interactive", "both"]:
                    out_i = make_filename(base_dir, "interactive", "2", deployment, timestamp,
                                          query_label, sf, iteration, strategy, number_of_nodes)
                    payload_i = build_payload(interactive_query, strategy, isInteractive=True)
                    tasks.append(run_query(session, payload_i, out_i))
                if tasks:
                    await asyncio.gather(*tasks)

# -------------------------------
# CLI
# -------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="SSB SmartRabbit Executor with _{sf} suffixing and dynamic variants")
    p.add_argument("--mode", choices=["blocking", "interactive", "both", "hybrid"], default="both")
    p.add_argument("--deployment", default="local")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--query", choices=[
        "SSB_q21","SSB_q22","SSB_q23","SSB_q31","SSB_q32","SSB_q33","SSB_q34","SSB_q41","SSB_q42","SSB_q43"])
    group.add_argument("--queries", nargs="+")
    p.add_argument("--base-dir", default="/scratch/SmartRabbitProfilerOutputFourNode")
    p.add_argument("--runs", type=int, default=1)
    sf_group = p.add_mutually_exclusive_group(required=True)
    sf_group.add_argument("--sf")
    sf_group.add_argument("--sfs", nargs="+")
    p.add_argument("--strategy", choices=["static", "dynamic"], default="static")
    p.add_argument("--nodes", dest="number_of_nodes", default="1")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    query_list = args.queries if args.queries else [args.query]
    sf_list = args.sfs if args.sfs else [args.sf]

    async def run_all():
        for sf in sf_list:
            print(f"\n==============================\n===== Starting SF={sf} =====\n==============================\n")
            for q in query_list:
                print(f"\n========== Running {q} @ SF={sf} ==========")
                await main(args.mode, args.deployment, q, args.base_dir, args.runs,
                           args.strategy, sf, args.number_of_nodes)
                print(f"Finished {q} @ SF={sf}")
            print(f"\n----- Completed all queries for SF={sf} -----\n")
            await asyncio.sleep(3)

    asyncio.run(run_all())
