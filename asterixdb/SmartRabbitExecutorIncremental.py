import aiohttp
import asyncio
import os
import argparse
from datetime import datetime
import time

# timeout configuration for aiohttp client
# total request timeout is set high due to long-running queries
# individual connection and socket timeouts are also extended

timeout = aiohttp.ClientTimeout(
    total=10000,        # total request time
    connect=600,        # max time to connect
    sock_connect=600,   # max time to establish socket
    sock_read=10000     # max time without receiving a chunk
)

# interactive_query = ("set `compiler.interactive.mode` `true`;SELECT l.l_shipdate,sum(l.l_quantity) FROM Lineitem_10 l where l.l_shipdate > \"1992-12-16\" AND l.l_shipdate <= \"1995-12-31\" GROUP BY l.l_shipdate")

# blocking_query = ("SELECT a.* FROM(SELECT l.l_shipdate,sum(l.l_quantity) FROM Lineitem_10 l where l.l_shipdate > \"1992-12-16\" AND l.l_shipdate <= \"1995-12-31\" GROUP BY l.l_shipdate)a where a.shipdate > \"1995-01-01\")

#tpch q10
interactive_query_q10 = (
    "SET `compiler.interactive.mode` `true`; "
    "SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, "
    "c.c_acctbal, c.c_address, c.c_phone, c.c_comment "
    "FROM Customer_10 AS c, Orders_10 AS o, Lineitem_10 AS l "
    "WHERE c.c_custkey /*+indexnl*/ = o.o_custkey AND o.o_orderkey /*+indexnl*/ = l.l_orderkey "
    "AND o.o_orderdate >= \"1993-10-01\" AND o.o_orderdate < \"1994-01-01\" AND l.l_returnflag = \"R\" AND c.c_custkey < 20000 "
    "GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, c.c_address, c.c_comment"
)

interactive_query_q10_dynamic = (
    "SET `compiler.interactive.mode` `true`; "
    "SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, "
    "c.c_acctbal, c.c_address, c.c_phone, c.c_comment "
    "FROM Customer_10 AS c, Orders_10 AS o, Lineitem_10 AS l "
    "WHERE c.c_custkey /*+indexnl*/ = o.o_custkey AND o.o_orderkey /*+indexnl*/ = l.l_orderkey "
    "AND o.o_orderdate >= \"1993-10-01\" AND o.o_orderdate < \"1994-01-01\" AND l.l_returnflag = \"R\"  "
    "GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, c.c_address, c.c_comment"
)

blocking_query_q10 = (
    "SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, "
    "c.c_acctbal, c.c_address, c.c_phone, c.c_comment "
    "FROM Customer_10 AS c "
    "JOIN Orders_10 AS o ON o.o_custkey = c.c_custkey "
    "JOIN Lineitem_10 AS l ON l.l_orderkey = o.o_orderkey "
    "WHERE o.o_orderdate >= \"1993-10-01\" AND o.o_orderdate < \"1994-01-01\" "
    "AND l.l_returnflag = \"R\"  "
    "GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_address, c.c_phone, c.c_comment"
)

blocking_query_q10_dynamic = (
    "SET `compiler.blocking.mode` `true`; "
    "SELECT a.* FROM( "
    "SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, "
    "c.c_acctbal, c.c_address, c.c_phone, c.c_comment "
    "FROM Customer_10 AS c "
    "JOIN Orders_10 AS o ON o.o_custkey = c.c_custkey "
    "JOIN Lineitem_10 AS l ON l.l_orderkey = o.o_orderkey "
    "WHERE o.o_orderdate >= \"1993-10-01\" AND o.o_orderdate < \"1994-01-01\" "
    "AND l.l_returnflag = \"R\"  "
    "GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_address, c.c_phone, c.c_comment"
    ")a where a.c_custkey > 20000"

)

# tpch q3
interactive_query_q3 = (
    "SET `compiler.interactive.mode` `true`; "
    "SELECT l.l_orderkey, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue "
    "FROM Lineitem_10 AS l, Orders_10 AS o, Customer_10 AS c "
    "WHERE c.c_mktsegment = \"BUILDING\" "
    "AND l.l_orderkey /*+ indexnl */ = o.o_orderkey "
    "AND o.o_custkey /*+ indexnl */ = c.c_custkey "
    "AND o.o_orderdate < \"1995-03-15\" AND l.l_shipdate > \"1995-03-15\" "
    "GROUP BY l.l_orderkey"
)

interactive_query_q3_dynamic = (
    "SET `compiler.interactive.mode` `true`; "
    "SELECT l.l_orderkey, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue "
    "FROM Lineitem_10 AS l, Orders_10 AS o, Customer_10 AS c "
    "WHERE c.c_mktsegment = \"BUILDING\" "
    "AND l.l_orderkey /*+ indexnl */ = o.o_orderkey "
    "AND o.o_custkey /*+ indexnl */ = c.c_custkey "
    "AND o.o_orderdate < \"1995-03-15\" AND l.l_shipdate > \"1995-03-15\" "
    "GROUP BY l.l_orderkey"
)
interactive_query_q3_static = (
    "SET `compiler.interactive.mode` `true`; "
    "SELECT l.l_orderkey, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue "
    "FROM Lineitem_10 AS l, Orders_10 AS o, Customer_10 AS c "
    "WHERE c.c_mktsegment = \"BUILDING\" "
    "AND l.l_orderkey /*+ indexnl */ = o.o_orderkey "
    "AND o.o_custkey /*+ indexnl */ = c.c_custkey "
    "AND o.o_orderdate < \"1995-03-15\" AND l.l_shipdate > \"1995-03-15\" and l_orderkey < 300000"
    "GROUP BY l.l_orderkey"
)
blocking_query_q3 = (
    "SELECT l.l_orderkey, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue "
    "FROM Customer_10 AS c, Orders_10 AS o, Lineitem_10 AS l "
    "WHERE c.c_mktsegment = \"BUILDING\" "
    "AND o.o_custkey = c.c_custkey "
    "AND l.l_orderkey = o.o_orderkey "
    "AND o.o_orderdate < \"1995-03-15\" AND l.l_shipdate > \"1995-03-15\" "
    "GROUP BY l.l_orderkey"
)

blocking_query_q3_dynamic = (
    "SET `compiler.blocking.mode` `true`; "
    "SELECT a.* FROM( "
    "SELECT l.l_orderkey, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue "
    "FROM Customer_10 AS c, Orders_10 AS o, Lineitem_10 AS l "
    "WHERE c.c_mktsegment = \"BUILDING\" "
    "AND o.o_custkey = c.c_custkey "
    "AND l.l_orderkey = o.o_orderkey "
    "AND o.o_orderdate < \"1995-03-15\" AND l.l_shipdate > \"1995-03-15\" "
    "GROUP BY l.l_orderkey "
    ")a where a.l_orderkey > 10000"
)

blocking_query_q3_static = (
    "SET `compiler.blocking.mode` `true`; "
    "SELECT a.* FROM( "
    "SELECT l.l_orderkey, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue "
    "FROM Customer_10 AS c, Orders_10 AS o, Lineitem_10 AS l "
    "WHERE c.c_mktsegment = \"BUILDING\" "
    "AND o.o_custkey = c.c_custkey "
    "AND l.l_orderkey = o.o_orderkey "
    "AND o.o_orderdate < \"1995-03-15\" AND l.l_shipdate > \"1995-03-15\" "
    "GROUP BY l.l_orderkey "
    ")a where a.l_orderkey > 10000"
)

blocking_query_q1 = "SELECT l_returnflag, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM Lineitem_10 WHERE l_shipdate <= \"1998-09-01\" GROUP BY l_returnflag"
interactive_query_q1 =  "SET `compiler.interactive.mode` \"true\"; SELECT l_returnflag, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM Lineitem_10 WHERE l_shipdate <= \"1998-09-01\" AND l_returnflag = \"A\" GROUP BY l_returnflag"
blocking_query_q2 = "SELECT s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr, s.s_address, s.s_phone, s.s_comment FROM Part_10 p, Supplier_10 s, Partsupp_10 ps, Nation_10 n, Region_10 r WHERE p.p_partkey = ps.ps_partkey AND s.s_suppkey = ps.ps_suppkey AND p.p_size = 15 AND p.p_type LIKE \"%BRASS\" AND s.s_nationkey = n.n_nationkey AND n.n_regionkey = r.r_regionkey AND r.r_name = \"EUROPE\" ORDER BY s.s_acctbal DESC"
interactive_query_q2 = "=SET `compiler.interactive.mode` \"true\"; SELECT s.s_acctbal, s.s_name, n.n_name, p.p_partkey,ps.ps_suppkey, p.p_mfgr, s.s_address, s.s_phone, s.s_comment FROM Supplier_10 s, Partsupp_10 ps, Part_10 p, Nation_10 n, Region_10 r WHERE s.s_suppkey /* +indexnl */= ps.ps_suppkey AND ps.ps_partkey /* +indexnl */= p.p_partkey  AND s.s_nationkey /* +indexnl*/ = n.n_nationkey AND n.n_regionkey/* +indexnl*/ = r.r_regionkey AND r.r_name = \"EUROPE\" AND p.p_size = 15 AND p.p_type LIKE \"%BRASS\" AND s.s_acctbal > -1000"
interactive_query_q4 = (
    "SET `compiler.interactive.mode` `true`; "
    "SELECT o.o_orderpriority, COUNT(DISTINCT o.o_orderkey) AS order_count "
    "FROM Orders_10 AS o JOIN Lineitem_10 AS l "
    "ON o.o_orderkey /*+ indexnl */ = l.l_orderkey "
    "WHERE o.o_orderdate >= \"1993-07-01\" AND o.o_orderdate < \"1993-10-01\" "
    "AND o.o_orderpriority <= \"2-HIGH\" "
    "AND l.l_commitdate < l.l_receiptdate "
    "GROUP BY o.o_orderpriority "
)

blocking_query_q4 = (

    "SELECT o.o_orderpriority, COUNT(DISTINCT o.o_orderkey) AS order_count "
    "FROM Orders_10 AS o JOIN Lineitem_10 AS l "
    "ON o.o_orderkey = l.l_orderkey "
    "WHERE o.o_orderdate >= \"1993-07-01\" AND o.o_orderdate < \"1993-10-01\" "
    "AND l.l_commitdate < l.l_receiptdate "
    "GROUP BY o.o_orderpriority"
    " ORDER BY o.o_orderpriority"
)

interactive_query_q5 = 'SET `compiler.interactive.mode` "true"; SELECT n.n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue FROM Nation_10 n, Region_10 r, Supplier_10 s, Customer_10 c, Lineitem_10 l, Orders_10 o WHERE n.n_regionkey /* +indexnl */ = r.r_regionkey AND n.n_nationkey /* +indexnl */ = s.s_nationkey AND n.n_nationkey /* +indexnl */ = c.c_nationkey AND c.custkey /* +indexnl */ = o.o_custkey AND o.o_orderkey /* +indexnl */ = l.l_orderkey AND s.s_suppkey /* +indexnl */ = l.l_suppkey AND r.r_name = "ASIA" AND o.o_orderdate >= "1994-01-01" AND o.o_orderdate < "1995-01-01" AND n.n_name > "A" GROUP BY n.n_name'

blocking_query_q5 = "SELECT n.n_name AS n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue FROM Customer_10 c, Orders_10 o, Lineitem_10 l, Supplier_10 s, Nation_10 n, Region_10 r WHERE c.c_custkey = o.o_custkey AND l.l_orderkey = o.o_orderkey AND l.l_suppkey = s.s_suppkey AND c.c_nationkey = s.s_nationkey AND s.s_nationkey = n.n_nationkey AND n.n_regionkey = r.r_regionkey AND r.r_name = \"ASIA\" AND o.o_orderdate >= \"1994-01-01\" AND o.o_orderdate < \"1995-01-01\" GROUP BY n.n_name"
headers = {
    "Content-Type": "application/json"
}
interactive_query_q9 = '=SET `compiler.interactive.mode` "true"; SELECT n.n_name AS nation, SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS sum_profit FROM Nation_10 AS n JOIN Supplier_10 AS s ON n.n_nationkey /*+ indexnl */ = s.s_nationkey JOIN Lineitem_10 AS l ON s.s_suppkey /*+ indexnl */ = l.l_suppkey JOIN Part_10 AS p ON l.l_partkey /*+ indexnl */ = p.p_partkey JOIN Partsupp_10 AS ps ON l.l_partkey /*+ indexnl */ = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey WHERE p.p_name LIKE "%green%" AND n.n_name = "ALGERIA" GROUP BY n.n_name'
blocking_query_q9 = 'SELECT n.n_name AS nation, SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS sum_profit FROM Part_10 AS p JOIN Lineitem_10 AS l ON l.l_partkey = p.p_partkey JOIN Partsupp_10 AS ps ON ps.ps_partkey = l.l_partkey AND ps.ps_suppkey = l.l_suppkey JOIN Supplier_10 AS s ON s.s_suppkey = l.l_suppkey JOIN Nation_10 AS n ON s.s_nationkey = n.n_nationkey WHERE p.p_name LIKE "%green%" GROUP BY n.n_name ORDER BY n.n_name'

url = "http://localhost:19002/query/service"

def build_payload(statement, strategy, isInteractive):
    payload = {
        "statement": statement,
#         "profile": "timings",
#         "optimized-logical-plan": "true"
    }
    if not (strategy == "dynamic" and isInteractive):
            payload["mode"] = "deferred"
    return payload

def make_filename(base_dir,mode, part, deployment, timestamp, query, iteration, strategy):
    return os.path.join(
        base_dir,"HybridExecution",
        f"output_{part}_{mode}_{strategy}_{deployment}_{timestamp}_{query}_run{iteration + 1}.json"
    )

# runs a single query and saves result to filename
async def run_query(session, payload, filename, delay=0):
    try:
        if delay > 0:
            print(f"Delaying query for {filename} by {delay} seconds...")
            await asyncio.sleep(delay)
        async with session.post(url, headers=headers, json=payload) as response:
            output = await response.text()
            with open(filename, "w") as file:
                file.write(output)
            print(f"Run completed. Output saved to {filename}")
    except asyncio.TimeoutError:
        print(f" Timeout error on query for {filename}")
    except Exception as e:
        print(f" Other error for {filename}: {e}")

# main function: selects queries, handles filenames and execution mode
async def main(mode, deployment, query, base_dir, runs, strategy):
    time.sleep(15)
    signal_dir = "/scratch/asterixdb/results"
    for iteration in range(int(runs)):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"\n--- Run {iteration + 1} ---")

        del_dir = os.path.join(signal_dir, "HybridExecution")
        hybrid_dir = os.path.join(base_dir, "HybridExecution")
        os.makedirs(hybrid_dir, exist_ok=True)

        files_to_delete = [
            os.path.join(del_dir, "B2ISignal"),
            os.path.join(del_dir, "I2BSignal"),
            os.path.join(del_dir, "InteractiveAnswers"),
            os.path.join(del_dir, "InteractiveAnswersAll"),
            os.path.join(del_dir, "output_blocking.json"),
            os.path.join(del_dir, "output_interactive.json")
        ]

        for f in files_to_delete:
            try:
                if os.path.exists(f):
                    os.remove(f)
                    print(f"Deleted file: {f}")
            except Exception as e:
                print(f"Failed to delete {f}: {e}")



        # Base fallback queries (default to q3)
        default_blocking_query = blocking_query_q3
        default_interactive_query = interactive_query_q3

        # If strategy is dynamic, try to fetch _dynamic variants
        if strategy == "dynamic":
            blocking_query = globals().get(f"blocking_query_{query}_dynamic", globals().get(f"blocking_query_{query}", default_blocking_query))
            interactive_query = globals().get(f"interactive_query_{query}_dynamic", globals().get(f"interactive_query_{query}", default_interactive_query))
        else:
          if query == "q3":
                          cutoff = (iteration + 1) * 2700000
                          interactive_query = (
                              "SET `compiler.interactive.mode` `true`; "
                              "SELECT l.l_orderkey, o.o_orderdate, o.o_shippriority, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue "
                              "FROM Lineitem_10 AS l, Orders_10 AS o, Customer_10 AS c "
                              "WHERE c.c_mktsegment = \"BUILDING\" "
                              "AND l.l_orderkey /*+ indexnl */ = o.o_orderkey "
                              "AND o.o_custkey /*+ indexnl */ = c.c_custkey "
                              "AND o.o_orderdate < \"1995-03-15\" "
                              "AND l.l_shipdate > \"1995-03-15\" "
                              f"AND l.l_orderkey < {cutoff} "
                              "GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority "
                          )
                          blocking_query = (

                              "SELECT l.l_orderkey, o.o_orderdate,o.o_shippriority, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue "
                              "FROM Customer_10 AS c, Orders_10 AS o, Lineitem_10 AS l "
                              "WHERE c.c_mktsegment = \"BUILDING\" "
                              "AND o.o_custkey = c.c_custkey "
                              "AND l.l_orderkey = o.o_orderkey "
                              "AND o.o_orderdate < \"1995-03-15\" "
                              "AND l.l_shipdate > \"1995-03-15\" "
                              f"AND l.l_orderkey >= {cutoff} "
                              "GROUP BY l.l_orderkey, o.o_orderdate,o.o_shippriority "
                          )
          elif query  == "q10":
                        cutoff = (iteration + 1)*12500
                        blocking_query =  (
                                             "SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, "
                                             "c.c_acctbal, c.c_address, c.c_phone, c.c_comment "
                                             "FROM Customer_10 AS c "
                                             "JOIN Orders_10 AS o ON o.o_custkey = c.c_custkey "
                                             "JOIN Lineitem_10 AS l ON l.l_orderkey = o.o_orderkey "
                                             "WHERE o.o_orderdate >= \"1993-10-01\" AND o.o_orderdate < \"1994-01-01\" "
                                             f"AND l.l_returnflag = \"R\" and c.c_custkey >= {cutoff} "
                                             "GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_address, c.c_phone, c.c_comment"
                                         )
                        interactive_query = ("SET `compiler.interactive.mode` `true`; "
                                                 "SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, "
                                                 "c.c_acctbal, c.c_address, c.c_phone, c.c_comment "
                                                 "FROM Customer_10 AS c, Orders_10 AS o, Lineitem_10 AS l "
                                                 "WHERE c.c_custkey /*+indexnl*/ = o.o_custkey AND o.o_orderkey /*+indexnl*/ = l.l_orderkey "
                                                 f"AND o.o_orderdate >= \"1993-10-01\" AND o.o_orderdate < \"1994-01-01\" AND l.l_returnflag = \"R\" AND c.c_custkey < {cutoff} "
                                                 "GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, c.c_address, c.c_comment")

        async with aiohttp.ClientSession(timeout=timeout) as session:
           if mode in ["blocking", "interactive"]:
               query_obj = blocking_query if mode == "blocking" else interactive_query
               payloads = [build_payload(query_obj, strategy, False)] * 2
               filenames = [
                   make_filename(base_dir, mode, "A", deployment, timestamp, query, iteration, strategy),
                   make_filename(base_dir, mode, "B", deployment, timestamp, query, iteration, strategy)
               ]

           elif mode in ["blocking_single", "interactive_single"]:
               query_obj = blocking_query if "blocking" in mode else interactive_query
               payloads = [build_payload(query_obj, strategy, False)]
               filenames = [
                   make_filename(base_dir, mode, "single", deployment, timestamp, query, iteration, strategy)
               ]

           else:  # hybrid
               payloads = [
                   build_payload(interactive_query, strategy, True),
                   build_payload(blocking_query, strategy, False)
               ]
               filenames = [
                   make_filename(base_dir, mode, "interactive", deployment, timestamp, query, iteration, strategy),
                   make_filename(base_dir, mode, "blocking", deployment, timestamp, query, iteration, strategy)
               ]

           tasks = [run_query(session, payloads[i], filenames[i], delay=0) for i in range(len(payloads))]
           await asyncio.gather(*tasks)
           interactive_answers_path = os.path.join(signal_dir, "HybridExecution", "InteractiveAnswers")
           if os.path.exists(interactive_answers_path):
               new_name = f"InteractiveAnswers_{query}_{timestamp}"
               new_path = os.path.join(signal_dir, "HybridExecution", new_name)
               try:
                   os.rename(interactive_answers_path, new_path)
                   print(f"Renamed {interactive_answers_path} to {new_path}")
               except Exception as e:
                   print(f"Failed to rename {interactive_answers_path}: {e}")
           else:
               print(f"No InteractiveAnswers file found for run {iteration + 1}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run AsterixDB queries")
    parser.add_argument("--mode", choices=["hybrid", "blocking", "blocking_single", "interactive", "interactive_single"], default="hybrid")
    parser.add_argument("--deployment", choices=["single", "multi"], default="multi")
    parser.add_argument("--query", choices=["q10", "q3", "q1", "q4"], default="q3")
    parser.add_argument("--base-dir", default="/scratch/SmartRabbitProfilerOutput", help="Base directory for output files and signals")
    parser.add_argument("--runs", default = 3, help = "No of runs you want to run")
    parser.add_argument("--strategy", default = "static", choices= ["dynamic", "static"])
    args = parser.parse_args()

    asyncio.run(main(args.mode, args.deployment, args.query, args.base_dir, args.runs, args.strategy))
