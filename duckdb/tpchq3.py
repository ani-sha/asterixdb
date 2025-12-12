"""This script runs the tpch q3 manually instead of using the default one"""
from time import time
import duckdb

# Parameters
sf = 300
runs = 10

print(f'tpch sf{sf}')

for i in range(runs):
    print(f"\nRunning {i+1}_th run:")
    for threads in [4, 8, 16, 24, 32, 40]:  # You can modify the number of threads here
        conn = duckdb.connect(f'./tpch-sf{sf}.db')
        conn.execute('INSTALL tpch; LOAD tpch;')
        # You can modify the memory limit here
        conn.execute("SET memory_limit = '1GB';")
        conn.execute(f"SET threads = {threads};")

        start = time()
        # conn.execute(f'PRAGMA tpch({3})')
        conn.execute("""
                    SELECT
                        l_orderkey,
                        sum(l_extendedprice * (1 - l_discount)) AS revenue,
                        o_orderdate,
                        o_shippriority
                    FROM
                        customer,
                        orders,
                        lineitem
                    WHERE
                        c_mktsegment = 'BUILDING'
                        AND c_custkey = o_custkey
                        AND l_orderkey = o_orderkey
                        AND o_orderdate < CAST('1995-03-15' AS date)
                        AND l_shipdate > CAST('1995-03-15' AS date)
                    GROUP BY
                        l_orderkey,
                        o_orderdate,
                        o_shippriority
                    """

                     # Uncommend below lines to add order by and limit clause
                     # + """
                     # ORDER BY
                     #     revenue DESC,
                     #     o_orderdate
                     # LIMIT 10
                     # """

                     # Uncomment this line to add limit 1 clause for ttfr
                     #  + "LIMIT 1"
                     )
        stop = time()

        print(f'q3 time (#{threads:0>2} threads) = {stop - start:.3f}s')
        conn.close()
