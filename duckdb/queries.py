"""This script runs tpch queries given a tpch database file"""
from time import time
import duckdb

# Parameters
sf = 300

conn = duckdb.connect(f'./tpch-sf{sf}.db')  # You can change the path here
print(f'loaded tpch sf{sf}')

# You can change the threads/memory limit here
conn.execute("SET memory_limit = '1GB';")
conn.execute("SET threads = 4;")

conn.execute('INSTALL tpch; LOAD tpch;')
for q in [1, 3, 4, 8, 9, 10, 12]:  # These are the set of queries to run
    start = time()
    # Duckdb tpch extension has all queries in-build
    # so we can just call that here using PRAGMA tpch(q)
    conn.execute(f'PRAGMA tpch({q})')
    stop = time()

    print(f'q{q:0>2} time = {stop - start:.3f}s')
