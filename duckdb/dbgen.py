"""This script is used to generate tpch database with given scale factor"""
import duckdb

# Parameters
sf = 300
children = 10

conn = duckdb.connect(f'./tpch-sf{sf}.db')  # You can update the file path here
conn.execute("""
    INSTALL tpch;
    LOAD tpch;
""")

# You can set the threads/memory limits here
conn.execute("""
    SET threads = 128;
    SET memory_limit = '24GB';
""")
conn.execute("""
    DROP TABLE IF EXISTS customer;
    DROP TABLE IF EXISTS lineitem;
    DROP TABLE IF EXISTS nation;
    DROP TABLE IF EXISTS orders;
    DROP TABLE IF EXISTS part;
    DROP TABLE IF EXISTS partsupp;
    DROP TABLE IF EXISTS region;
    DROP TABLE IF EXISTS supplier;
""")

# This will run the dbgen in {children} number of steps
# For larger scale factor you will need more number of steps
# because it won't be able to fit everything in memory if no. of steps is very low
for step in range(children):
    conn.execute(
        f"CALL dbgen(sf = {sf}, children = {children}, step = {step});"
    )
    print(f"tpch sf{sf} dbgen (step {step+1} of {children})")

conn.commit()
conn.close()
