#!/usr/bin/env python3
import subprocess
import argparse

SCALE_FACTORS = [1, 5, 10, 30, 50, 100, 300]

def run_curl(statement, is_json=True):
    if is_json:
        cmd = [
            "curl", "-s", "-X", "POST", "http://localhost:19002/query/service",
            "-H", "Content-Type: application/json",
            "-d", f'{{"statement":"{statement}"}}'
        ]
    else:
        cmd = [
            "curl", "-s", "-X", "POST", "http://localhost:19002/query/service",
            "-H", "Content-Type: application/x-www-form-urlencoded",
            "-d", f"statement={statement}"
        ]
    subprocess.run(cmd, check=False)

def make_statements(sf):
    """Generate CREATE TYPE/DATASET statements with the given scale factor."""
    return [
        f"CREATE TYPE CustomerType_{sf} AS {{ c_custkey: int64, c_name: string, c_address: string, c_nationkey: int64, c_phone: string, c_acctbal: double, c_mktsegment: string, c_comment: string }};",
        f"CREATE DATASET Customer_{sf} (CustomerType_{sf}) PRIMARY KEY c_custkey;",
        f"CREATE TYPE LineitemType_{sf} AS {{ l_orderkey: int64, l_partkey: int64, l_suppkey: int64, l_linenumber: int64, l_quantity: double, l_extendedprice: double, l_discount: double, l_tax: double, l_returnflag: string, l_linestatus: string, l_shipdate: string, l_commitdate: string, l_receiptdate: string, l_shipinstruct: string, l_shipmode: string, l_comment: string }};",
        f"CREATE DATASET Lineitem_{sf} (LineitemType_{sf}) PRIMARY KEY l_orderkey, l_linenumber;",
        f"CREATE TYPE NationType_{sf} AS {{ n_nationkey: int64, n_name: string, n_regionkey: int64, n_comment: string }};",
        f"CREATE DATASET Nation_{sf} (NationType_{sf}) PRIMARY KEY n_nationkey;",
        f"CREATE TYPE OrdersType_{sf} AS {{ o_orderkey: int64, o_custkey: int64, o_orderstatus: string, o_totalprice: double, o_orderdate: string, o_orderpriority: string, o_clerk: string, o_shippriority: int64, o_comment: string }};",
        f"CREATE DATASET Orders_{sf} (OrdersType_{sf}) PRIMARY KEY o_orderkey;",
        f"CREATE TYPE PartType_{sf} AS {{ p_partkey: int64, p_name: string, p_mfgr: string, p_brand: string, p_type: string, p_size: int64, p_container: string, p_retailprice: double, p_comment: string }};",
        f"CREATE DATASET Part_{sf} (PartType_{sf}) PRIMARY KEY p_partkey;",
        f"CREATE TYPE PartsuppType_{sf} AS {{ ps_partkey: int64, ps_suppkey: int64, ps_availqty: int64, ps_supplycost: double, ps_comment: string }};",
        f"CREATE DATASET Partsupp_{sf} (PartsuppType_{sf}) PRIMARY KEY ps_partkey, ps_suppkey;",
        f"CREATE TYPE RegionType_{sf} AS {{ r_regionkey: int64, r_name: string, r_comment: string }};",
        f"CREATE DATASET Region_{sf} (RegionType_{sf}) PRIMARY KEY r_regionkey;",
        f"CREATE TYPE SupplierType_{sf} AS {{ s_suppkey: int64, s_name: string, s_address: string, s_nationkey: int64, s_phone: string, s_acctbal: double, s_comment: string }};",
        f"CREATE DATASET Supplier_{sf} (SupplierType_{sf}) PRIMARY KEY s_suppkey;"
    ]

def make_indexes(sf):
    return [
        f"DROP INDEX Orders_{sf}.idx_o_custkey IF EXISTS;",
        f"CREATE INDEX idx_o_custkey ON Orders_{sf}(o_custkey) TYPE BTREE;"
    ]

def load_one(sf, base_path):
    datasets = {
        f"Customer_{sf}": "customer.tbl",
        f"Lineitem_{sf}": "lineitem.tbl",
        f"Nation_{sf}": "nation.tbl",
        f"Orders_{sf}": "orders.tbl",
        f"Part_{sf}": "part.tbl",
        f"Partsupp_{sf}": "partsupp.tbl",
        f"Region_{sf}": "region.tbl",
        f"Supplier_{sf}": "supplier.tbl"
    }

    print(f"\n=== Loading SF={sf} from {base_path} ===")

    # Drop datasets and types
    for ds in datasets.keys():
        run_curl(f"DROP DATASET {ds} IF EXISTS;")
    for t in ["CustomerType", "LineitemType", "NationType", "OrdersType",
              "PartType", "PartsuppType", "RegionType", "SupplierType"]:
        run_curl(f"DROP TYPE {t}_{sf} IF EXISTS;")

    # Create
    for stmt in make_statements(sf):
        run_curl(stmt)

    # Load
    for dataset, filename in datasets.items():
        full_path = f"asterix_nc1://{base_path}/{filename}"
        load_stmt = (f'LOAD DATASET {dataset} USING localfs (("path"="{full_path}"), '
                     '("format"="delimited-text"), ("delimiter"="|"));')
        run_curl(load_stmt, is_json=False)

    # Index
    for stmt in make_indexes(sf):
        run_curl(stmt)

def main(sfs, base_path, all_flag):
    if all_flag:
        sfs = SCALE_FACTORS

    for sf in sfs:
        bp = base_path or f"/scratch/tpch_data/sf{sf}"
        load_one(sf, bp)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load TPC-H datasets into AsterixDB")
    parser.add_argument(
        "--sf", type=int, nargs="+", default=[10],
        choices=SCALE_FACTORS,
        help="TPC-H scale factor(s); you can specify multiple (e.g. --sf 10 30 100)"
    )
    parser.add_argument("--base-path", help="Override dataset base path")
    parser.add_argument("--all", action="store_true",
                        help="Load data for all scale factors")
    args = parser.parse_args()

    main(args.sf, args.base_path, args.all)
