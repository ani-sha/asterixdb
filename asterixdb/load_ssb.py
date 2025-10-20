#!/usr/bin/env python3
import subprocess
import argparse
import os

SCALE_FACTORS = [10, 30, 50, 100, 300]

def run_curl(statement, is_json=True):
    """Execute a curl statement against AsterixDB."""
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
    """Generate CREATE TYPE/DATASET statements for SSB with the given scale factor."""
    return [
        # Customer
        f"CREATE TYPE SSB_CustomerType_{sf} AS {{ c_custkey: int64, c_name: string, c_address: string, c_city: string, c_nation: string, c_region: string, c_phone: string, c_mktsegment: string }};",
        f"CREATE DATASET SSB_Customer_{sf} (SSB_CustomerType_{sf}) PRIMARY KEY c_custkey;",

        # Supplier
        f"CREATE TYPE SSB_SupplierType_{sf} AS {{ s_suppkey: int64, s_name: string, s_address: string, s_city: string, s_nation: string, s_region: string, s_phone: string }};",
        f"CREATE DATASET SSB_Supplier_{sf} (SSB_SupplierType_{sf}) PRIMARY KEY s_suppkey;",

        # Part
        f"CREATE TYPE SSB_PartType_{sf} AS {{ p_partkey: int64, p_name: string, p_mfgr: string, p_category: string, p_brand1: string, p_color: string, p_type: string, p_size: int64, p_container: string }};",
        f"CREATE DATASET SSB_Part_{sf} (SSB_PartType_{sf}) PRIMARY KEY p_partkey;",

        # Date
        f"CREATE TYPE SSB_DateType_{sf} AS {{ d_datekey: int64, d_date: string, d_dayofweek: string, d_month: string, d_year: int64, d_yearmonthnum: int64, d_yearmonth: string, d_daynuminweek: int64, d_daynuminmonth: int64, d_daynuminyear: int64, d_monthnuminyear: int64, d_weeknuminyear: int64, d_sellingseason: string, d_lastdayinweekfl: string, d_lastdayinmonthfl: string, d_holidayfl: string, d_weekdayfl: string }};",
        f"CREATE DATASET SSB_Date_{sf} (SSB_DateType_{sf}) PRIMARY KEY d_datekey;",

        # Lineorder
        f"CREATE TYPE SSB_LineorderType_{sf} AS {{ lo_orderkey: int64, lo_linenumber: int64, lo_custkey: int64, lo_partkey: int64, lo_suppkey: int64, lo_orderdate: int64, lo_orderpriority: string, lo_shippriority: string, lo_quantity: int64, lo_extendedprice: double, lo_ordtotalprice: double, lo_discount: double, lo_revenue: double, lo_supplycost: double, lo_tax: double, lo_commitdate: int64, lo_shipmode: string }};",
        f"CREATE DATASET SSB_Lineorder_{sf} (SSB_LineorderType_{sf}) PRIMARY KEY lo_orderkey, lo_linenumber;"
    ]

def make_indexes(sf):
    """Example indexes (optional)."""
    return [
        f"DROP INDEX SSB_Lineorder_{sf}.idx_lo_orderdate IF EXISTS;",
        f"CREATE INDEX idx_lo_orderdate ON SSB_Lineorder_{sf}(lo_orderdate) TYPE BTREE;"
    ]

def load_one(sf, base_path):
    """Drop, recreate, and load all SSB datasets for the given scale factor."""
    datasets = {
        f"SSB_Customer_{sf}": "customer.tbl",
        f"SSB_Supplier_{sf}": "supplier.tbl",
        f"SSB_Part_{sf}": "part.tbl",
        f"SSB_Date_{sf}": "date.tbl",
        f"SSB_Lineorder_{sf}": "lineorder.tbl",
    }

    print(f"\n=== Loading SSB SF={sf} from {base_path} ===")

    # Drop datasets and types
    for ds in datasets.keys():
        run_curl(f"DROP DATASET {ds} IF EXISTS;")
    for t in ["SSB_CustomerType", "SSB_SupplierType", "SSB_PartType", "SSB_DateType", "SSB_LineorderType"]:
        run_curl(f"DROP TYPE {t}_{sf} IF EXISTS;")

    # Create
    for stmt in make_statements(sf):
        run_curl(stmt)

    # Load
    for dataset, filename in datasets.items():
        full_path = f"asterix_nc1://{base_path}/{filename}"
        load_stmt = (
            f'LOAD DATASET {dataset} USING localfs (("path"="{full_path}"), '
            '("format"="delimited-text"), ("delimiter"="|"));'
        )
        run_curl(load_stmt, is_json=False)

    # Index (optional)
    for stmt in make_indexes(sf):
        run_curl(stmt)

def main(sfs, base_path, all_flag):
    if all_flag:
        sfs = SCALE_FACTORS

    for sf in sfs:
        bp = base_path or f"/scratch/sbb-data/SF{sf}"
        if not os.path.exists(bp):
            print(f"WARNING: base path {bp} not found, skipping SF={sf}")
            continue
        load_one(sf, bp)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load SSB datasets into AsterixDB")
    parser.add_argument(
        "--sf", type=int, nargs="+", default=[10],
        choices=SCALE_FACTORS,
        help="SSB scale factor(s); e.g., --sf 10 30 100"
    )
    parser.add_argument("--base-path", help="Override dataset base path (default /scratch/sbb-data/SF{sf})")
    parser.add_argument("--all", action="store_true", help="Load data for all scale factors")
    args = parser.parse_args()

    main(args.sf, args.base_path, args.all)
