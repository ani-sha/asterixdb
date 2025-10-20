import subprocess
import argparse

# Dataset name to filename map
datasets = {
    "Customer_1": "customer.tbl",
    "Lineitem_1": "lineitem.tbl",
    "Nation_1": "nation.tbl",
    "Orders_1": "orders.tbl",
    "Part_1": "part.tbl",
    "Partsupp_1": "partsupp.tbl",
    "Region_1": "region.tbl",
    "Supplier_1": "supplier.tbl"
}

create_statements = [
    "CREATE TYPE CustomerType_1 AS { c_custkey: int64, c_name: string, c_address: string, c_nationkey: int64, c_phone: string, c_acctbal: double, c_mktsegment: string, c_comment: string };",
    "CREATE DATASET Customer_1 (CustomerType_1) PRIMARY KEY c_custkey;",

    "CREATE TYPE LineitemType_1 AS { l_orderkey: int64, l_partkey: int64, l_suppkey: int64, l_linenumber: int64, l_quantity: double, l_extendedprice: double, l_discount: double, l_tax: double, l_returnflag: string, l_linestatus: string, l_shipdate: string, l_commitdate: string, l_receiptdate: string, l_shipinstruct: string, l_shipmode: string, l_comment: string };",
    "CREATE DATASET Lineitem_1 (LineitemType_1) PRIMARY KEY l_orderkey, l_linenumber;",

    "CREATE TYPE NationType_1 AS { n_nationkey: int64, n_name: string, n_regionkey: int64, n_comment: string };",
    "CREATE DATASET Nation_1 (NationType_1) PRIMARY KEY n_nationkey;",

    "CREATE TYPE OrdersType_1 AS { o_orderkey: int64, o_custkey: int64, o_orderstatus: string, o_totalprice: double, o_orderdate: string, o_orderpriority: string, o_clerk: string, o_shippriority: int64, o_comment: string };",
    "CREATE DATASET Orders_1 (OrdersType_1) PRIMARY KEY o_orderkey;",

    "CREATE TYPE PartType_1 AS { p_partkey: int64, p_name: string, p_mfgr: string, p_brand: string, p_type: string, p_size: int64, p_container: string, p_retailprice: double, p_comment: string };",
    "CREATE DATASET Part_1 (PartType_1) PRIMARY KEY p_partkey;",

    "CREATE TYPE PartsuppType_1 AS { ps_partkey: int64, ps_suppkey: int64, ps_availqty: int64, ps_supplycost: double, ps_comment: string };",
    "CREATE DATASET Partsupp_1 (PartsuppType_1) PRIMARY KEY ps_partkey, ps_suppkey;",

    "CREATE TYPE RegionType_1 AS { r_regionkey: int64, r_name: string, r_comment: string };",
    "CREATE DATASET Region_1 (RegionType_1) PRIMARY KEY r_regionkey;",

    "CREATE TYPE SupplierType_1 AS { s_suppkey: int64, s_name: string, s_address: string, s_nationkey: int64, s_phone: string, s_acctbal: double, s_comment: string };",
    "CREATE DATASET Supplier_1 (SupplierType_1) PRIMARY KEY s_suppkey;"
]

# Indexes
index_statements = [
    "DROP INDEX Orders_1.idx_o_custkey IF EXISTS;",
    "CREATE INDEX idx_o_custkey ON Orders_1(o_custkey) TYPE BTREE;"
]

def run_curl(statement, is_json=True):
    if is_json:
        cmd = [
            "curl", "-X", "POST", "http://localhost:19002/query/service",
            "-H", "Content-Type: application/json",
            "-d", f'{{"statement":"{statement}"}}'
        ]
    else:
        cmd = [
            "curl", "-X", "POST", "http://localhost:19002/query/service",
            "-H", "Content-Type: application/x-www-form-urlencoded",
            "-d", f"statement={statement}"
        ]
    subprocess.run(cmd, check=False)

def main(local_path):
    # Drop all datasets and types if they exist
    dataset_names = list(datasets.keys())
    type_names = [
        "CustomerType_1", "LineitemType_1", "NationType_1", "OrdersType_1",
        "PartType_1", "PartsuppType_1", "RegionType_1", "SupplierType_1"
    ]

    print("Dropping datasets if they exist...")
    for ds in dataset_names:
        run_curl(f"DROP DATASET {ds} IF EXISTS;", is_json=True)

    print("Dropping types if they exist...")
    for t in type_names:
        run_curl(f"DROP TYPE {t} IF EXISTS;", is_json=True)

    print("Creating types and datasets...")
    for stmt in create_statements:
        run_curl(stmt, is_json=True)

    print("Creating index on Orders_1(o_custkey)...")
    for stmt in index_statements:
        run_curl(stmt, is_json=True)

    print("Loading data from local files...")
    for dataset, filename in datasets.items():
        full_path = f"asterix_nc1://{local_path}/{filename}"
        load_stmt = (
            f'LOAD DATASET {dataset} USING localfs (("path"="{full_path}"), '
            f'("format"="delimited-text"), ("delimiter"="|"));'
        )
        run_curl(load_stmt, is_json=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load TPC-H datasets (SF1) into AsterixDB")
    parser.add_argument("--base-path", default="/scratch/pratyoyd/",
                        help="Local base path (e.g., /home/pratyoyd/tpch-dbgen)")
    args = parser.parse_args()

    main(args.base_path)
