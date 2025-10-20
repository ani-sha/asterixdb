import subprocess
import argparse

# Dataset name to filename map
datasets = {
    "Customer_30": "customer.tbl",
    "Lineitem_30": "lineitem.tbl",
    "Nation_30": "nation.tbl",
    "Orders_30": "orders.tbl",
    "Part_30": "part.tbl",
    "Partsupp_30": "partsupp.tbl",
    "Region_30": "region.tbl",
    "Supplier_30": "supplier.tbl"
}

create_statements = [
    "CREATE TYPE CustomerType_30 AS { c_custkey: int64, c_name: string, c_address: string, c_nationkey: int64, c_phone: string, c_acctbal: double, c_mktsegment: string, c_comment: string };",
    "CREATE DATASET Customer_30 (CustomerType_30) PRIMARY KEY c_custkey;",

    "CREATE TYPE LineitemType_30 AS { l_orderkey: int64, l_partkey: int64, l_suppkey: int64, l_linenumber: int64, l_quantity: double, l_extendedprice: double, l_discount: double, l_tax: double, l_returnflag: string, l_linestatus: string, l_shipdate: string, l_commitdate: string, l_receiptdate: string, l_shipinstruct: string, l_shipmode: string, l_comment: string };",
    "CREATE DATASET Lineitem_30 (LineitemType_30) PRIMARY KEY l_orderkey, l_linenumber;",

    "CREATE TYPE NationType_30 AS { n_nationkey: int64, n_name: string, n_regionkey: int64, n_comment: string };",
    "CREATE DATASET Nation_30 (NationType_30) PRIMARY KEY n_nationkey;",

    "CREATE TYPE OrdersType_30 AS { o_orderkey: int64, o_custkey: int64, o_orderstatus: string, o_totalprice: double, o_orderdate: string, o_orderpriority: string, o_clerk: string, o_shippriority: int64, o_comment: string };",
    "CREATE DATASET Orders_30 (OrdersType_30) PRIMARY KEY o_orderkey;",

    "CREATE TYPE PartType_30 AS { p_partkey: int64, p_name: string, p_mfgr: string, p_brand: string, p_type: string, p_size: int64, p_container: string, p_retailprice: double, p_comment: string };",
    "CREATE DATASET Part_30 (PartType_30) PRIMARY KEY p_partkey;",

    "CREATE TYPE PartsuppType_30 AS { ps_partkey: int64, ps_suppkey: int64, ps_availqty: int64, ps_supplycost: double, ps_comment: string };",
    "CREATE DATASET Partsupp_30 (PartsuppType_30) PRIMARY KEY ps_partkey, ps_suppkey;",

    "CREATE TYPE RegionType_30 AS { r_regionkey: int64, r_name: string, r_comment: string };",
    "CREATE DATASET Region_30 (RegionType_30) PRIMARY KEY r_regionkey;",

    "CREATE TYPE SupplierType_30 AS { s_suppkey: int64, s_name: string, s_address: string, s_nationkey: int64, s_phone: string, s_acctbal: double, s_comment: string };",
    "CREATE DATASET Supplier_30 (SupplierType_30) PRIMARY KEY s_suppkey;"
]

# Indexes
index_statements = [
    "DROP INDEX Orders_30.idx_o_custkey IF EXISTS;",
    "CREATE INDEX idx_o_custkey ON Orders_30(o_custkey) TYPE BTREE;"
    "DROP INDEX Orders_30.idx_o_totalprice IF EXISTS;",
    "CREATE INDEX idx_o_totalprice ON Orders_30(o_custkey) TYPE BTREE;"
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
        "CustomerType_30", "LineitemType_30", "NationType_30", "OrdersType_30",
        "PartType_30", "PartsuppType_30", "RegionType_30", "SupplierType_30"
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

    print("Creating index on Orders_30(o_custkey)...")
    for stmt in index_statements:
        run_curl(stmt, is_json=True)

    print("Loading data from local files...")
    for dataset, filename in datasets.items():
        full_path = f"localhost://{local_path}/{filename}"
        load_stmt = (
            f'LOAD DATASET {dataset} USING localfs (("path"="{full_path}"), '
            f'("format"="delimited-text"), ("delimiter"="|"));'
        )
        run_curl(load_stmt, is_json=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load TPC-H datasets (SF30) into AsterixDB")
    parser.add_argument("--base-path", default="/scratch/pratyoyd",
                        help="Local base path (e.g., /home/pratyoyd/tpch-dbgen)")
    args = parser.parse_args()

    main(args.base_path)
