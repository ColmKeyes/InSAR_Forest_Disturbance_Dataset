#!/usr/bin/env python3

import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pandas as pd
from shapely import wkb

# ——— Configuration —————————————————————————————
# Path to your Hive directory of Parquet files
CATALOG_DIR = "/home/colm-the-conjurer/VSCode/workspace/InSAR_Forest_Disturbance_Dataset/data/products/borneo_s1_catalog/"

# (Optional) Or point directly at a single part file:
PART_FILE = CATALOG_DIR + "year=2021/month=01/part-0.parquet"


def print_hive_dataset(path):
    # Read the entire Hive-partitioned dataset
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    table   = dataset.to_table()
    print("=== PyArrow Table ===")
    print(table)             # shows schema and a small in-memory preview

    # Convert to Pandas for a nicer view
    df = table.to_pandas()
    # If you have a WKB-encoded geometry column:
    if df["geometry"].dtype == object and isinstance(df["geometry"].iat[0], (bytes, bytearray)):
        df["geometry"] = df["geometry"].apply(wkb.loads)

    print("\n=== Pandas DataFrame (first 5 rows) ===")
    print(df.head())
    print("\nColumns and dtypes:")
    print(df.dtypes)
    print(f"\nTotal rows: {len(df)}")


def print_single_file(path):
    # Read just one Parquet file
    table = pq.read_table(path)
    print("=== Single Parquet File ===")
    print(table)


if __name__ == "__main__":
    print(">>> Hive‐partitioned dataset:")
    print_hive_dataset(CATALOG_DIR)
    print("\n>>> Single partition file:")
    print_single_file(PART_FILE)
