#!/usr/bin/env python3
import os
import asf_search as asf
from shapely.geometry import box

# 1. Authenticate
token = os.getenv("EARTHDATA_TOKEN")
if not token:
    raise RuntimeError("EARTHDATA_TOKEN not set in env")
session = asf.ASFSession().auth_with_token(token)

# 2. Run a small test search (just grab 1 or 2 products)
products = asf.search(
    platform="Sentinel-1",
    processingLevel="SLC",
    intersectsWith=box(108.0, -4.5, 119.0, 7.0).wkt,
    start="2021-01-01",
    end="2021-01-02"
)

if not products:
    raise RuntimeError("No products returned")

prod = products[0]

# 3. List all public attributes and methods
attrs = [a for a in dir(prod) if not a.startswith("_")]
print("=== ASF Product attributes & methods ===")
for a in attrs:
    print(" ", a)

# 4. Dump the properties dict
print("\n=== prod.properties keys ===")
for k, v in prod.properties.items():
    print(f" {k}: {v}")

# 5. Dump the meta dict
print("\n=== prod.meta keys ===")
for k, v in prod.meta.items():
    print(f" {k}: {v}")

# 6. Show UMM (Unified Metadata Model) if present
if hasattr(prod, "umm"):
    print("\n=== prod.umm keys ===")
    for k in prod.umm.keys():
        print(" ", k)


# assume `prod` is your S1Product instance from the ASF search:

# 1. The raw baseline dict (state vectors, node time, etc.)
print("baseline:", prod.baseline)
print("baseline cal prop:", prod.get_baseline_calc_properties)

# 2. The baseline type enum
print("baseline_type:", prod.baseline_type)

# 3. All members of the BaselineCalcType enum
print("BaselineCalcType members:")
for member in prod.BaselineCalcType:
    print(" ", member)

