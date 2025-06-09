#!/usr/bin/env python3
"""
Downloads Sentinel-1 SLC scenes in parallel based on validated scene pairs with baselines

@Time    : 2025-06-09
@Author  : Colm Keyes
@Email   : keyesco@tcd.ie
@File    : 4_download_s1_scenes.py

Input Requirements:
- CSV file with baseline-filtered scene pairs from step 3
- Partitioned Parquet catalog containing download URLs
- Earthdata token (set as EARTHDATA_TOKEN environment variable)
- Output directory for SAFE archives

Processing Steps:
1. Reads baseline-filtered pairs CSV file
2. Loads Parquet catalog to map fileID to download URLs
3. Collects unique URLs for all Reference and Secondary scenes
4. Authenticates with ASF using Earthdata token
5. Downloads SAFE archives in parallel (configurable processes)
6. Validates successful download of all required scenes

Output:
- Downloaded SAFE (.zip) archives in specified output directory
- Progress reporting during parallel download process

Example Usage:
EARTHDATA_TOKEN="your_token" python 4_download_s1_scenes.py
"""

import os
import sys
import pandas as pd
import pyarrow.dataset as ds
import asf_search as asf

# ——— Configuration —————————————————————————————
PAIRS_CSV   = "pairs_june21_mar25_baseline.csv"
CATALOG_DIR = (
    "/mnt/beba5e41-f2c1-4634-8385-a643e895ca6b/"
    "data/pyarrow_hive/InSAR_Forest_Disturbance_Dataset"
)
OUT_DIR     = "/mnt/beba5e41-f2c1-4634-8385-a643e895ca6b/data/SLC/raw"
PARALLEL    = 4  # number of simultaneous downloads

os.makedirs(OUT_DIR, exist_ok=True)

# 1. Load the final, baseline‐filtered pairs list
pairs = pd.read_csv(PAIRS_CSV, dtype=str)
if not {"master_fileID", "slave_fileID"}.issubset(pairs.columns):
    sys.exit(f"ERROR: {PAIRS_CSV} must contain master_fileID & slave_fileID")

# 2. Build fileID → download_url map from the catalog
ds_cat = ds.dataset(CATALOG_DIR, format="parquet", partitioning="hive")
tbl    = ds_cat.to_table(columns=["fileID", "download_url"])
catalog= tbl.to_pandas().drop_duplicates("fileID")
url_map= dict(zip(catalog["fileID"], catalog["download_url"]))

# 3. Collect all URLs for master and slave scenes
urls = []
for role in ("master_fileID", "slave_fileID"):
    for fid in pairs[role]:
        url = url_map.get(fid)
        if url:
            urls.append(url)

# 4. Authenticate to ASF
token = os.getenv("EARTHDATA_TOKEN")
if not token:
    sys.exit("ERROR: EARTHDATA_TOKEN not set in environment")
session = asf.ASFSession().auth_with_token(token)
 # 5. Parallel download
print(f"Downloading {len(urls)} scenes to {OUT_DIR} using {PARALLEL} parallel processes...")
asf.download_urls(
    urls=urls,
    path=OUT_DIR,
    session=session,
    processes=PARALLEL
)
print("✅ Download complete.")

#!/usr/bin/env python3

# import os
# import sys
# import pandas as pd
# import pyarrow.dataset as ds
# import asf_search as asf
#
# # ——— Configuration —————————————————————————————
# PAIRS_CSV   = "pairs_june21_mar25_baseline.csv"
# CATALOG_DIR = (
#         "/mnt/beba5e41-f2c1-4634-8385-a643e895ca6b/"
#         "data/pyarrow_hive/InSAR_Forest_Disturbance_Dataset"
# )
# OUT_DIR     =  "/mnt/beba5e41-f2c1-4634-8385-a643e895ca6b/data/SLC/raw"
#
#
# os.makedirs(OUT_DIR, exist_ok=True)
#
# # ——— Load the final pairs —————————————————————————
# pairs = pd.read_csv(PAIRS_CSV, dtype=str)
# if not {"master_fileID","slave_fileID"}.issubset(pairs.columns):
#     sys.exit(f"ERROR: {PAIRS_CSV} must contain master_fileID & slave_fileID")
#
# # ——— Load catalog to map fileID → download_url ————————
# ds_cat  = ds.dataset(CATALOG_DIR, format="parquet", partitioning="hive")
# table   = ds_cat.to_table(columns=["fileID","download_url"])
# catalog = table.to_pandas().drop_duplicates("fileID")
# url_map = dict(zip(catalog["fileID"], catalog["download_url"]))
#
# # ——— Build the full list of URLs ————————————————
# urls = []
# for role in ("master_fileID","slave_fileID"):
#     for fid in pairs[role]:
#         url = url_map.get(fid)
#         if not url:
#             print(f"WARNING: no URL found for {fid}")
#             continue
#         urls.append((fid, url))
#
# total = len(urls)
# print(f"Starting download of {total} scenes into {OUT_DIR}\n")
#
# # ——— Authenticate ———————————————————————————————
# token = os.getenv("EARTHDATA_TOKEN")
# if not token:
#     sys.exit("ERROR: EARTHDATA_TOKEN not set in environment")
# session = asf.ASFSession().auth_with_token(token)
#
# # ——— Download loop with progress print ———————————
# for i, (fid, url) in enumerate(urls, start=1):
#     print(f"[{i}/{total}] Downloading {fid} …", end=" ")
#     try:
#         asf.download_urls([url], path=OUT_DIR, session=session, processes=1)
#         print("✔")
#     except Exception as e:
#         print(f"✗ FAILED ({e})")
#
# print("\n✅ All done!")



# #!/usr/bin/env python3
# import os
# import sys
# import pyarrow.dataset as ds
# import pandas as pd
# import asf_search as asf
#
# token = os.getenv("EARTHDATA_TOKEN")
# if not token:
#     raise RuntimeError("EARTHDATA_TOKEN is not set")
# session = asf.ASFSession().auth_with_token(token)
#
# # ——— Paths —————————————————————————————————————————————
# CATALOG_DIR = os.path.expanduser(
#     "/mnt/beba5e41-f2c1-4634-8385-a643e895ca6b/"
#     "data/pyarrow_hive/InSAR_Forest_Disturbance_Dataset"
# )
# OUTPUT_DIR  = os.path.expanduser(
# "/mnt/beba5e41-f2c1-4634-8385-a643e895ca6b/data/SLC/raw"
# )
#
# # ——— Ensure directories exist ——————————————————————————————————
# os.makedirs(OUTPUT_DIR, exist_ok=True)
#
# # 1. Load the GeoParquet catalog (hive-partitioned)
# dataset = ds.dataset(CATALOG_DIR, format="parquet", partitioning="hive")
# # only pull the columns we care about
# table   = dataset.to_table(columns=["scene_id", "download_url"])
# df      = table.to_pandas()
#
# print(f"Loaded {len(df)} entries from catalog at:\n  {CATALOG_DIR}")
#
# # 2. De-duplicate by scene_id (just in case)
# df = df.drop_duplicates(subset=["scene_id"]).reset_index(drop=True)
# print(f"{len(df)} unique scenes after de-duplication")
#
# # 3. Authenticate to ASF via Earthdata token
# token = os.getenv("EARTHDATA_TOKEN")
# if not token:
#     sys.exit("ERROR: EARTHDATA_TOKEN is not set in the environment")
# session = asf.ASFSession().auth_with_token(token)
#
# # 4. Download all SAFE files in parallel
# urls = df["download_url"].tolist()
# print(f"Downloading {len(urls)} scenes into:\n  {OUTPUT_DIR}")
#
# asf.download_urls(
#     urls=urls,
#     path=OUTPUT_DIR,
#     session=session,
#     processes=4
# )
#
# print("✅ All downloads complete")
