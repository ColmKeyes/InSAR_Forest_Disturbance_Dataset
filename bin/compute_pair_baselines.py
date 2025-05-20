#!/usr/bin/env python3

import os
import sys
import numpy as np
import pandas as pd
import asf_search as asf

class BaselineCalculator:
    """
    Reads a CSV of master/slave fileID pairs,
    computes:
      - temp_baseline = (slave.startTime - master.startTime).days
      - perp_baseline = Euclidean distance between their ECEF prePosition vectors
    Filters out any pairs with perp_baseline > max_perp, and writes an enriched CSV.
    """
    def __init__(
        self,
        pairs_csv:  str   = "pairs_jan2021.csv",
        output_csv: str   = "pairs_with_baselines_filtered.csv",
        max_perp:   float = 200.0    # metres
    ):
        self.pairs_csv  = pairs_csv
        self.output_csv = output_csv
        self.max_perp   = max_perp

        token = os.getenv("EARTHDATA_TOKEN")
        if not token:
            sys.exit("ERROR: EARTHDATA_TOKEN not set")
        asf.ASFSession().auth_with_token(token)

    def load_pairs(self) -> pd.DataFrame:
        df = pd.read_csv(self.pairs_csv, dtype=str)
        req = {"master_fileID","slave_fileID"}
        if not req.issubset(df.columns):
            sys.exit(f"ERROR: {self.pairs_csv} must contain columns {req}")
        return df

    def fetch_product(self, fileID: str):
        prods = asf.product_search([fileID])
        if not prods:
            raise RuntimeError(f"Product not found: {fileID}")
        return prods[0]

    def compute_baselines(self, df: pd.DataFrame) -> pd.DataFrame:
        df["perp_baseline"] = None
        df["temp_baseline"] = None

        for idx, row in df.iterrows():
            m_fid = row["master_fileID"]
            s_fid = row["slave_fileID"]

            try:
                m_prod = self.fetch_product(m_fid)
                s_prod = self.fetch_product(s_fid)
            except Exception as e:
                print(f"[{idx}] ERROR fetching products: {e}")
                continue

            # temporal baseline
            mt = pd.to_datetime(m_prod.properties["startTime"])
            st = pd.to_datetime(s_prod.properties["startTime"])
            dt = (st - mt).days

            # perpendicular baseline: distance between prePosition vectors
            m_pos = np.array(m_prod.baseline["stateVectors"]["positions"]["prePosition"], dtype=float)
            s_pos = np.array(s_prod.baseline["stateVectors"]["positions"]["prePosition"], dtype=float)
            perp = float(np.linalg.norm(s_pos - m_pos))

            df.at[idx, "temp_baseline"] = dt
            df.at[idx, "perp_baseline"] = perp
            print(f"[{idx}] {m_fid}→{s_fid}: temp={dt} d, perp={perp:.1f} m")

        return df

    def run(self):
        df = self.load_pairs()
        df = self.compute_baselines(df)
        # filter by perpendicular baseline
        df = df[df["perp_baseline"].astype(float) <= self.max_perp].reset_index(drop=True)
        df.to_csv(self.output_csv, index=False)
        print(f"✅ Wrote {len(df)} pairs with perp_baseline ≤ {self.max_perp} m to {self.output_csv}")

if __name__ == "__main__":
    BaselineCalculator(
        pairs_csv  = "pairs_june21_mar25.csv",
        output_csv = "pairs_june21_mar25_baseline.csv",
        max_perp   = 200.0
    ).run()
