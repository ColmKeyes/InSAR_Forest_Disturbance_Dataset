#!/usr/bin/env python3
import os
import pandas as pd
import pyarrow.dataset as ds

class ScenePairSelector:
    """
    Selects master–slave pairs based on a minimum temporal baseline,
    grouping by track, and outputs both sceneName and fileID for each.
    """
    def __init__(
        self,
        catalog_dir: str,
        output_csv:  str = "scene_pairs.csv",
        same_track:  bool = True,
        min_days:    int  = 12
    ):
        self.catalog_dir = catalog_dir
        self.output_csv  = output_csv
        self.same_track  = same_track
        self.min_days    = min_days

    def load_catalog(self) -> pd.DataFrame:
        ds_cat = ds.dataset(
            self.catalog_dir,
            format="parquet",
            partitioning="hive"
        )
        print("Catalog schema:", ds_cat.schema)
        # *** Include fileID here ***
        tbl = ds_cat.to_table(
            columns=["scene_id", "fileID", "startTime", "track"]
        )
        df = tbl.to_pandas()
        df["startTime"] = pd.to_datetime(df["startTime"])
        return df

    def select_pairs(self) -> pd.DataFrame:
        df = self.load_catalog()
        groups = df.groupby("track") if self.same_track else [("all", df)]
        rows   = []

        for _, grp in groups:
            grp = grp.sort_values("startTime").reset_index(drop=True)
            for i in range(len(grp) - 1):
                m = grp.loc[i]
                for j in range(i + 1, len(grp)):
                    s  = grp.loc[j]
                    dt = (s["startTime"] - m["startTime"]).days
                    if dt < self.min_days:
                        continue
                    # Append both sceneName and true fileID
                    rows.append({
                        "master_id":     m["scene_id"],
                        "master_fileID": m["fileID"],
                        "slave_id":      s["scene_id"],
                        "slave_fileID":  s["fileID"],
                        "delta_days":    dt,
                        "track":         m["track"]
                    })
                    break  # only the nearest-in-time slave

        return pd.DataFrame(rows)

    def run(self):
        pairs_df = self.select_pairs()
        pairs_df.to_csv(self.output_csv, index=False)
        print(f"✅ Wrote {len(pairs_df)} pairs to {self.output_csv}")

if __name__ == "__main__":
    selector = ScenePairSelector(
        catalog_dir=(
            "/mnt/beba5e41-f2c1-4634-8385-a643e895ca6b/"
            "data/pyarrow_hive/InSAR_Forest_Disturbance_Dataset"
        ),
        output_csv="pairs_june21_mar25.csv",
        same_track=True,
        min_days=12
    )
    selector.run()
