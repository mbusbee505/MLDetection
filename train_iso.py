from __future__ import annotations
import argparse, pathlib, pandas as pd
from mldetection import model

p = argparse.ArgumentParser()
p.add_argument("--parquet-dir", default="parquet", type=pathlib.Path)
p.add_argument("--out", default="ids_iso.joblib", type=pathlib.Path)
p.add_argument("--contamination", type=float, default=0.01)
p.add_argument("--n-estimators", type=int, default=300)
p = p.parse_args()

files = sorted(p.parquet_dir.glob("*.parquet"))
if not files:
    raise SystemExit("no parquet files found")

df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
pipe = model.train_iso(df, contamination=p.contamination, n_estimators=p.n_estimators)
model.save(pipe, p.out)
print(p.out)
