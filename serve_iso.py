from __future__ import annotations
import argparse, pathlib, time, orjson, pandas as pd, joblib
from mldetection.features import NUMERIC, CATEGORICAL, add_features

p = argparse.ArgumentParser()
p.add_argument("--log-file", default="logs/current/conn.log", type=pathlib.Path)
p.add_argument("--model", default="ids_iso.joblib", type=pathlib.Path)
p.add_argument("--threshold", type=float, default=-0.15)
args = p.parse_args()

pipe = joblib.load(args.model)


def _tail(fp):
    fp.seek(0, 2)
    while True:
        line = fp.readline()
        if line:
            yield line
        else:
            time.sleep(0.2)

with args.log_file.open() as fh:
    for raw in _tail(fh):
        try:
            row = orjson.loads(raw)
        except ValueError:
            continue
        df = pd.DataFrame([row])
        add_features(df)
        X = df[NUMERIC + CATEGORICAL]
        score = pipe.decision_function(X)[0]
        if score < args.threshold:
            ts = row.get("ts", "?")
            uid = row.get("uid", "?")
            src = row.get("id.orig_h", "?")
            dst = row.get("id.resp_h", "?")
            print(f"ALERT ts={ts} uid={uid} {src}->{dst} score={score:.2f}")
