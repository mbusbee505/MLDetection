from __future__ import annotations
import pathlib, pandas as pd

__all__ = [
    "NUMERIC",
    "CATEGORICAL",
    "read_file",
    "add_features",
    "load_day",
]

NUMERIC = [
    "duration",
    "orig_bytes", "resp_bytes",
    "orig_pkts", "resp_pkts",
    "orig_ip_bytes", "resp_ip_bytes",
]

CATEGORICAL = [
    "proto", "service", "conn_state", "history",
]

def read_file(path: pathlib.Path) -> pd.DataFrame:
    with open(path, "rb") as fh:
        first = fh.readline().lstrip()
    if first.startswith(b"{"):
        return pd.read_json(path, lines=True)
    return pd.read_csv(path, sep="\t", comment="#", low_memory=False)

def add_features(df: pd.DataFrame) -> None:
    df["hour"] = pd.to_datetime(df["ts"], unit="s").dt.hour
    df["bytes_ratio"] = (df["orig_bytes"] + 1) / (df["resp_bytes"] + 1)
    df["pkts_total"] = df["orig_pkts"] + df["resp_pkts"]

NUMERIC_EXT = NUMERIC + ["bytes_ratio", "pkts_total", "hour"]

def load_day(day_dir: pathlib.Path) -> pd.DataFrame:
    files = [f for f in sorted(day_dir.glob("conn*.log*")) if not f.name.endswith(".idx")]
    df = pd.concat([read_file(f) for f in files], ignore_index=True)
    df = df[pd.to_numeric(df["ts"], errors="coerce").notna()].copy()
    df[NUMERIC] = df[NUMERIC].apply(pd.to_numeric, errors="coerce")
    df[CATEGORICAL] = df[CATEGORICAL].astype("category")
    add_features(df)
    return df
