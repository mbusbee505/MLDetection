from __future__ import annotations
import gzip, pandas as pd, pathlib

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
    # choose text or binary opener depending on .gz
    opener_bin  = gzip.open if path.suffix == ".gz" else open
    opener_text = gzip.open if path.suffix == ".gz" else open

    with opener_bin(path, "rb") as fh:
        first = fh.readline().lstrip()

    if first.startswith(b"{"):                     # JSON (Zeek with @load json-logs)
        with opener_text(path, "rt", encoding="utf-8", errors="ignore") as fh:
            return pd.read_json(fh, lines=True)
    else:                                          # TSV
        with opener_text(path, "rt", encoding="utf-8", errors="ignore") as fh:
            return pd.read_csv(fh, sep="\t", comment="#", low_memory=False)


def add_features(df: pd.DataFrame) -> None:
    if {"orig_bytes", "resp_bytes"}.issubset(df.columns):
        df["bytes_ratio"] = (df["orig_bytes"].fillna(0) + 1) / (
            df["resp_bytes"].fillna(0) + 1
        )
    else:
        # mark as unprocessable; scorer will skip it
        df["bytes_ratio"] = pd.NA

    if {"orig_pkts", "resp_pkts"}.issubset(df.columns):
        df["pkts_total"] = df["orig_pkts"].fillna(0) + df["resp_pkts"].fillna(0)
    else:
        df["pkts_total"] = pd.NA

    df["hour"] = pd.to_datetime(df.get("ts", pd.NA), unit="s", errors="coerce").dt.hour


NUMERIC_EXT = NUMERIC + ["bytes_ratio", "pkts_total", "hour"]

def load_day(day_dir: pathlib.Path) -> pd.DataFrame:
    files = [f for f in sorted(day_dir.glob("conn*.log*")) if not f.name.endswith(".idx")]
    df = pd.concat([read_file(f) for f in files], ignore_index=True)
    df = df[pd.to_numeric(df["ts"], errors="coerce").notna()].copy()
    df[NUMERIC] = df[NUMERIC].apply(pd.to_numeric, errors="coerce")
    df[CATEGORICAL] = df[CATEGORICAL].astype("category")
    add_features(df)
    return df
