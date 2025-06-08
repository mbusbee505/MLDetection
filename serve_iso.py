#!/usr/bin/env python3
from __future__ import annotations
import os, time, paramiko, orjson, pandas as pd, pathlib, joblib
from mldetection.features import NUMERIC, CATEGORICAL, add_features

# env vars (required)
from dotenv import load_dotenv
load_dotenv()
HOST = os.environ["ZEEK_HOST"]
USER = os.environ["ZEEK_USER"]
PASS = os.environ["ZEEK_PASS"]
REMOTE_FILE = os.getenv("REMOTE_CONN_LOG", "/opt/zeek/logs/current/conn.log")
THRESH = float(os.getenv("IDS_THRESHOLD", "-0.15"))

MODEL_PATH = pathlib.Path("ids_iso.joblib")
pipe = joblib.load(MODEL_PATH)

def ssh_tail(host: str, user: str, pwd: str, remote_file: str):
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(host, username=user, password=pwd)
    cmd = f"tail -F {remote_file}"
    ch = cli.get_transport().open_session()
    ch.exec_command(cmd)
    buf = b""
    while True:
        if ch.recv_ready():
            buf += ch.recv(4096)
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                yield line.decode(errors="ignore")
        else:
            time.sleep(0.2)

for raw in ssh_tail(HOST, USER, PASS, REMOTE_FILE):
    try:
        row = orjson.loads(raw)
    except ValueError:
        continue

    df = pd.DataFrame([row])
    add_features(df)

    required_cols = set(NUMERIC + CATEGORICAL)
    if not required_cols.issubset(df.columns) or df[required_cols].isna().any(axis=None):
        continue                      # skip incomplete rows

    score = pipe.decision_function(df[NUMERIC + CATEGORICAL])[0]
    if score < THRESH:
        print(f"ALERT ts={row.get('ts')} uid={row.get('uid')} "
              f"{row.get('id.orig_h')}->{row.get('id.resp_h')} score={score:.2f}",
              flush=True)