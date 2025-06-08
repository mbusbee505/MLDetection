#!/usr/bin/env python3
from __future__ import annotations
import os, time, paramiko, orjson, pandas as pd, pathlib, joblib
from mldetection.features import NUMERIC, CATEGORICAL, add_features
import requests, json, datetime as dt, os, itertools, time
from requests.exceptions import RequestException
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
ES_URL = os.getenv("ES_URL")          # e.g. https://elk:9200
ES_API = os.getenv("ES_API_KEY")      # base64 api key
ES_USER = os.getenv("ES_USER")
ES_PASS = os.getenv("ES_PASS")
IDX_PREFIX = "mlids"   
headers = {"Content-Type": "application/json"}
if ES_API:
    headers["Authorization"] = f"ApiKey {ES_API}"


def backoff():
    yield from (0, 1, 2, 4, 8, 16, 16, 16)

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

def send_to_es(doc: dict):
    index = f"{IDX_PREFIX}-{datetime.date.today():%Y.%m.%d}"
    url   = f"{ES_URL}/{index}/_doc"
    payload = json.dumps(doc).encode()

    for delay in backoff():
        try:
            if delay:
                time.sleep(delay)
            r = requests.post(
                url, data=payload, headers=headers,
                auth=(ES_USER, ES_PASS) if ES_USER else None,
                timeout=3, verify=True  # set verify=False only for a self-signed lab cert
            )
            r.raise_for_status()
            return
        except RequestException as e:
            # Log locally, then retry; final failure just drops the alert
            print(f"ES post failed ({e}); retry in {delay}s", file=sys.stderr)
    print("dropped alert after retries", file=sys.stderr)

for raw in ssh_tail(HOST, USER, PASS, REMOTE_FILE):
    try:
        row = orjson.loads(raw)
    except ValueError:
        continue

    df = pd.DataFrame([row])
    add_features(df)

    required_cols = NUMERIC + CATEGORICAL          # list
    have_all_cols = set(required_cols).issubset(df.columns)
    if not have_all_cols:
        continue

    if df[required_cols].isna().any(axis=None):    # now safe—list indexer
        continue


    score = pipe.decision_function(df[NUMERIC + CATEGORICAL])[0]
    if score < THRESH:
        
        ALERT = {
            "ts": row.get("ts"),
            "uid": row.get("uid"),
            "src": row.get("id.orig_h"),
            "src_p": row.get("id.orig_p"),
            "dst": row.get("id.resp_h"),
            "dst_p": row.get("id.resp_p"),
            "proto": row.get("proto"),
            "score": score,
}

        send_to_es(ALERT)               # ⇦ pushes to Elasticsearch
        print(json.dumps(ALERT)) 