# mldetection/io.py

from __future__ import annotations

import os
import pathlib
import re
import gzip
import shutil
from stat import S_ISDIR
from typing import Iterable

import paramiko
from dotenv import load_dotenv

__all__ = [
    "download_day",
]

_bad = re.compile(r'[<>:"/\\|?*]')

def _safe(name: str) -> str:
    return _bad.sub("-", name)

load_dotenv()
_ZEEK_HOST = os.getenv("ZEEK_HOST")
_ZEEK_USER = os.getenv("ZEEK_USER")
_ZEEK_PASS = os.getenv("ZEEK_PASS")
_ZEEK_REMOTE_ROOT = os.getenv("ZEEK_REMOTE_ROOT", "/opt/zeek/logs")

if not all([_ZEEK_HOST, _ZEEK_USER, _ZEEK_PASS]):
    raise RuntimeError(
        "ZEEK_HOST, ZEEK_USER, ZEEK_PASS must be set in .env before using download_day()"
    )


def _fetch_file(sftp, rfile: str, lfile: pathlib.Path):
    if rfile.endswith(".gz"):
        target = lfile.with_suffix("")               # remove .gz
        if target.suffix == ".gz":                   # handles .log.gz → .log
            target = target.with_suffix("")
        with sftp.open(rfile, "rb") as rf, gzip.open(rf) as gz, target.open("wb") as lf:
            shutil.copyfileobj(gz, lf)
    else:
        sftp.get(rfile, str(lfile))



def _copy_dir(sftp: paramiko.SFTPClient, rpath: str, lpath: pathlib.Path):
    lpath.mkdir(parents=True, exist_ok=True)
    for entry in sftp.listdir_attr(rpath):
        rfile = f"{rpath}/{entry.filename}"
        lfile = lpath / _safe(entry.filename)
        if S_ISDIR(entry.st_mode):
            _copy_dir(sftp, rfile, lfile)
        else:
            _fetch_file(sftp, rfile, lfile)

def download_day(day_dirname: str | pathlib.Path, *, local_root: pathlib.Path) -> pathlib.Path:
    """Download a single YYYY‑MM‑DD folder from the Zeek sensor to *local_root*.

    The remote path is constructed as ``<REMOTE_ROOT>/<day_dirname>``.
    If files already exist locally they are skipped.
    Returns the local folder path.
    """
    day_dirname = pathlib.Path(day_dirname).name 
    local_day = local_root / _safe(day_dirname)
    if local_day.exists():
        print(f"[SKIP] {local_day} already exists – not re‑downloading")
        return local_day

    ssh = paramiko.Transport((_ZEEK_HOST, 22))
    ssh.connect(username=_ZEEK_USER, password=_ZEEK_PASS)
    sftp = paramiko.SFTPClient.from_transport(ssh)

    try:
        remote_day = f"{_ZEEK_REMOTE_ROOT}/{day_dirname}"
        _copy_dir(sftp, remote_day, local_day)
    finally:
        sftp.close(); ssh.close()

    print(f"[OK] Downloaded → {local_day}")
    return local_day

if __name__ == "__main__":
    import argparse
    import datetime as dt

    p = argparse.ArgumentParser(description="Download a Zeek log day")
    p.add_argument("day", nargs="?", default=(dt.date.today() - dt.timedelta(days=1)).isoformat())
    p.add_argument("--local-root", default="logs", type=pathlib.Path)
    ns = p.parse_args()
    download_day(ns.day, local_root=ns.local_root)



"""
Usage (after installing requirements and creating .env):

    python -m mldetection.io 2025-06-06 --local-root logs
"""
