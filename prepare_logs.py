from __future__ import annotations
import pathlib, datetime as dt, argparse, mldetection.features as feat

def main():
    p = argparse.ArgumentParser()
    p.add_argument("day", nargs="?", default=(dt.date.today() - dt.timedelta(days=1)).isoformat())
    p.add_argument("--log-root", default="logs", type=pathlib.Path)
    p.add_argument("--out", default="parquet", type=pathlib.Path)
    ns = p.parse_args()

    day_dir = ns.log_root / ns.day
    df = feat.load_day(day_dir)
    ns.out.mkdir(parents=True, exist_ok=True)
    out_path = ns.out / f"{ns.day}.parquet"
    df.to_parquet(out_path)
    print(out_path)

if __name__ == "__main__":
    main()
