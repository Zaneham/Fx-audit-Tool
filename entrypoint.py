# -*- coding: utf-8 -*-
"""
Created on Mon Sep 29 12:53:27 2025

@author: Zane
"""
# -*- coding: utf-8 -*-
"""
CLI wrapper for running a hedge audit on one or more CSV files.
Usage examples:
  python entrypoint.py --file hedge_log_nzdusd.csv --actual 0.61123
  python entrypoint.py --file hedge_log_nzdusd.csv --infer-pair --as-of-yesterday
"""

import argparse
import sys
from datetime import datetime, timedelta
import pandas as pd

from audit.evaluator import evaluate_dataframe
from audit.summary import compute_summary
from ingest.rate_fetcher import fetch_actual_rate  # implement this per earlier plan
from validators import infer_pair_from_df_or_filename  # implement this helper

def _read_csv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception as e:
        raise SystemExit(f"Failed to read CSV {path}: {e}")

def _write_csv(df: pd.DataFrame, out_path: str) -> None:
    df.to_csv(out_path, index=False)

def main():
    p = argparse.ArgumentParser(description="Run hedge audit on a CSV")
    p.add_argument("--file", "-f", required=True, nargs="+", help="Path(s) to hedge log CSV")
    p.add_argument("--actual", "-a", type=float, help="Actual rate to use for evaluation (optional)")
    p.add_argument("--infer-pair", action="store_true", help="Infer currency pair from file or data when actual not provided")
    p.add_argument("--as-of-yesterday", action="store_true", help="If inferring rate, fetch rate as of yesterday (23:59) instead of now")
    args = p.parse_args()

    for path in args.file:
        print(f"Processing: {path}")
        df = _read_csv(path)

        actual = args.actual
        if actual is None and args.infer_pair:
            pair = infer_pair_from_df_or_filename(df, path)
            if pair is None:
                print(f"Could not infer pair for {path}; skipping. Provide --actual or add Pair column.", file=sys.stderr)
                continue
            base, quote = pair
            as_of = None
            if args.as_of_yesterday:
                as_of = (datetime.utcnow() - timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0)
            try:
                actual = fetch_actual_rate(base, quote, as_of=as_of)
            except Exception as e:
                print(f"Rate fetch failed for {base}/{quote}: {e}", file=sys.stderr)
                continue
            if actual is None:
                print(f"Rate fetch returned no value for {base}/{quote}; skipping {path}", file=sys.stderr)
                continue

        if actual is None:
            print("Pass --actual <rate> or use --infer-pair to fetch a rate automatically.", file=sys.stderr)
            continue

        audited = evaluate_dataframe(df, actual_rate=actual, fill_missing_only=True)
        summary = compute_summary(audited, by_pair=True)
        out_path = path.replace(".csv", ".audited.csv")
        _write_csv(audited, out_path)

        # print concise human-friendly summary
        print("Summary:", summary)
        print("Saved audited CSV to", out_path)

if __name__ == "__main__":
    main()


