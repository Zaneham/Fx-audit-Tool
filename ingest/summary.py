# -*- coding: utf-8 -*-


from typing import Dict, Optional
import pandas as pd

DEFAULT_ROUND = 6

def _safe_mean(series: pd.Series) -> Optional[float]:
    s = series.dropna()
    return float(s.mean()) if not s.empty else None

def _safe_rmse(series: pd.Series) -> Optional[float]:
    s = series.dropna()
    return float((s**2).mean() ** 0.5) if not s.empty else None

def _get_date_range(df: pd.DataFrame, ts_col: str = "Timestamp") -> Optional[Dict[str, str]]:
    if ts_col not in df.columns:
        return None
    try:
        ts = pd.to_datetime(df[ts_col], errors="coerce").dropna()
        if ts.empty:
            return None
        return {"min": ts.min().isoformat(), "max": ts.max().isoformat()}
    except Exception:
        return None

def compute_summary(df: pd.DataFrame, round_digits: int = DEFAULT_ROUND, by_pair: bool = False) -> Dict:
    # defensive copy
    df = df.copy()

    # ensure cols exist
    for c in ["Actual", "Error", "CorrectDirection", "HedgeOutcome", "Pair"]:
        if c not in df.columns:
            df[c] = pd.NA

    total = int(len(df))
    rows_evaluated = int(df["Actual"].notna().sum())

    mean_error = _safe_mean(df["Error"])
    rmse = _safe_rmse(df["Error"])
    directional_acc = _safe_mean(df["CorrectDirection"].astype("float", errors="ignore")) if "CorrectDirection" in df else None

    profitable_hedges = int((df["HedgeOutcome"] == "Profitable").sum())
    missed_hedges = int((df["HedgeOutcome"] == "Should've Hedged").sum())

    percent_profitable = round((profitable_hedges / rows_evaluated) * 100, 4) if rows_evaluated else None
    percent_missing_actuals = round(((total - rows_evaluated) / total) * 100, 4) if total else None

    date_range = _get_date_range(df, "Timestamp")

    summary = {
        "total_rows": total,
        "rows_evaluated": rows_evaluated,
        "percent_missing_actuals": percent_missing_actuals,
        "mean_error": round(mean_error, round_digits) if mean_error is not None else None,
        "rmse": round(rmse, round_digits) if rmse is not None else None,
        "directional_accuracy": round(directional_acc, round_digits) if directional_acc is not None else None,
        "profitable_hedges": profitable_hedges,
        "missed_hedges": missed_hedges,
        "percent_profitable": percent_profitable,
        "date_range": date_range
    }

    if by_pair:
        pair_grp = {}
        for pair, sub in df.groupby(df["Pair"].fillna("UNKNOWN")):
            pair_summary = compute_summary(sub, round_digits=round_digits, by_pair=False)
            pair_grp[str(pair)] = pair_summary
        summary["by_pair"] = pair_grp

    return summary
