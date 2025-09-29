# -*- coding: utf-8 -*-
"""
Created on Mon Sep 29 12:44:56 2025

@author: Zane Hambly
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict

REQUIRED_COLUMNS = ["Timestamp", "Predicted_Rate", "Live_Rate", "Decision"]

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.replace(" ", "_")
    for col in ["Actual", "Error", "CorrectDirection", "HedgeOutcome", "Pair"]:
        if col not in df.columns:
            df[col] = np.nan
    return df

def evaluate_row(row: pd.Series, actual_rate: Optional[float]) -> pd.Series:
    out = row.copy()
    if pd.isna(out.get("Predicted_Rate")) or pd.isna(out.get("Live_Rate")) or actual_rate is None:
        return out

    error = float(out["Predicted_Rate"]) - float(actual_rate)
    correct_direction = (float(out["Predicted_Rate"]) > float(out["Live_Rate"])) == (float(actual_rate) > float(out["Live_Rate"]))
    decision = str(out.get("Decision", "")).strip()

    if decision == "Hedge now":
        hedge_outcome = "Profitable" if actual_rate < float(out["Live_Rate"]) else "Missed"
    elif decision == "Wait":
        hedge_outcome = "Good Wait" if actual_rate > float(out["Live_Rate"]) else "Should've Hedged"
    else:
        hedge_outcome = "Unknown"

    out["Actual"] = actual_rate
    out["Error"] = error
    out["CorrectDirection"] = bool(correct_direction)
    out["HedgeOutcome"] = hedge_outcome
    return out

def evaluate_dataframe(df: pd.DataFrame, actual_rate: Optional[float], fill_missing_only: bool = True) -> pd.DataFrame:
    """
    Evaluate rows in df using actual_rate.
    - If fill_missing_only is True, only rows with Actual==NaN are evaluated.
    - Returns a new DataFrame (does not mutate input).
    """
    df = normalize_df(df)
    if fill_missing_only:
        mask = df["Actual"].isna()
    else:
        mask = pd.Series(True, index=df.index)

    for idx in df[mask].index:
        df.loc[idx] = evaluate_row(df.loc[idx], actual_rate)
    return df
