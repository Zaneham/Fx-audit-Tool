# -*- coding: utf-8 -*-
"""
Created on Mon Sep 29 13:06:12 2025

@author: GGPC
"""

# validators.py
from typing import List, Optional, Tuple
import re
import pandas as pd


REQUIRED_COLUMNS = ["Timestamp", "Predicted_Rate", "Live_Rate", "Decision"]


def validate_schema(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Check that required columns are present (case/space tolerant).
    Returns (ok, missing_columns).
    """
    cols_norm = {c.strip().lower().replace(" ", "_"): c for c in df.columns}
    missing = []
    for req in REQUIRED_COLUMNS:
        key = req.strip().lower().replace(" ", "_")
        if key not in cols_norm:
            missing.append(req)
    return (len(missing) == 0, missing)


_FILENAME_PAIR_REGEXES = [
    re.compile(r"([A-Za-z]{3})[_-]?([A-Za-z]{3})", re.IGNORECASE),   # nzdusd, nzd_usd, NZD-USD
    re.compile(r"([A-Za-z]{3})/([A-Za-z]{3})", re.IGNORECASE),       # NZD/USD
]


def _normalize_pair_tuple(a: str, b: str) -> Tuple[str, str]:
    return (a.upper().strip(), b.upper().strip())


def infer_pair_from_df_or_filename(df: pd.DataFrame, filename: Optional[str] = None) -> Optional[Tuple[str, str]]:
    """
    Attempt to infer currency pair in priority:
      1) 'Pair' column (first non-null entry like 'NZD/USD' or 'nzdusd')
      2) Filename patterns (nzdusd, nzd_usd, NZD-USD, NZD/USD)
    Returns (BASE, QUOTE) or None.
    """
    # 1) Try Pair column
    if "Pair" in df.columns:
        col = df["Pair"].dropna().astype(str)
        if not col.empty:
            first = col.iloc[0].strip()
            parsed = _parse_pair_string(first)
            if parsed:
                return parsed

    # 2) Try other columns that might contain pair info
    for alt in ["pair", "currency_pair", "pair_name"]:
        if alt in (c.lower() for c in df.columns):
            series = df[[c for c in df.columns if c.lower() == alt][0]].dropna().astype(str)
            if not series.empty:
                parsed = _parse_pair_string(series.iloc[0].strip())
                if parsed:
                    return parsed

    # 3) Try filename
    if filename:
        base = _parse_pair_string(filename)
        if base:
            return base

    return None


def _parse_pair_string(s: str) -> Optional[Tuple[str, str]]:
    """Parse strings like NZDUSD, NZD_USD, NZD-USD, NZD/USD, nzd/usd, 'NZD USD'."""
    s = s.strip()
    # common separators
    for sep in [" ", "_", "-", "/"]:
        if sep in s:
            parts = [p for p in re.split(r"[_\-/\s]+", s) if p]
            if len(parts) >= 2 and all(len(p) == 3 for p in parts[:2]):
                return _normalize_pair_tuple(parts[0], parts[1])

    # contiguous 6-letter code e.g., nzdusd or NZDUSD
    m = re.match(r"^([A-Za-z]{6})$", s)
    if m:
        code = m.group(1)
        return _normalize_pair_tuple(code[:3], code[3:6])

    # try regex search inside string (for filenames)
    for rx in _FILENAME_PAIR_REGEXES:
        m = rx.search(s)
        if m:
            a, b = m.group(1), m.group(2)
            if len(a) == 3 and len(b) == 3:
                return _normalize_pair_tuple(a, b)

    return None
