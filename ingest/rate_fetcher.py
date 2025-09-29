# -*- coding: utf-8 -*-
"""
Created on Mon Sep 29 13:06:32 2025

@author: GGPC
"""

# ingest/rate_fetcher.py
import time
from datetime import datetime, timedelta
from typing import Optional
import requests
import shelve
import os
import math

# Configuration
DEFAULT_PROVIDER = "https://api.exchangerate.host"
CACHE_PATH = os.getenv("RATE_CACHE_PATH", ".rate_cache.db")
CACHE_TTL_SECONDS = int(os.getenv("RATE_CACHE_TTL_SECONDS", str(60 * 60 * 24)))  # default 24h
REQUEST_TIMEOUT = 8  # seconds
MAX_RETRIES = 3
RETRY_BACKOFF = 1.5  # multiplier


def _cache_key(base: str, quote: str, as_of_dt: Optional[datetime]) -> str:
    date_key = as_of_dt.strftime("%Y-%m-%dT%H:%M") if as_of_dt else "latest"
    return f"{base.upper()}_{quote.upper()}_{date_key}"


def _read_cache(key: str) -> Optional[float]:
    try:
        with shelve.open(CACHE_PATH) as db:
            entry = db.get(key)
            if not entry:
                return None
            ts, val = entry.get("ts"), entry.get("val")
            if ts is None or val is None:
                return None
            if (time.time() - ts) > CACHE_TTL_SECONDS:
                return None
            return float(val)
    except Exception:
        return None


def _write_cache(key: str, value: float) -> None:
    try:
        with shelve.open(CACHE_PATH) as db:
            db[key] = {"ts": time.time(), "val": float(value)}
    except Exception:
        # swallow cache failures; caching is best-effort
        pass


def fetch_actual_rate(
    base: str,
    quote: str,
    as_of: Optional[datetime] = None,
    provider: str = "https://v6.exchangerate-api.com/v6",
    as_of_yesterday: bool = False,
) -> Optional[float]:
    base = base.upper().strip()
    quote = quote.upper().strip()
    api_key = os.getenv("FX_API_KEY")

    if not api_key:
        print("[ERROR] FX_API_KEY not found in environment.")
        return None

    if as_of_yesterday:
        print("[WARNING] ExchangeRate-API does not support historical rates. Using latest instead.")

    url = f"{provider}/{api_key}/latest/{base}"
    print(f"[FETCH] {url} → looking for {quote}")

    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        print(f"[RESPONSE] Status: {resp.status_code}")
        print(f"[RESPONSE] Body: {resp.text}")
        resp.raise_for_status()
        data = resp.json()
        rate = data.get("conversion_rates", {}).get(quote)
        if rate is not None:
            rate = float(rate)
            _write_cache(_cache_key(base, quote, None), rate)
            print(f"[SUCCESS] {base}/{quote} → {rate}")
            return rate
        print(f"[ERROR] No rate found for {base}/{quote}")
        return None
    except Exception as e:
        print(f"[FAILURE] Error fetching rate: {e}")
        return None



