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
    provider: str = DEFAULT_PROVIDER,
    as_of_yesterday: bool = False,
) -> Optional[float]:
    """
    Fetches exchange rate base->quote.
    - as_of: timezone-naive UTC datetime to request historical rate; if None, fetches latest.
    - as_of_yesterday: when True, overrides as_of to yesterday 23:59 UTC to avoid midnight ambiguity.
    - Uses simple shelve-backed cache and retry/backoff logic.
    Returns float rate (base->quote), or None on failure.
    """
    base = base.upper().strip()
    quote = quote.upper().strip()

    if as_of_yesterday:
        as_of = (datetime.utcnow() - timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0)

    cache_key = _cache_key(base, quote, as_of)
    cached = _read_cache(cache_key)
    if cached is not None and not math.isnan(cached):
        return cached

    # Build request: exchangerate.host supports /{date}?base=BASE&symbols=QUOTE or /latest
    if as_of:
        date_str = as_of.strftime("%Y-%m-%d")
        url = f"{provider}/{date_str}"
    else:
        url = f"{provider}/latest"

    params = {"base": base, "symbols": quote}

    backoff = 1.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            body = resp.json()
            # Expected shape for exchangerate.host: {"success": true, "rates": {"USD": 1.23}, ...}
            rates = body.get("rates") or {}
            rate = rates.get(quote)
            if rate is None:
                # Some providers use "result" or different shapes; try common fallbacks
                if "result" in body:
                    rate = body.get("result")
                else:
                    rate = None
            if rate is not None:
                rate = float(rate)
                _write_cache(cache_key, rate)
                return rate
            # if no rate, raise to trigger retry/fallback
            raise ValueError(f"No rate found in provider response for {base}/{quote}")
        except Exception as exc:
            # last attempt -> give up
            if attempt == MAX_RETRIES:
                return None
            # exponential backoff
            time.sleep(backoff)
            backoff *= RETRY_BACKOFF
            continue

    return None
