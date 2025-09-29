# -*- coding: utf-8 -*-
"""
Created on Mon Sep 29 13:22:56 2025

@author: GGPC
"""

import streamlit as st
import pandas as pd
import io
from datetime import datetime, timedelta
import os
import sys
from audit.evaluator import evaluate_dataframe
from audit.summary import compute_summary
from validators import infer_pair_from_df_or_filename
from ingest.rate_fetcher import fetch_actual_rate

ROOT = os.path.abspath(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

st.set_page_config(page_title="Hedge Audit Demo", layout="wide")

st.title("Hedge Audit Demo")

with st.sidebar:
    st.header("Options")
    infer_pair = st.checkbox("Infer currency pair from file (if no Pair column)", value=True)
    use_yesterday = st.checkbox("Use yesterday 23:59 UTC for rate fetch (avoid midnight ambiguity)", value=True)
    show_preview_rows = st.slider("Preview rows", min_value=5, max_value=200, value=50, step=5)
    st.markdown("---")
    st.markdown("Sample CSV: header should include")
    st.code("Timestamp,Predicted_Rate,Live_Rate,Decision,Pair")

uploaded = st.file_uploader("Upload hedge CSV", type=["csv"])
col1, col2 = st.columns([2, 1])

with col1:
    actual_input = st.text_input("Actual rate (optional)", help="Enter a numeric rate (e.g., 0.61123). Leave blank to fetch by pair.")
    base = st.text_input("Base currency (optional)", max_chars=3, help="Use ISO code like NZD")
    quote = st.text_input("Quote currency (optional)", max_chars=3, help="Use ISO code like USD")
    run = st.button("Run audit")

with col2:
    st.markdown("Quick actions")
    if st.button("Load sample CSV"):
        try:
            sample_df = pd.read_csv("tests/sample_files/good.csv")
            st.experimental_set_query_params(_loaded="sample")
            st.session_state["_sample_df"] = sample_df
            st.success("Sample CSV loaded (use Run audit to execute).")
        except Exception as e:
            st.error(f"Failed to load sample: {e}")

def _parse_actual(text: str):
    try:
        if text is None or text.strip() == "":
            return None
        return float(text.strip())
    except Exception:
        return None

def _display_error(msg: str):
    st.error(msg)
    st.stop()

if run:
    # obtain DataFrame
    if "_sample_df" in st.session_state and st.session_state.get("_sample_df") is not None and uploaded is None:
        df = st.session_state["_sample_df"]
    elif uploaded is not None:
        try:
            df = pd.read_csv(io.BytesIO(uploaded.read()))
        except Exception as e:
            _display_error(f"Failed to parse uploaded CSV: {e}")
    else:
        _display_error("Please upload a CSV or load the sample CSV.")

    # validate basic shape
    ok, missing = (True, [])  # light validation here; detailed validation occurs in backend
    # determine actual rate
    actual_rate = _parse_actual(actual_input)
    if actual_rate is None:
        # explicit base/quote provided
        if base and quote:
            pair = (base.upper().strip(), quote.upper().strip())
        elif infer_pair:
            pair = infer_pair_from_df_or_filename(df, uploaded.name if uploaded else "sample.csv")
        else:
            pair = None

        if pair is None:
            _display_error("No actual rate provided and unable to determine currency pair. Provide actual rate or base+quote.")
        # fetch rate (best-effort)
        try:
            actual_rate = fetch_actual_rate(pair[0], pair[1], as_of_yesterday=use_yesterday)
        except Exception as e:
            _display_error(f"Rate fetch failed for {pair}: {e}")
        if actual_rate is None:
            _display_error(f"Rate provider returned no rate for {pair}.")

    # run evaluation
    try:
        audited = evaluate_dataframe(df, actual_rate=actual_rate, fill_missing_only=True)
        summary = compute_summary(audited, by_pair=True)
    except Exception as e:
        _display_error(f"Audit failed: {e}")

    st.success("Audit complete")
    st.markdown("### Summary")
    st.json(summary)

    st.markdown("### Preview")
    st.dataframe(audited.head(show_preview_rows))

    csv_bytes = audited.to_csv(index=False).encode("utf-8")
    st.download_button("Download audited CSV", data=csv_bytes, file_name="audited.csv", mime="text/csv")

    # optional: small PDF summary (if you add report generator later)
    st.markdown("---")
    st.caption(f"Rate used: {actual_rate} · Rows: {len(audited)} · Generated: {datetime.utcnow().isoformat()}Z")
