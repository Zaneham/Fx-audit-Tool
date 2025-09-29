# streamlit_app.py
# -*- coding: utf-8 -*-
"""
Streamlit front-end for Hedge Audit Demo
"""

import os
import sys
from datetime import datetime
import io

# Ensure repository root (folder containing this file) is on sys.path
ROOT = os.path.abspath(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import streamlit as st
import pandas as pd

from validators import infer_pair_from_df_or_filename, validate_schema
from audit.evaluator import evaluate_dataframe
from audit.summary import compute_summary
from ingest.rate_fetcher import fetch_actual_rate

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
            st.session_state["_sample_df"] = sample_df
            st.success("Sample CSV loaded (use Run audit to execute).")
        except Exception as e:
            st.error(f"Failed to load sample: {e}")

def _parse_actual(text: str):
    try:
        if text is None or str(text).strip() == "":
            return None
        return float(str(text).strip())
    except Exception:
        return None

def _display_error(msg: str):
    st.error(msg)
    raise RuntimeError(msg)

@st.cache_data(ttl=60 * 60)
def _cached_fetch_rate(base: str, quote: str, use_yesterday_flag: bool):
    return fetch_actual_rate(base, quote, as_of_yesterday=use_yesterday_flag)

# Main audit logic
if run:
    audit_success = False
    summary = None
    audited = None

    # Load DataFrame
    if "_sample_df" in st.session_state and st.session_state.get("_sample_df") is not None and uploaded is None:
        df = st.session_state["_sample_df"]
        filename = "sample.csv"
    elif uploaded is not None:
        try:
            df = pd.read_csv(io.BytesIO(uploaded.read()))
            filename = getattr(uploaded, "name", "uploaded.csv") or "uploaded.csv"
        except Exception as e:
            _display_error(f"Failed to parse uploaded CSV: {e}")
    else:
        _display_error("Please upload a CSV or load the sample CSV.")

    # Validate schema
    ok, missing = validate_schema(df)
    if not ok:
        _display_error(f"CSV missing required columns: {', '.join(missing)}")

    actual_rate = _parse_actual(actual_input)

    # Infer pair if needed
    pair = None
    if actual_rate is None:
        if base and quote:
            pair = (base.upper().strip(), quote.upper().strip())
        elif infer_pair:
            try:
                pair = infer_pair_from_df_or_filename(df, filename)
            except Exception as e:
                _display_error(f"Failed to infer pair: {e}")
        else:
            pair = None

        if pair is None:
            _display_error("No actual rate provided and unable to determine currency pair. Provide actual rate or base+quote.")

    # Run audit
    try:
        with st.spinner("Fetching rate and evaluating..."):
            if actual_rate is None:
                actual_rate = _cached_fetch_rate(pair[0], pair[1], use_yesterday)
                if actual_rate is None:
                    _display_error(f"Rate provider returned no rate for {pair}.")

            audited = evaluate_dataframe(df, actual_rate=actual_rate, fill_missing_only=True)
            summary = compute_summary(audited, by_pair=True)
            audit_success = True
    except RuntimeError:
        audit_success = False
    except Exception as e:
        st.error(f"Unexpected error during audit: {e}")
        audit_success = False
        raise

    # Display results only if audit succeeded
    if audit_success:
        st.success("Audit complete")
        st.markdown("### Summary")
        st.json(summary)

        st.markdown("### Preview")
        st.dataframe(audited.head(show_preview_rows))

        csv_bytes = audited.to_csv(index=False).encode("utf-8")
        st.download_button("Download audited CSV", data=csv_bytes, file_name="audited.csv", mime="text/csv")

        st.markdown("---")
        st.caption(f"Rate used: {actual_rate} · Rows: {len(audited)} · Generated: {datetime.utcnow().isoformat()}Z")
