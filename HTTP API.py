# -*- coding: utf-8 -*-
"""
Created on Mon Sep 29 12:54:41 2025

@author: GGPC
"""

# -*- coding: utf-8 -*-
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional, Tuple
import pandas as pd
import io
import traceback

from audit.evaluator import evaluate_dataframe
from audit.summary import compute_summary
from validators import validate_schema, infer_pair_from_df_or_filename
from ingest.rate_fetcher import fetch_actual_rate  # implement as discussed

app = FastAPI(title="Hedge Audit Service")

def _read_csv_bytes(contents: bytes) -> pd.DataFrame:
    try:
        return pd.read_csv(io.BytesIO(contents))
    except Exception as e:
        raise ValueError(f"Failed to parse CSV: {e}")

@app.post("/audit")
async def audit_csv(
    file: UploadFile = File(...),
    actual_rate: Optional[float] = Form(None),
    base: Optional[str] = Form(None),
    quote: Optional[str] = Form(None),
    as_of_yesterday: Optional[bool] = Form(False),
):
    """
    Upload a hedge log CSV and return an audit summary and a preview of the audited rows.

    You can either:
      - provide actual_rate directly, or
      - provide base+quote (e.g., NZD, USD) so the service fetches the rate, or
      - omit both and allow pair inference from the file (if a Pair column or filename pattern exists).
    """
    contents = await file.read()
    try:
        df = _read_csv_bytes(contents)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Basic schema validation
    ok, missing = validate_schema(df)
    if not ok:
        raise HTTPException(status_code=400, detail=f"Missing required columns: {missing}")

    # Determine actual_rate
    rate = actual_rate
    if rate is None:
        # priority: explicit base+quote -> infer from file -> fail
        pair: Optional[Tuple[str, str]] = None
        if base and quote:
            pair = (base.upper().strip(), quote.upper().strip())
        else:
            pair = infer_pair_from_df_or_filename(df, file.filename)

        if pair is None:
            raise HTTPException(status_code=400, detail="No actual_rate supplied and unable to infer currency pair; provide actual_rate or base+quote.")
        try:
            # as_of handling to avoid midnight ambiguity can be implemented inside fetch_actual_rate
            rate = fetch_actual_rate(pair[0], pair[1], as_of_yesterday=as_of_yesterday)
        except Exception as e:
            # return helpful error instead of raw stack trace
            raise HTTPException(status_code=502, detail=f"Failed to fetch rate for pair {pair}: {e}")

    # Evaluate
    try:
        audited = evaluate_dataframe(df, actual_rate=rate, fill_missing_only=True)
        summary = compute_summary(audited, by_pair=True)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Audit evaluation failed: {e}")

    preview = audited.head(50).to_dict(orient="records")
    # Return summary, preview, and some metadata
    response = {
        "summary": summary,
        "preview": preview,
        "meta": {
            "rows": len(audited),
            "rate_used": rate,
        }
    }
    return JSONResponse(content=response)
