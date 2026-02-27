"""Interactive scratchpad for scanned options analysis.

Edit variables in the CONFIG block, then run this file.
No CLI parser, no main, no local function definitions.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analyze_scanned_options import build_signal_table

# =========================
# CONFIG
# =========================
RAW_OPTIONS_PATH = "data/raw_options.csv"
CM_IV_PATH = "data/constant_maturity_iv.csv"
AS_OF_DATE = None  # Example: "2026-02-24" or None (today)

TARGET_DTE = 30
MIN_DTE = 7
MAX_DTE = 120
MIN_LEG_VOLUME = 0.0
MIN_LEG_OPEN_INTEREST = 0.0
MAX_SPREAD_PCT_MID = 0.25
TOP_N = 150

# Extra interactive screen on top of build_signal_table output
TOP_VIEW_N = 15
MAX_DISPLAY_SPREAD = 0.30
EXPORT_PATH = "data/analysis_interactive_table.csv"

# =========================
# RUN BASE ANALYSIS
# =========================
table = build_signal_table(
    raw_options_path=RAW_OPTIONS_PATH,
    cm_iv_path=CM_IV_PATH,
    as_of_date=AS_OF_DATE,
    target_dte=TARGET_DTE,
    min_dte=MIN_DTE,
    max_dte=MAX_DTE,
    min_leg_volume=MIN_LEG_VOLUME,
    min_leg_open_interest=MIN_LEG_OPEN_INTEREST,
    max_spread_pct_mid=MAX_SPREAD_PCT_MID,
    top_n=TOP_N,
)

print(f"Rows after base analysis: {len(table)}")
if table.empty:
    print("No rows. Relax filters in CONFIG.")
else:
    print("Side counts:")
    print(table["side"].value_counts(dropna=False).to_string())

# =========================
# INTERACTIVE PLAYGROUND
# =========================
if not table.empty:
    working = table.copy()

    # Optional extra filter for exploration only
    working = working[working["spread_pct_mid"] <= MAX_DISPLAY_SPREAD].copy()
    print(f"\nRows after extra spread filter (<= {MAX_DISPLAY_SPREAD:.2f}): {len(working)}")

    # Rank views
    long_view = working[working["side"] == "long_straddle"].sort_values(
        "iv_slope", ascending=False
    )
    short_view = working[working["side"] == "short_straddle"].sort_values("iv_slope")

    cols = [
        "symbol",
        "side",
        "iv_slope",
        "IV30D",
        "IV180D",
        "days_to_expiry",
        "straddle_mid",
        "spread_pct_mid",
        "call_volume",
        "put_volume",
        "call_open_interest",
        "put_open_interest",
        "min_leg_volume",
        "min_leg_open_interest",
    ]
    cols = [c for c in cols if c in working.columns]

    print("\nTop long candidates:")
    print(long_view[cols].head(TOP_VIEW_N).to_string(index=False))

    print("\nTop short candidates:")
    print(short_view[cols].head(TOP_VIEW_N).to_string(index=False))

    # Decile cut for quick factor inspection
    finite_slope = np.isfinite(working["iv_slope"])
    dec_df = working[finite_slope].copy()
    if len(dec_df) >= 10 and dec_df["iv_slope"].nunique() > 1:
        dec_df["iv_slope_decile"] = (
            pd.qcut(dec_df["iv_slope"], 10, labels=False, duplicates="drop") + 1
        )
        dec_summary = (
            dec_df.groupby("iv_slope_decile", as_index=False)
            .agg(
                n=("symbol", "count"),
                mean_slope=("iv_slope", "mean"),
                median_slope=("iv_slope", "median"),
                mean_spread=("spread_pct_mid", "mean"),
                mean_min_leg_oi=("min_leg_open_interest", "mean"),
            )
            .sort_values("iv_slope_decile")
        )
        print("\nDecile summary:")
        print(dec_summary.to_string(index=False))
    else:
        print("\nNot enough variation for decile summary.")

    # Save current working table for further analysis elsewhere
    working.to_csv(EXPORT_PATH, index=False)
    print(f"\nSaved interactive table to: {EXPORT_PATH}")

    # =========================
    # OPTIONAL PLOTS
    # =========================
    try:
        import matplotlib.pyplot as plt

        fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))

        axes[0].hist(working["iv_slope"].dropna(), bins=25, edgecolor="black")
        axes[0].set_title("IV Slope Distribution")
        axes[0].set_xlabel("iv_slope")
        axes[0].set_ylabel("count")

        colors = working["side"].map(
            {"long_straddle": "tab:green", "short_straddle": "tab:red", "none": "tab:gray"}
        ).fillna("tab:blue")
        axes[1].scatter(working["iv_slope"], working["spread_pct_mid"], c=colors, alpha=0.75)
        axes[1].set_title("IV Slope vs Spread % Mid")
        axes[1].set_xlabel("iv_slope")
        axes[1].set_ylabel("spread_pct_mid")

        fig.tight_layout()
        plt.show()
    except ModuleNotFoundError:
        print("matplotlib not installed. Install with: pip install matplotlib")

