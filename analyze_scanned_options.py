"""Analyze scanned option data and generate basic IV-slope straddle signals.

Uses:
- data/raw_options.csv (paired call/put contracts with bid/ask and liquidity)
- data/constant_maturity_iv.csv (IV30D/IV180D per symbol)

Workflow:
1) Pick one candidate contract per symbol: nearest target DTE and near-ATM strike.
2) Compute straddle price/spread and liquidity metrics.
3) Filter low-quality contracts by volume/open interest/spread.
4) Join constant-maturity IV and compute slope: (IV180D - IV30D) / IV180D.
5) Mark top-N slope as long straddles and bottom-N slope as short straddles.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

# Main analysis parameters (edit these defaults if needed)
DEFAULT_ANALYSIS = {
    "raw_options": "data/raw_options.csv",
    "constant_maturity_iv": "data/constant_maturity_iv.csv",
    "output": "data/straddle_signal_table.csv",
    "as_of_date": None,  # YYYY-MM-DD or None for today
    "target_dte": 30,
    "min_dte": 7,
    "max_dte": 90,
    "min_leg_volume": 0.0,
    "min_leg_open_interest": 0.0,
    "max_spread_pct_mid": 0.25,
    "top_n": 20,
}

DEFAULT_RAW_OPTIONS = DEFAULT_ANALYSIS["raw_options"]
DEFAULT_CM_IV = DEFAULT_ANALYSIS["constant_maturity_iv"]
DEFAULT_OUTPUT = DEFAULT_ANALYSIS["output"]


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _ensure_columns(df: pd.DataFrame, required: list[str], optional: list[str]) -> pd.DataFrame:
    missing_required = [c for c in required if c not in df.columns]
    if missing_required:
        raise ValueError(f"Missing required columns in raw options file: {missing_required}")

    out = df.copy()
    for col in optional:
        if col not in out.columns:
            out[col] = np.nan
    return out


def _pick_contract_per_symbol(
    raw_df: pd.DataFrame,
    as_of_date: date,
    target_dte: int,
    min_dte: int,
    max_dte: int,
) -> pd.DataFrame:
    df = raw_df.copy()
    df["expiry_date"] = pd.to_datetime(df["expiry"], format="%Y%m%d", errors="coerce").dt.date
    df = df.dropna(subset=["expiry_date"])
    df["days_to_expiry"] = (pd.to_datetime(df["expiry_date"]) - pd.Timestamp(as_of_date)).dt.days

    # Keep only contracts in the desired DTE window
    df = df[df["days_to_expiry"].between(min_dte, max_dte)]
    if df.empty:
        return df

    # Prefer near target DTE, then near ATM strike
    df["dte_diff"] = (df["days_to_expiry"] - target_dte).abs()
    df["atm_diff"] = (df["strike"] - df["stock_price"]).abs()
    df = df.sort_values(["symbol", "dte_diff", "atm_diff", "days_to_expiry"])

    return df.groupby("symbol", as_index=False).first()


def _add_straddle_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["straddle_bid"] = out["call_bid"] + out["put_bid"]
    out["straddle_ask"] = out["call_ask"] + out["put_ask"]
    out["straddle_mid"] = (out["straddle_bid"] + out["straddle_ask"]) / 2
    out["spread_abs"] = out["straddle_ask"] - out["straddle_bid"]
    out["spread_pct_mid"] = np.where(
        out["straddle_mid"] > 0,
        out["spread_abs"] / out["straddle_mid"],
        np.nan,
    )

    # Liquidity metrics
    out["call_volume"] = pd.to_numeric(out["call_volume"], errors="coerce").fillna(0.0)
    out["put_volume"] = pd.to_numeric(out["put_volume"], errors="coerce").fillna(0.0)
    out["call_open_interest"] = pd.to_numeric(
        out["call_open_interest"], errors="coerce"
    ).fillna(0.0)
    out["put_open_interest"] = pd.to_numeric(
        out["put_open_interest"], errors="coerce"
    ).fillna(0.0)

    out["min_leg_volume"] = out[["call_volume", "put_volume"]].min(axis=1)
    out["total_volume"] = out["call_volume"] + out["put_volume"]
    out["min_leg_open_interest"] = out[["call_open_interest", "put_open_interest"]].min(
        axis=1
    )
    out["total_open_interest"] = out["call_open_interest"] + out["put_open_interest"]
    return out


def _assign_signals(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    out = df.sort_values(["iv_slope", "symbol"], ascending=[False, True]).reset_index(drop=True)
    out["side"] = "none"
    out["rank_desc"] = np.arange(1, len(out) + 1)
    out["rank_asc"] = out["rank_desc"][::-1]

    if top_n <= 0 or out.empty:
        return out

    long_idx = out.head(top_n).index
    out.loc[long_idx, "side"] = "long_straddle"

    short_candidates = out.loc[out["side"] == "none"]
    short_idx = short_candidates.tail(top_n).index
    out.loc[short_idx, "side"] = "short_straddle"
    return out


def build_signal_table(
    raw_options_path: str = DEFAULT_ANALYSIS["raw_options"],
    cm_iv_path: str = DEFAULT_ANALYSIS["constant_maturity_iv"],
    as_of_date: date | None = DEFAULT_ANALYSIS["as_of_date"],
    target_dte: int = DEFAULT_ANALYSIS["target_dte"],
    min_dte: int = DEFAULT_ANALYSIS["min_dte"],
    max_dte: int = DEFAULT_ANALYSIS["max_dte"],
    min_leg_volume: float = DEFAULT_ANALYSIS["min_leg_volume"],
    min_leg_open_interest: float = DEFAULT_ANALYSIS["min_leg_open_interest"],
    max_spread_pct_mid: float = DEFAULT_ANALYSIS["max_spread_pct_mid"],
    top_n: int = DEFAULT_ANALYSIS["top_n"],
) -> pd.DataFrame:
    if as_of_date is None:
        as_of_date = date.today()

    raw = pd.read_csv(raw_options_path)
    raw = _ensure_columns(
        raw,
        required=[
            "symbol",
            "expiry",
            "strike",
            "stock_price",
            "call_bid",
            "call_ask",
            "put_bid",
            "put_ask",
        ],
        optional=[
            "call_volume",
            "put_volume",
            "call_open_interest",
            "put_open_interest",
        ],
    )

    # Basic tradability guardrails
    raw = raw.dropna(subset=["symbol", "expiry", "strike", "stock_price"])
    raw = raw[(raw["call_bid"] > 0) & (raw["call_ask"] > 0) & (raw["put_bid"] > 0) & (raw["put_ask"] > 0)]
    if raw.empty:
        return raw

    picked = _pick_contract_per_symbol(
        raw_df=raw,
        as_of_date=as_of_date,
        target_dte=target_dte,
        min_dte=min_dte,
        max_dte=max_dte,
    )
    if picked.empty:
        return picked

    picked = _add_straddle_metrics(picked)
    picked = picked[
        (picked["min_leg_volume"] >= min_leg_volume)
        & (picked["min_leg_open_interest"] >= min_leg_open_interest)
        & (picked["spread_pct_mid"] <= max_spread_pct_mid)
    ]
    if picked.empty:
        return picked

    cm = pd.read_csv(cm_iv_path)
    for col in ("symbol", "IV30D", "IV180D"):
        if col not in cm.columns:
            raise ValueError(f"Missing required column in CM IV file: {col}")

    out = picked.merge(cm[["symbol", "IV30D", "IV180D"]], on="symbol", how="inner")
    if out.empty:
        return out

    out["iv_slope"] = np.where(
        out["IV180D"] > 0,
        (out["IV180D"] - out["IV30D"]) / out["IV180D"],
        np.nan,
    )
    out = out[np.isfinite(out["iv_slope"])].copy()
    if out.empty:
        return out

    out = _assign_signals(out, top_n=top_n)
    out.insert(0, "analysis_date", as_of_date.isoformat())
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze scanned options and generate basic IV-slope straddle signals.",
    )
    parser.add_argument(
        "--raw-options",
        default=DEFAULT_ANALYSIS["raw_options"],
        help="Path to raw paired options CSV.",
    )
    parser.add_argument(
        "--constant-maturity-iv",
        default=DEFAULT_ANALYSIS["constant_maturity_iv"],
        help="Path to constant maturity IV CSV.",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_ANALYSIS["output"],
        help="Output CSV path.",
    )
    parser.add_argument(
        "--as-of-date",
        type=_parse_date,
        default=DEFAULT_ANALYSIS["as_of_date"],
        help="As-of date (YYYY-MM-DD) used for days-to-expiry. Default: today.",
    )
    parser.add_argument(
        "--target-dte",
        type=int,
        default=DEFAULT_ANALYSIS["target_dte"],
        help=f"Target days to expiry (default: {DEFAULT_ANALYSIS['target_dte']}).",
    )
    parser.add_argument(
        "--min-dte",
        type=int,
        default=DEFAULT_ANALYSIS["min_dte"],
        help=f"Min days to expiry (default: {DEFAULT_ANALYSIS['min_dte']}).",
    )
    parser.add_argument(
        "--max-dte",
        type=int,
        default=DEFAULT_ANALYSIS["max_dte"],
        help=f"Max days to expiry (default: {DEFAULT_ANALYSIS['max_dte']}).",
    )
    parser.add_argument(
        "--min-leg-volume",
        type=float,
        default=DEFAULT_ANALYSIS["min_leg_volume"],
        help="Min volume required on each leg (call and put).",
    )
    parser.add_argument(
        "--min-leg-open-interest",
        type=float,
        default=DEFAULT_ANALYSIS["min_leg_open_interest"],
        help="Min open interest required on each leg (call and put).",
    )
    parser.add_argument(
        "--max-spread-pct-mid",
        type=float,
        default=DEFAULT_ANALYSIS["max_spread_pct_mid"],
        help=(
            "Max allowed straddle spread as %% of straddle mid "
            f"(default: {DEFAULT_ANALYSIS['max_spread_pct_mid']})."
        ),
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_ANALYSIS["top_n"],
        help=f"Number of symbols for long and short signal buckets (default: {DEFAULT_ANALYSIS['top_n']}).",
    )
    args = parser.parse_args()

    table = build_signal_table(
        raw_options_path=args.raw_options,
        cm_iv_path=args.constant_maturity_iv,
        as_of_date=args.as_of_date,
        target_dte=args.target_dte,
        min_dte=args.min_dte,
        max_dte=args.max_dte,
        min_leg_volume=args.min_leg_volume,
        min_leg_open_interest=args.min_leg_open_interest,
        max_spread_pct_mid=args.max_spread_pct_mid,
        top_n=args.top_n,
    )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(out_path, index=False)

    if table.empty:
        print("No rows after filtering. Try relaxing liquidity/spread/DTE constraints.")
        print(f"Wrote empty table to {out_path}")
        return

    counts = table["side"].value_counts().to_dict()
    print(f"Wrote {len(table)} rows to {out_path}")
    print(f"  side counts: {counts}")
    print(
        "  filters: "
        f"target_dte={args.target_dte}, min_dte={args.min_dte}, max_dte={args.max_dte}, "
        f"min_leg_volume={args.min_leg_volume}, min_leg_open_interest={args.min_leg_open_interest}, "
        f"max_spread_pct_mid={args.max_spread_pct_mid}, top_n={args.top_n}"
    )


if __name__ == "__main__":
    main()
