"""Filtering, ATM selection, and constant maturity IV interpolation.

Replicates the methodology from the R reference code:
1. Filter options (stock price, bid, delta, IV thresholds)
2. Find ATM option per symbol/expiry (closest strike to stock price)
3. Compute ATM straddle IV = (call_mid_iv + put_mid_iv) / 2
4. Interpolate across expirations for constant maturity IV
"""

from datetime import datetime

import numpy as np
import pandas as pd


def filter_options(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Apply the 5 filters from the academic paper methodology.

    Expects a wide-format DataFrame with call/put data paired per row.
    Columns required: stock_price, call_bid, call_ask, put_bid, put_ask,
                      call_bid_iv, put_bid_iv, call_delta, put_delta
    """
    f = cfg.get("filters", {})
    min_delta = f.get("min_abs_delta", 0.35)
    max_delta = f.get("max_abs_delta", 0.65)
    min_iv = f.get("min_iv", 0.03)
    max_iv = f.get("max_iv", 2.0)

    out = df.copy()

    # Filter 1: non-zero bid-ask spread
    out = out[(out["call_ask"] - out["call_bid"]).abs() > 0]
    out = out[(out["put_ask"] - out["put_bid"]).abs() > 0]

    # Filter 3: bid > 0
    out = out[out["call_bid"] > 0]
    out = out[out["put_bid"] > 0]

    # Filter 4: |delta| between 0.35 and 0.65
    # Use call delta (positive) for filtering
    out = out[out["call_delta"].abs().between(min_delta, max_delta)]

    # Filter 5: IV between 3% and 200%
    out = out[out["call_bid_iv"].between(min_iv, max_iv)]
    out = out[out["put_bid_iv"].between(min_iv, max_iv)]

    return out.reset_index(drop=True)


def pair_calls_puts(rows: list[dict]) -> pd.DataFrame:
    """Pivot long-format (one row per option leg) into wide-format
    (one row per symbol/expiry/strike with call_ and put_ columns)."""
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    calls = df[df["right"] == "C"].copy()
    puts = df[df["right"] == "P"].copy()

    call_cols = {
        "bid": "call_bid", "ask": "call_ask",
        "volume": "call_volume", "open_interest": "call_open_interest",
        "bid_iv": "call_bid_iv", "ask_iv": "call_ask_iv",
        "mid_iv": "call_mid_iv", "model_iv": "call_model_iv",
        "delta": "call_delta", "gamma": "call_gamma",
        "vega": "call_vega", "theta": "call_theta",
    }
    put_cols = {
        "bid": "put_bid", "ask": "put_ask",
        "volume": "put_volume", "open_interest": "put_open_interest",
        "bid_iv": "put_bid_iv", "ask_iv": "put_ask_iv",
        "mid_iv": "put_mid_iv", "model_iv": "put_model_iv",
        "delta": "put_delta", "gamma": "put_gamma",
        "vega": "put_vega", "theta": "put_theta",
    }

    key_cols = ["symbol", "expiry", "strike", "stock_price"]

    calls = calls.rename(columns=call_cols)[key_cols + list(call_cols.values())]
    puts = puts.rename(columns=put_cols)[key_cols + list(put_cols.values())]

    merged = pd.merge(calls, puts, on=key_cols, how="inner")
    return merged


def find_atm_options(df: pd.DataFrame) -> pd.DataFrame:
    """For each symbol/expiry, keep the strike closest to the stock price."""
    if df.empty:
        return df
    df = df.copy()
    df["atm_diff"] = (df["strike"] - df["stock_price"]).abs()
    df = df.sort_values("atm_diff")
    atm = df.groupby(["symbol", "expiry"], as_index=False).first()
    return atm.drop(columns=["atm_diff"])


def compute_straddle_iv(df: pd.DataFrame) -> pd.DataFrame:
    """Compute ATM straddle IV = average of call and put mid IV."""
    df = df.copy()
    # Prefer mid IV (average of bid/ask IV), fall back to model IV
    call_iv = df["call_mid_iv"].fillna(df["call_model_iv"])
    put_iv = df["put_mid_iv"].fillna(df["put_model_iv"])
    df["atm_iv"] = (call_iv + put_iv) / 2
    return df


def compute_days_to_expiry(df: pd.DataFrame) -> pd.DataFrame:
    """Add days_to_expiry column from expiry date string (YYYYMMDD)."""
    df = df.copy()
    today = datetime.now().date()
    df["expiry_date"] = pd.to_datetime(df["expiry"], format="%Y%m%d").dt.date
    df["days_to_expiry"] = df["expiry_date"].apply(lambda d: (d - today).days)
    return df


def interpolate_constant_maturity(
    df: pd.DataFrame, tenors: list[int] = (30, 180),
) -> pd.DataFrame:
    """Interpolate ATM straddle IV to constant maturity tenors.

    Uses linear interpolation with clamping at boundaries,
    matching R's approx(rule=2, ties=mean).
    Returns one row per symbol with IV{tenor}D columns.
    """
    results = []
    for symbol, grp in df.groupby("symbol"):
        grp = grp.sort_values("days_to_expiry")
        x = grp["days_to_expiry"].values.astype(float)
        y = grp["atm_iv"].values.astype(float)

        # Remove NaN
        mask = np.isfinite(x) & np.isfinite(y)
        x, y = x[mask], y[mask]

        row = {"symbol": symbol}
        if len(x) == 0:
            for t in tenors:
                row[f"IV{t}D"] = np.nan
        elif len(np.unique(x)) < 2:
            # Only one unique expiry - use that IV for all tenors
            for t in tenors:
                row[f"IV{t}D"] = y[0]
        else:
            # Average duplicate x values (ties=mean)
            ux, idx = np.unique(x, return_inverse=True)
            uy = np.zeros_like(ux)
            for i in range(len(ux)):
                uy[i] = y[idx == i].mean()

            # Linear interpolation with extrapolation (rule=2: clamp)
            for t in tenors:
                row[f"IV{t}D"] = float(np.interp(t, ux, uy))

        # Add stock price from first row
        row["stock_price"] = grp["stock_price"].iloc[0]
        results.append(row)

    return pd.DataFrame(results)


def run_pipeline(
    raw_rows: list[dict], config: dict, tenors: list[int] = (30, 180),
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run the full IV calculation pipeline.

    Returns: (paired_df, atm_df, constant_maturity_df)
    """
    # Step 1: Pair calls and puts
    paired = pair_calls_puts(raw_rows)
    if paired.empty:
        print("No paired call/put data found.")
        empty_cm = pd.DataFrame(columns=["symbol"] + [f"IV{t}D" for t in tenors])
        return paired, paired, empty_cm

    print(f"  Paired options: {len(paired)} rows")

    # Step 2: Filter
    filtered = filter_options(paired, config)
    print(f"  After filtering: {len(filtered)} rows")

    if filtered.empty:
        print("  No options survived filtering.")
        empty_cm = pd.DataFrame(columns=["symbol"] + [f"IV{t}D" for t in tenors])
        return paired, filtered, empty_cm

    # Step 3: Find ATM options
    atm = find_atm_options(filtered)
    print(f"  ATM options: {len(atm)} rows")

    # Step 4: Straddle IV
    atm = compute_straddle_iv(atm)

    # Step 5: Days to expiry
    atm = compute_days_to_expiry(atm)

    # Step 6: Constant maturity interpolation
    cm = interpolate_constant_maturity(atm, tenors=tenors)

    return paired, atm, cm
