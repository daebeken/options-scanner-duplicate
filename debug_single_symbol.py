"""Interactive one-symbol debug script for option data collection.

No parser, no custom functions, no main. Edit CONFIG and run line-by-line.
"""

from __future__ import annotations

import os
import time
from datetime import datetime

import pandas as pd
import yaml

from scanner import OptionsScanner

# =========================
# CONFIG (edit these)
# =========================
SYMBOL = "NVDA"
CONFIG_PATH = "config.yaml"
OUTPUT_PATH = "data/debug_nvda.csv"

# Use None to keep value from config.yaml
MARKET_DATA_TYPE_OVERRIDE = 1
RATE_LIMIT_OVERRIDE = None

MAX_STRIKES = 2
TENORS_OVERRIDE = None  # e.g. [30, 180]
EXPIRIES_PER_TENOR = 2  # 2 => nearest below+above each tenor when available
WAIT_TIMEOUT = 20.0
DONE_PCT = 0.95
PREVALIDATE_CONTRACTS = True
PREVALIDATION_CANDIDATE_FACTOR = 12
PREVALIDATION_FALLBACK_UNVALIDATED = False

# =========================
# LOAD CONFIG
# =========================
with open(CONFIG_PATH) as f:
    cfg = yaml.safe_load(f)

tws = cfg["tws"]
market_data_type = cfg.get("market_data_type", 1)
if MARKET_DATA_TYPE_OVERRIDE is not None:
    market_data_type = MARKET_DATA_TYPE_OVERRIDE

rate_limit = cfg.get("rate_limit", 0.03)
if RATE_LIMIT_OVERRIDE is not None:
    rate_limit = RATE_LIMIT_OVERRIDE

tenors = cfg.get("tenors", [30, 180])
if TENORS_OVERRIDE is not None:
    tenors = TENORS_OVERRIDE


def pick_expiries_around_tenors(
    expiry_map: dict[str, list[float]],
    tenors_days: list[int],
    as_of_date,
    per_tenor: int = 2,
) -> list[str]:
    """Select expiries around each tenor (prefer one below + one above)."""
    if not expiry_map:
        return []

    entries = []
    for exp in expiry_map:
        dte = (datetime.strptime(exp, "%Y%m%d").date() - as_of_date).days
        entries.append((exp, dte))
    entries = sorted(entries, key=lambda x: x[1])

    selected: set[str] = set()
    for tenor in tenors_days:
        below = [e for e in entries if e[1] < tenor]
        above = [e for e in entries if e[1] >= tenor]
        picked: list[str] = []

        if below:
            picked.append(below[-1][0])  # closest below tenor
        if above and above[0][0] not in picked:
            picked.append(above[0][0])  # closest above tenor

        if len(picked) < per_tenor:
            by_dist = sorted(entries, key=lambda x: abs(x[1] - tenor))
            for exp, _ in by_dist:
                if exp not in picked:
                    picked.append(exp)
                if len(picked) >= per_tenor:
                    break

        selected.update(picked[:per_tenor])

    return sorted(
        selected,
        key=lambda exp: (datetime.strptime(exp, "%Y%m%d").date() - as_of_date).days,
    )

# =========================
# CONNECT
# =========================
app = OptionsScanner()
app.connectAndStart(tws["host"], tws["port"], tws["client_id"])
app.setMarketDataType(market_data_type, settle=0.2)

try:
    # =========================
    # RESOLVE CONTRACT + CHAIN
    # =========================
    symbol = SYMBOL.upper()
    print(f"Debugging symbol: {symbol}")
    print(f"Market data type: {market_data_type}")
    print(f"Rate limit: {rate_limit}")

    con_id = app.getConId(symbol)
    if con_id is None:
        raise RuntimeError(f"Could not resolve conId for {symbol}")
    print(f"conId: {con_id}")

    chain = app.getOptionChain(symbol, con_id)
    if chain is None:
        raise RuntimeError(f"Could not load option chain for {symbol}")
    print(f"Chain expiries: {len(chain['expirations'])}, strikes: {len(chain['strikes'])}")

    # =========================
    # PICK STRIKES + EXPIRIES
    # =========================
    und_price, used_mkt_data_type = app.getUnderlyingPriceAuto(
        symbol,
        con_id=con_id,
        preferred_type=market_data_type,
    )
    if und_price is None or und_price <= 0:
        raise RuntimeError(f"No underlying price for {symbol}; cannot select ATM strikes.")
    if used_mkt_data_type is not None and used_mkt_data_type != market_data_type:
        if used_mkt_data_type == 0:
            print("Underlying price fallback source: historical_close")
        else:
            print(f"Underlying quote fallback market_data_type: {used_mkt_data_type}")

    candidate_strikes = MAX_STRIKES
    if PREVALIDATE_CONTRACTS:
        candidate_strikes = max(MAX_STRIKES, MAX_STRIKES * PREVALIDATION_CANDIDATE_FACTOR)

    est_price, strikes_per_expiry = app.selectStrikes(
        chain,
        max_strikes=candidate_strikes,
        underlying_price=und_price,
    )
    today = datetime.now().date()

    expiries = pick_expiries_around_tenors(
        strikes_per_expiry,
        tenors_days=tenors,
        as_of_date=today,
        per_tenor=EXPIRIES_PER_TENOR,
    )
    strikes_per_expiry = {e: strikes_per_expiry[e] for e in expiries}
    fallback_strikes_per_expiry = {
        e: strikes_per_expiry[e][:MAX_STRIKES]
        for e in expiries
        if strikes_per_expiry.get(e)
    }

    if PREVALIDATE_CONTRACTS and expiries:
        print("Prevalidating selected option contracts...")
        strikes_per_expiry, pre_stats = app.prevalidateOptionContracts(
            symbol,
            expiries=expiries,
            strikes_per_expiry=strikes_per_expiry,
            trading_class=chain.get("tradingClass", ""),
            multiplier=chain.get("multiplier", "100"),
            timeout_per_contract=2.0,
            rate_limit=0.0,
        )
        expiries = [e for e in expiries if e in strikes_per_expiry]
        invalid_codes = pre_stats.get("invalid_code_counts", {})
        invalid_code_text = ""
        if invalid_codes:
            top_code = max(invalid_codes.items(), key=lambda kv: kv[1])[0]
            invalid_code_text = f", top_invalid_code={top_code}"
        print(
            "Prevalidation kept "
            f"{pre_stats['strikes_out']}/{pre_stats['strikes_in']} strikes "
            f"across {pre_stats['expiries_out']}/{pre_stats['expiries_in']} expiries "
            f"(invalid legs={pre_stats['legs_invalid']}{invalid_code_text})"
        )

    if strikes_per_expiry:
        trimmed = {}
        for exp in expiries:
            picks = strikes_per_expiry.get(exp, [])[:MAX_STRIKES]
            if picks:
                trimmed[exp] = picks
        strikes_per_expiry = trimmed
        expiries = [e for e in expiries if e in strikes_per_expiry]

    if (
        PREVALIDATE_CONTRACTS
        and PREVALIDATION_FALLBACK_UNVALIDATED
        and not strikes_per_expiry
        and fallback_strikes_per_expiry
    ):
        print("Prevalidation yielded zero contracts; falling back to unvalidated strikes.")
        strikes_per_expiry = fallback_strikes_per_expiry
        expiries = list(strikes_per_expiry.keys())

    requested = sum(len(strikes_per_expiry[e]) * 2 for e in expiries)

    print(f"Price anchor (underlying): {est_price:.2f}")
    print(f"Tenors target: {tenors}")
    print(f"Requesting expiries: {len(expiries)} (around target tenors)")
    print(f"Total option requests (C+P): {requested}")
    if requested == 0:
        raise RuntimeError("No valid option contracts after prevalidation.")

    # =========================
    # REQUEST + WAIT + CANCEL
    # =========================
    fallback_types = (2, 4, 3)
    min_data_pct = 0.25
    candidates = []
    for mkt_type in (market_data_type, *fallback_types):
        if mkt_type not in candidates:
            candidates.append(mkt_type)

    req_ids = []
    for idx, mkt_type in enumerate(candidates):
        app.setMarketDataType(mkt_type, settle=0.05)
        req_ids = app.requestBatch(
            symbol=symbol,
            expiries=expiries,
            strikes_per_expiry=strikes_per_expiry,
            rate_limit=rate_limit,
        )
        print(
            f"Requesting batch with market_data_type={mkt_type} "
            f"({len(req_ids)} subscriptions)"
        )
        app.waitForData(req_ids, timeout=WAIT_TIMEOUT, done_pct=DONE_PCT)
        summary = app.summarizeOptionBatch(req_ids)
        top_err = summary.get("top_error_code")
        top_err_part = (
            f", top_error={top_err} x{summary.get('top_error_count', 0)}"
            if top_err is not None else ""
        )
        print(
            f"Coverage: has_data={summary['has_data']}/{summary['eligible']}"
            f"{top_err_part}"
        )

        good_enough = (
            summary["eligible"] == 0
            or summary["has_data_pct"] >= min_data_pct
            or idx == len(candidates) - 1
        )
        if good_enough:
            if mkt_type != market_data_type:
                print(f"Batch fallback market_data_type: {mkt_type}")
            break

        app.cancelBatch(req_ids)
        print(
            f"Low data coverage ({summary['has_data']}/{summary['eligible']} usable), "
            "retrying fallback market_data_type..."
        )

    app.cancelBatch(req_ids)
    time.sleep(0.2)

    # =========================
    # BUILD DEBUG TABLE
    # =========================
    rows: list[dict] = []
    for rid in req_ids:
        meta = app._req_map.get(rid, (None, None, None, None))
        symbol_i, expiry_i, strike_i, right_i = meta
        d = app._mkt_data.get(rid, {})
        err = app._option_req_errors.get(rid)
        err_code = err[0] if err else None
        err_msg = err[1] if err else None

        ready_evt = app._data_ready.get(rid)
        ready = bool(ready_evt and ready_evt.is_set())
        failed_req = rid in app._failed_reqs

        bid = d.get("bid")
        ask = d.get("ask")
        bid_iv = d.get("bid_iv")
        ask_iv = d.get("ask_iv")
        model_iv = d.get("model_iv")

        has_quote = bid is not None and ask is not None
        has_any_iv = bid_iv is not None or ask_iv is not None or model_iv is not None
        has_underlying = d.get("und_price") is not None

        if failed_req:
            status = "failed_req"
        elif not ready:
            status = "not_ready_before_timeout"
        elif has_quote or has_any_iv:
            status = "has_data"
        else:
            status = "ready_but_empty"

        dte = None
        if expiry_i:
            exp_date = datetime.strptime(expiry_i, "%Y%m%d").date()
            dte = (exp_date - today).days

        rows.append(
            {
                "req_id": rid,
                "symbol": symbol_i,
                "expiry": expiry_i,
                "dte": dte,
                "strike": strike_i,
                "right": right_i,
                "ready": ready,
                "failed_req": failed_req,
                "status": status,
                "bid": bid,
                "ask": ask,
                "option_volume": d.get("option_volume", d.get("volume")),
                "open_interest": d.get("open_interest"),
                "bid_iv": bid_iv,
                "ask_iv": ask_iv,
                "model_iv": model_iv,
                "delta": d.get("model_delta") or d.get("bid_delta"),
                "gamma": d.get("model_gamma"),
                "vega": d.get("model_vega"),
                "theta": d.get("model_theta"),
                "und_price": d.get("und_price"),
                "has_quote": has_quote,
                "has_any_iv": has_any_iv,
                "has_underlying": has_underlying,
                "error_code": err_code,
                "error_msg": err_msg,
            }
        )

    df = pd.DataFrame(rows)

    # =========================
    # SAVE + PRINT SUMMARY
    # =========================
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print("\n=== Summary ===")
    print(f"Rows written: {len(df)} -> {OUTPUT_PATH}")
    print(f"failed_req: {int(df['failed_req'].sum())}")
    print(f"ready: {int(df['ready'].sum())}")
    print(f"has_quote: {int(df['has_quote'].sum())}")
    print(f"has_any_iv: {int(df['has_any_iv'].sum())}")
    print(f"has_underlying: {int(df['has_underlying'].sum())}")

    print("\nStatus counts:")
    print(df["status"].value_counts(dropna=False).to_string())

    if "error_code" in df.columns and df["error_code"].notna().any():
        print("\nError code counts:")
        print(df["error_code"].value_counts(dropna=False).to_string())

    by_expiry = (
        df.groupby("expiry", as_index=False)
        .agg(
            rows=("req_id", "count"),
            has_quote=("has_quote", "sum"),
            has_any_iv=("has_any_iv", "sum"),
            failed_req=("failed_req", "sum"),
        )
        .sort_values("expiry")
    )
    print("\nBy expiry:")
    print(by_expiry.to_string(index=False))
finally:
    app.disconnect()
