"""Options Scanner - Constant Maturity IV Calculator.

Connects to IBKR TWS, collects options data for configured symbols,
and computes constant maturity implied volatility at specified tenors.
"""

import os
import sys
import time
from datetime import datetime

import pandas as pd
import yaml

from iv_calculator import run_pipeline
from scanner import OptionsScanner


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_symbols(path: str) -> list[str]:
    """Read symbols from a text file, one ticker per line."""
    with open(path) as f:
        symbols = [line.strip().upper() for line in f if line.strip() and not line.startswith("#")]
    return symbols


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


def scan_symbol(
    app: OptionsScanner, symbol: str, con_id: int, chain: dict,
    batch_size: int = 200, rate_limit: float = 0.03,
    preferred_mkt_data_type: int = 1,
    tenors: list[int] | None = None,
    expiries_per_tenor: int = 2,
    prevalidate_contracts: bool = True,
    prevalidation_fallback_unvalidated: bool = False,
    max_strikes_per_expiry: int = 2,
    prevalidation_candidate_factor: int = 2,
) -> list[dict]:
    """Collect options data for a single symbol using streaming subscriptions."""
    print(f"\n{'='*60}")
    print(f"Scanning {symbol}...")
    print(f"  conId: {con_id}")
    print(f"  Expirations: {len(chain['expirations'])}, Strikes: {len(chain['strikes'])}")

    # Step 1: Select strikes near ATM using real underlying price.
    # Try live first, then fallback (frozen/delayed) if unavailable.
    und_price, used_mkt_data_type = app.getUnderlyingPriceAuto(
        symbol,
        con_id=con_id,
        preferred_type=preferred_mkt_data_type,
    )
    if und_price is None or und_price <= 0:
        print(f"  No underlying price for {symbol}; skipping symbol.")
        return []
    if used_mkt_data_type is not None and used_mkt_data_type != preferred_mkt_data_type:
        if used_mkt_data_type == 0:
            print("  Underlying price fallback source=historical_close")
        else:
            print(f"  Underlying quote fallback market_data_type={used_mkt_data_type}")

    candidate_strikes = max_strikes_per_expiry
    if prevalidate_contracts:
        candidate_strikes = max(
            max_strikes_per_expiry,
            max_strikes_per_expiry * prevalidation_candidate_factor,
        )

    est_price, strikes_per_expiry = app.selectStrikes(
        chain,
        max_strikes=candidate_strikes,
        underlying_price=und_price,
    )
    today = datetime.now().date()
    tenor_targets = tenors if tenors else [30, 180]
    expiry_list = pick_expiries_around_tenors(
        strikes_per_expiry,
        tenors_days=tenor_targets,
        as_of_date=today,
        per_tenor=expiries_per_tenor,
    )
    strikes_per_expiry = {e: strikes_per_expiry[e] for e in expiry_list}
    fallback_strikes_per_expiry = {
        e: strikes_per_expiry[e][:max_strikes_per_expiry]
        for e in expiry_list
        if strikes_per_expiry.get(e)
    }

    if prevalidate_contracts and expiry_list:
        approx_legs = sum(len(strikes_per_expiry.get(e, [])) * 2 for e in expiry_list)
        print("  Prevalidating selected option contracts...")
        print(
            f"  Prevalidation scope: {len(expiry_list)} expiries, "
            f"{sum(len(strikes_per_expiry.get(e, [])) for e in expiry_list)} strike candidates "
            f"(~{approx_legs} option legs)"
        )
        strikes_per_expiry, pre_stats = app.prevalidateOptionContracts(
            symbol,
            expiries=expiry_list,
            strikes_per_expiry=strikes_per_expiry,
            trading_class=chain.get("tradingClass", ""),
            multiplier=chain.get("multiplier", "100"),
            timeout_per_contract=2.0,
            rate_limit=0.0,
        )
        expiry_list = [e for e in expiry_list if e in strikes_per_expiry]
        invalid_codes = pre_stats.get("invalid_code_counts", {})
        invalid_code_text = ""
        if invalid_codes:
            top_code = max(invalid_codes.items(), key=lambda kv: kv[1])[0]
            invalid_code_text = f", top_invalid_code={top_code}"
        print(
            "  Prevalidation kept "
            f"{pre_stats['strikes_out']}/{pre_stats['strikes_in']} strikes "
            f"across {pre_stats['expiries_out']}/{pre_stats['expiries_in']} expiries "
            f"(invalid legs={pre_stats['legs_invalid']}{invalid_code_text})"
        )

    if strikes_per_expiry:
        trimmed = {}
        for exp in expiry_list:
            picks = strikes_per_expiry.get(exp, [])[:max_strikes_per_expiry]
            if picks:
                trimmed[exp] = picks
        strikes_per_expiry = trimmed
        expiry_list = [e for e in expiry_list if e in strikes_per_expiry]

    if (
        prevalidate_contracts
        and prevalidation_fallback_unvalidated
        and not strikes_per_expiry
        and fallback_strikes_per_expiry
    ):
        print("  Prevalidation yielded zero contracts; falling back to unvalidated strikes.")
        strikes_per_expiry = fallback_strikes_per_expiry
        expiry_list = list(strikes_per_expiry.keys())

    total_options = sum(len(s) * 2 for s in strikes_per_expiry.values())
    print(f"  Price anchor (underlying): ${est_price:.2f}")
    print(f"  Tenor targets: {tenor_targets}")
    print(f"  Requesting {total_options} options across {len(strikes_per_expiry)} expirations...")

    if total_options == 0:
        print(f"  No near-ATM strikes found for {symbol}")
        return []

    # Step 2: Subscribe in batches, wait for data, cancel, collect
    all_req_ids = []

    batch_expiries = []
    batch_count = 0
    for exp in expiry_list:
        n = len(strikes_per_expiry[exp]) * 2  # *2 for C+P
        if batch_count + n > batch_size and batch_expiries:
            _process_batch(app, symbol, batch_expiries, strikes_per_expiry,
                           all_req_ids, rate_limit, preferred_mkt_data_type)
            batch_expiries = []
            batch_count = 0
        batch_expiries.append(exp)
        batch_count += n

    if batch_expiries:
        _process_batch(app, symbol, batch_expiries, strikes_per_expiry,
                       all_req_ids, rate_limit, preferred_mkt_data_type)

    # Step 3: Collect results
    rows = app.collectResults(all_req_ids)
    valid = sum(1 for r in rows if r.get("bid_iv") is not None or r.get("model_iv") is not None)
    print(f"  Collected {len(rows)} rows ({valid} with IV data)")

    return rows


def _process_batch(
    app,
    symbol,
    expiries,
    strikes_per_expiry,
    all_req_ids,
    rate_limit,
    preferred_mkt_data_type,
):
    """Subscribe to a batch, with market-data-type fallback for low coverage."""
    batch_dict = {e: strikes_per_expiry[e] for e in expiries}
    fallback_types = (2, 4, 3)
    min_data_pct = 0.25
    done_pct = 0.95
    timeout = 15

    candidates = []
    for mkt_type in (preferred_mkt_data_type, *fallback_types):
        if mkt_type not in candidates:
            candidates.append(mkt_type)

    chosen_rids = []
    chosen_summary = None
    chosen_type = None

    for idx, mkt_type in enumerate(candidates):
        app.setMarketDataType(mkt_type, settle=0.05)
        rids = app.requestBatch(symbol, expiries, batch_dict, rate_limit=rate_limit)
        print(
            f"    Batch: {len(rids)} subscriptions (market_data_type={mkt_type}), "
            "waiting for data..."
        )
        app.waitForData(rids, timeout=timeout, done_pct=done_pct)
        summary = app.summarizeOptionBatch(rids)
        top_err = summary.get("top_error_code")
        top_err_part = (
            f", top_error={top_err} x{summary.get('top_error_count', 0)}"
            if top_err is not None else ""
        )
        print(
            f"    Coverage: has_data={summary['has_data']}/{summary['eligible']}"
            f"{top_err_part}"
        )

        good_enough = (
            summary["eligible"] == 0
            or summary["has_data_pct"] >= min_data_pct
            or idx == len(candidates) - 1
        )
        if good_enough:
            chosen_rids = rids
            chosen_summary = summary
            chosen_type = mkt_type
            break

        app.cancelBatch(rids)
        print(
            f"    Low data coverage ({summary['has_data']}/{summary['eligible']} usable); "
            "retrying with fallback market_data_type..."
        )
        time.sleep(0.05)

    all_req_ids.extend(chosen_rids)
    app.cancelBatch(chosen_rids)

    if chosen_type is not None and chosen_type != preferred_mkt_data_type:
        print(f"    Batch fallback market_data_type={chosen_type}")
    if chosen_summary is not None and chosen_summary["failed"]:
        print(f"    ({chosen_summary['failed']} invalid contracts skipped)")
    time.sleep(0.05)


def _fetch_symbol_info(app, symbol):
    """Fetch conId and chain for a symbol (blocking)."""
    con_id = app.getConId(symbol)
    if con_id is None:
        return None, None
    chain = app.getOptionChain(symbol, con_id)
    return con_id, chain


def main():
    config = load_config()
    tws = config["tws"]
    symbols = load_symbols(config.get("symbols_file", "symbols.txt"))
    tenors = config.get("tenors", [30, 180])
    expiries_per_tenor = config.get("expiries_per_tenor", 2)
    prevalidate_contracts = config.get("prevalidate_contracts", True)
    prevalidation_fallback_unvalidated = config.get("prevalidation_fallback_unvalidated", False)
    max_strikes_per_expiry = config.get("max_strikes_per_expiry", 2)
    prevalidation_candidate_factor = config.get("prevalidation_candidate_factor", 2)
    batch_size = config.get("batch_size", 200)
    rate_limit = config.get("rate_limit", 0.03)
    mkt_data_type = config.get("market_data_type", 1)

    print(f"Loaded {len(symbols)} symbols from {config.get('symbols_file', 'symbols.txt')}")

    # Ensure output directory exists
    output_cfg = config.get("output", {})
    for path in output_cfg.values():
        os.makedirs(os.path.dirname(path), exist_ok=True)

    # Connect to TWS
    app = OptionsScanner()
    try:
        app.connectAndStart(tws["host"], tws["port"], tws["client_id"])
    except TimeoutError as e:
        print(f"Connection failed: {e}")
        print("Make sure TWS/IB Gateway is running with API enabled.")
        sys.exit(1)

    # Set market data type from config
    # 1=live, 2=frozen (last values when closed), 3=delayed, 4=delayed-frozen
    app.setMarketDataType(mkt_data_type)

    # Scan all symbols with pipelined lookups
    all_rows = []
    t_start = time.monotonic()

    # Prefetch first symbol's conId
    prefetch_rid = app.startConIdRequest(symbols[0]) if symbols else None

    for i, symbol in enumerate(symbols):
        # Await prefetched conId (or first symbol's initial request)
        con_id = app.awaitConId(symbol, prefetch_rid)
        if con_id is None:
            print(f"\n  Skipping {symbol}: could not resolve conId")
            # Prefetch next symbol
            if i + 1 < len(symbols):
                prefetch_rid = app.startConIdRequest(symbols[i + 1])
            continue

        # Fetch chain (blocking — can't prefetch without conId)
        chain = app.getOptionChain(symbol, con_id)
        if chain is None:
            print(f"\n  Skipping {symbol}: could not get option chain")
            if i + 1 < len(symbols):
                prefetch_rid = app.startConIdRequest(symbols[i + 1])
            continue

        # Start prefetching next symbol's conId BEFORE data collection
        if i + 1 < len(symbols):
            prefetch_rid = app.startConIdRequest(symbols[i + 1])

        rows = scan_symbol(app, symbol, con_id, chain,
                           batch_size=batch_size, rate_limit=rate_limit,
                           preferred_mkt_data_type=mkt_data_type,
                           tenors=tenors,
                           expiries_per_tenor=expiries_per_tenor,
                           prevalidate_contracts=prevalidate_contracts,
                           prevalidation_fallback_unvalidated=prevalidation_fallback_unvalidated,
                           max_strikes_per_expiry=max_strikes_per_expiry,
                           prevalidation_candidate_factor=prevalidation_candidate_factor)
        all_rows.extend(rows)
        elapsed = time.monotonic() - t_start
        print(f"  [{i + 1}/{len(symbols)}] elapsed: {elapsed:.1f}s")
        time.sleep(0.1)

    t_scan = time.monotonic() - t_start
    app.disconnect()
    print(f"\n{'='*60}")
    print(f"Total options collected: {len(all_rows)}")
    print(f"Data collection time: {t_scan:.1f}s")

    if not all_rows:
        print("No data collected. Check your TWS connection and market data subscriptions.")
        sys.exit(1)

    # Run the IV calculation pipeline
    print("\nRunning IV calculation pipeline...")
    paired_df, atm_df, cm_df = run_pipeline(all_rows, config, tenors=tenors)

    # Save results
    raw_path = output_cfg.get("raw_data", "data/raw_options.csv")
    atm_path = output_cfg.get("atm_iv", "data/atm_iv.csv")
    cm_path = output_cfg.get("constant_maturity", "data/constant_maturity_iv.csv")

    if not paired_df.empty:
        paired_df.to_csv(raw_path, index=False)
        print(f"\nRaw paired data saved to {raw_path}")

    if not atm_df.empty:
        atm_df.to_csv(atm_path, index=False)
        print(f"ATM IV data saved to {atm_path}")

    if not cm_df.empty:
        cm_df.to_csv(cm_path, index=False)
        print(f"Constant maturity IV saved to {cm_path}")

        # Print summary
        print(f"\n{'='*60}")
        print("CONSTANT MATURITY IMPLIED VOLATILITY")
        print(f"{'='*60}")
        tenor_cols = [f"IV{t}D" for t in tenors]
        display = cm_df[["symbol", "stock_price"] + tenor_cols].copy()
        for col in tenor_cols:
            display[col] = display[col].apply(
                lambda v: f"{v*100:.1f}%" if pd.notna(v) else "N/A"
            )
        display["stock_price"] = display["stock_price"].apply(lambda v: f"${v:.2f}")
        print(display.to_string(index=False))
    else:
        print("\nNo constant maturity IV could be computed.")


if __name__ == "__main__":
    main()
