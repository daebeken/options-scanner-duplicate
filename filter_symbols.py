"""Filter US stock universe to build symbols.txt for the options scanner.

Reads a hive-partitioned parquet database of daily stock data and keeps
symbols that pass price, recency, and volume thresholds.
"""

import argparse
import glob
import os
from datetime import date, timedelta

import polars as pl

DEFAULT_DB = r"C:\Users\Mislav\qc_snp\data\all_stocks_daily"
DEFAULT_OUTPUT = "symbols.txt"


def filter_symbols(
    db_path: str = DEFAULT_DB,
    min_price: float = 10.0,
    max_age_days: int = 60,
    min_volume_22d: int = 300_000,
    min_dollar_volume: float = 100_000_000,
    min_history_days: int = 252,
    max_abs_daily_return: float = 0.5,
    lookback_days: int = 22,
    top_n_dollar_volume: int | None = None,
    exclude_investment_vehicles: bool = True,
    exclude_dot_symbols: bool = True,
) -> list[str]:
    cutoff = date.today() - timedelta(days=max_age_days)

    # Read each symbol's parquet, filter by date, collect into one frame
    frames = []
    history_rows: list[tuple[str, int]] = []
    for d in glob.glob(f"{db_path}/Symbol=*"):
        sym = os.path.basename(d).split("=", 1)[1]
        if exclude_dot_symbols and "." in sym:
            continue

        pf = os.path.join(d, "part-0.parquet")
        if not os.path.exists(pf):
            continue

        history_days = (
            pl.scan_parquet(pf)
            .select(pl.count().alias("history_days"))
            .collect()
            .item()
        )
        if history_days < min_history_days:
            continue

        lf = pl.scan_parquet(pf).filter(pl.col("Date") >= cutoff)
        if "Inv Vehicle" in lf.columns:
            lf = lf.select("Date", "Close", "Volume", "Inv Vehicle")
        else:
            lf = lf.select("Date", "Close", "Volume").with_columns(
                pl.lit(False).alias("Inv Vehicle")
            )
        df = lf.collect()
        if len(df) == 0:
            continue
        frames.append(df.with_columns(pl.lit(sym).alias("Symbol")))
        history_rows.append((sym, history_days))

    if not frames:
        return []

    df = pl.concat(frames)
    print(f"  {df['Symbol'].n_unique()} symbols with data since {cutoff}")

    # Last row per symbol
    latest = (
        df.sort("Date")
        .group_by("Symbol")
        .last()
        .rename(
            {
                "Close": "last_close",
                "Date": "last_date",
                "Inv Vehicle": "is_investment_vehicle",
            }
        )
        .select("Symbol", "last_close", "last_date", "is_investment_vehicle")
    )

    # Total (share + dollar) volume over the last N trading days per symbol
    liq = (
        df.sort("Date")
        .group_by("Symbol")
        .tail(lookback_days)
        .group_by("Symbol")
        .agg(
            pl.col("Volume").sum().alias("vol_22d"),
            (pl.col("Close") * pl.col("Volume")).sum().alias("dollar_vol_22d"),
        )
    )

    # Max absolute daily return over the recent window
    returns = (
        df.sort(["Symbol", "Date"])
        .with_columns(
            (pl.col("Close") / pl.col("Close").shift(1) - 1)
            .over("Symbol")
            .abs()
            .alias("abs_daily_return")
        )
        .group_by("Symbol")
        .agg(pl.col("abs_daily_return").max().fill_null(0.0).alias("max_abs_ret"))
    )

    history = pl.DataFrame(history_rows, schema=["Symbol", "history_days"])

    merged = latest.join(liq, on="Symbol").join(returns, on="Symbol").join(
        history, on="Symbol"
    )

    base_filter = (
        (pl.col("last_close") >= min_price)
        & (pl.col("last_date") >= cutoff)
        & (pl.col("vol_22d") >= min_volume_22d)
        & (pl.col("dollar_vol_22d") >= min_dollar_volume)
        & (pl.col("history_days") >= min_history_days)
        & (pl.col("max_abs_ret") <= max_abs_daily_return)
    )
    if exclude_investment_vehicles:
        base_filter = base_filter & (
            ~pl.col("is_investment_vehicle").fill_null(False)
        )

    filtered = merged.filter(base_filter)

    if top_n_dollar_volume and top_n_dollar_volume > 0:
        filtered = filtered.sort(
            ["dollar_vol_22d", "Symbol"],
            descending=[True, False],
        ).head(top_n_dollar_volume)
        symbols = [s.upper() for s in filtered["Symbol"].to_list()]
    else:
        symbols = sorted(s.upper() for s in filtered["Symbol"].to_list())
    return symbols


def main():
    parser = argparse.ArgumentParser(
        description="Filter stock universe for options scanner",
    )
    parser.add_argument("--db", default=DEFAULT_DB,
                        help="Path to hive-partitioned parquet DB")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help="Output symbols file")
    parser.add_argument("--min-price", type=float, default=10.0,
                        help="Min last close price (default: 10)")
    parser.add_argument("--max-age-days", type=int, default=60,
                        help="Max days since last data point (default: 60)")
    parser.add_argument("--min-volume", type=int, default=300_000,
                        help="Min total volume over lookback period (default: 200000)")
    parser.add_argument(
        "--min-dollar-volume",
        type=float,
        default=150_000_000,
        help=(
            "Min total dollar volume (Close*Volume) over lookback period "
            "(default: 150000000)"
        ),
    )
    parser.add_argument(
        "--min-history-days",
        type=int,
        default=252,
        help="Min available daily bars per symbol (default: 252)",
    )
    parser.add_argument(
        "--max-abs-daily-return",
        type=float,
        default=0.5,
        help="Max allowed absolute daily return in recent window (default: 0.25)",
    )
    parser.add_argument("--lookback-days", type=int, default=22,
                        help="Trading days for volume calc (default: 22)")
    parser.add_argument(
        "--top-n-dollar-volume",
        type=int,
        default=None,
        help=(
            "After all filters, keep only top N symbols by dollar volume "
            "(dollar_vol_22d). Example: 200"
        ),
    )
    parser.add_argument(
        "--exclude-investment-vehicles",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Exclude symbols flagged as investment vehicles "
            "(ETFs/ETNs). Use --no-exclude-investment-vehicles to keep them."
        ),
    )
    parser.add_argument(
        "--exclude-dot-symbols",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Exclude symbols containing a dot ('.'). "
            "Use --no-exclude-dot-symbols to keep them."
        ),
    )
    args = parser.parse_args()

    print(f"Reading data from {args.db}...")
    symbols = filter_symbols(
        db_path=args.db,
        min_price=args.min_price,
        max_age_days=args.max_age_days,
        min_volume_22d=args.min_volume,
        min_dollar_volume=args.min_dollar_volume,
        min_history_days=args.min_history_days,
        max_abs_daily_return=args.max_abs_daily_return,
        lookback_days=args.lookback_days,
        top_n_dollar_volume=args.top_n_dollar_volume,
        exclude_investment_vehicles=args.exclude_investment_vehicles,
        exclude_dot_symbols=args.exclude_dot_symbols,
    )

    with open(args.output, "w") as f:
        f.write("\n".join(symbols) + "\n")

    print(f"Wrote {len(symbols)} symbols to {args.output}")
    print(f"  min_price={args.min_price}, max_age={args.max_age_days}d, "
          f"min_vol_22d={args.min_volume}, min_dollar_vol={args.min_dollar_volume}, "
          f"min_history_days={args.min_history_days}, "
          f"max_abs_daily_return={args.max_abs_daily_return}, "
          f"lookback={args.lookback_days}d, "
          f"top_n_dollar_volume={args.top_n_dollar_volume}, "
          f"exclude_investment_vehicles={args.exclude_investment_vehicles}, "
          f"exclude_dot_symbols={args.exclude_dot_symbols}")


if __name__ == "__main__":
    main()
