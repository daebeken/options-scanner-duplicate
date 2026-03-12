# =============================================================================
# Updates (search for # [CHANGED] in the code):
#
# Step 7 (_compute_constant_maturity_iv):
#   - Added cOpra_ST, pOpra_ST: contract names for the expiry closest to the short-term tenor (e.g. 30d)
#   - Added cOpra_LT, pOpra_LT: contract names for the expiry closest to the long-term tenor (e.g. 180d)
#   - Added cBidPx_ST, cAskPx_ST, pBidPx_ST, pAskPx_ST: bid/ask prices for the ST contract
#
# Step 8 (_join_options_with_cm):
#   - Passes through ST/LT contract names and ST bid/ask prices from Step 7
#
# Step 10 (_compute_derived_columns):
#   - Added straddle_price_bid_ST, straddle_price_ask_ST, straddle_price_mid_ST:
#     straddle prices computed from the ST (30d) contract instead of nearest expiry
#
# Step 11 (_select_final_and_compute_wret):
#   - wret now computed on ST straddle prices (straddle_price_bid_ST / straddle_price_ask_ST)
#     to align the traded contract with the 30d IV signal (SLOPE, IVRV_SLOPE)
#   - Shift is done .over(["ticker", "cOpra_ST", "pOpra_ST"]) to ensure returns
#     are computed within each ST option pair contract (null on roll days)
# =============================================================================

from datetime import date, timedelta, datetime
from typing import Optional, Dict, Any, List, Union
from dataclasses import dataclass
import multiprocessing as mp
import math
import os

from dotenv import load_dotenv
import numpy as np
import polars as pl
import duckdb
from matplotlib import pyplot as plt
from massive import RESTClient
from extract_orats import ExtractOratsEngine

load_dotenv()


@dataclass
class OptionsFilterConfig:
    min_stock_price: float = 10.0
    min_delta: float = 0.35
    max_delta: float = 0.65
    min_iv: float = 0.03
    max_iv: float = 2.0
    short_term_max_days: int = 60
    short_term_target_days: int = 30
    long_term_min_days: int = 180


@dataclass
class EquityFilterConfig:
    min_price: float = 10.0
    max_age_days: int = 60
    min_volume_22d: int = 300_000
    min_dollar_volume: float = 100_000_000
    min_history_days: int = 252
    max_abs_daily_return: float = 0.5
    lookback_days: int = 22
    top_n_dollar_volume: Optional[int] = None


class constantIVTermStructureEngine:
    """Computes constant-maturity IV term structure for one or many tickers.

    Input: ticker (str or list of str)
    Output: filtered_consolidated_df (Polars DataFrame with 12 columns)

    Usage:
        # Single ticker
        engine = constantIVTermStructureEngine(ticker="AAPL")
        result = engine.run()

        # Multiple tickers (parallel across all CPU cores)
        engine = constantIVTermStructureEngine(ticker=["AAPL", "MSFT", "GOOGL"])
        result = engine.run()
    """

    _MASSIVE_COL_MAP = {
        "close": "close",
        "high": "high",
        "low": "low",
        "open": "open",
        "volume": "volume",
        "time": "timestamp",
    }

    _FREQ_SHIFT_MAP = {"d": 1, "w": 5, "y": 252}

    def __init__(
        self,
        ticker: Union[str, List[str]],
        massive_api_key: str = "JqmhWpb1vmo5kjpneuy7ssu3rJ0tS6Mu",
        db_path: str = "data/ORATS_OPTIONS_DB.duckdb",
        options_filter_config: Optional[Dict[str, Any]] = None,
        equity_filter_config: Optional[Dict[str, Any]] = None,
        max_history_days: int = 50000,
        tenors: Optional[List[int]] = None,
        target_frequency: Optional[str] = None,
        rank_column: Optional[str] = None,
        n_tiles: Optional[int] = None,
        auto_sync: bool = True,
    ) -> None:
        if isinstance(ticker, str):
            self.tickers = [ticker.upper()]
        else:
            self.tickers = [t.upper() for t in ticker]
        self.ticker = self.tickers[0]  # used by single-ticker pipeline methods

        self.massive_api_key = massive_api_key
        self.db_path = db_path
        self.max_history_days = max_history_days
        self.tenors = tenors or [30, 180]

        # Read from .env if not explicitly provided
        self.target_frequency = target_frequency or os.getenv("TARGET_FREQUENCY", "d")
        self.rank_column = rank_column or os.getenv("RANK_COLUMN", "SLOPE")
        self.n_tiles = n_tiles or int(os.getenv("N_TILES", "10"))

        self.auto_sync = auto_sync

        self.options_cfg = self._build_config(OptionsFilterConfig, options_filter_config)
        self.equity_cfg = self._build_config(EquityFilterConfig, equity_filter_config)

        self._client: Optional[RESTClient] = None
        self._equity_df: Optional[pl.DataFrame] = None
        self._options_df: Optional[pl.DataFrame] = None

    @staticmethod
    def _build_config(config_cls, overrides: Optional[Dict[str, Any]]):
        if overrides is None:
            return config_cls()
        valid_fields = {f.name for f in config_cls.__dataclass_fields__.values()}
        return config_cls(**{k: v for k, v in overrides.items() if k in valid_fields})

    def _get_client(self) -> RESTClient:
        if self._client is None:
            self._client = RESTClient(self.massive_api_key)
        return self._client

    # Step 0: Sync ORATS cache so options data is up to date
    def _sync_orats_cache(self) -> None:
        """Sync the ORATS DuckDB cache for the current and previous year."""
        year = date.today().year
        print("[SYNC] Syncing ORATS cache...")
        with ExtractOratsEngine(db_path=self.db_path) as orats:
            for y in [year - 1, year]:
                orats.sync_year_sequential(y, verbose=True)
        print("[SYNC] ORATS cache is up to date.")

    # Step 1: Fetch equity data from Massive API
    def _fetch_equity_data(self) -> pl.DataFrame:
        today = date.today().strftime("%Y-%m-%d")
        previous = max(
            date(1970, 1, 1),
            date.today() - timedelta(days=self.max_history_days),
        ).strftime("%Y-%m-%d")

        client = self._get_client()
        aggs = list(client.list_aggs(
            self.ticker,
            multiplier=1,
            timespan="day",
            from_=previous,
            to=today,
            adjusted="true",
            sort="asc",
            limit=50000,
        ))

        if not aggs:
            raise ValueError(f"No equity data returned for {self.ticker}")

        columns = ["close", "volume", "time"]
        df = pl.DataFrame([
            {col: getattr(a, self._MASSIVE_COL_MAP[col]) for col in columns}
            for a in aggs
        ])

        df = df.with_columns(
            pl.from_epoch("time", time_unit="ms").cast(pl.Date),
            pl.lit(self.ticker).alias("ticker"),
        ).select(["time", "ticker", "close", "volume"])

        return df

    # Step 2: Fetch options data from DuckDB
    def _fetch_options_data(self) -> pl.DataFrame:
        year = date.today().year
        tables = [f"options_data_{y}" for y in [year - 1, year]]
        ticker_sql = f"'{self.ticker}'"

        con = duckdb.connect(self.db_path, read_only=True)
        try:
            union_query = " UNION ALL ".join(f"SELECT * FROM {t}" for t in tables)
            df = pl.from_pandas(con.execute(f"""
                SELECT * FROM ({union_query})
                WHERE expirDate >= CURRENT_DATE
                AND ticker IN ({ticker_sql})
            """).fetchdf())
        finally:
            con.close()

        if df.is_empty():
            raise ValueError(f"No options data found for {self.ticker}")

        return df

    # Step 3: Filter options (5 Campasano & Linn filters)
    def _filter_options(self, options_df: pl.DataFrame) -> pl.DataFrame:
        cfg = self.options_cfg
        return options_df.filter(
            (pl.col("stkPx") > cfg.min_stock_price) &
            ((pl.col("cBidPx") - pl.col("cAskPx")).abs() > 0) &
            ((pl.col("pBidPx") - pl.col("pAskPx")).abs() > 0) &
            (pl.col("cBidPx") > 0) &
            (pl.col("pBidPx") > 0) &
            (pl.col("delta").abs() > cfg.min_delta) &
            (pl.col("delta").abs() < cfg.max_delta) &
            (pl.col("cBidIv") > cfg.min_iv) &
            (pl.col("cBidIv") < cfg.max_iv) &
            (pl.col("pBidIv") > cfg.min_iv) &
            (pl.col("pBidIv") < cfg.max_iv)
        )

    # Step 4: Add rolling equity metrics
    def _add_equity_rolling_metrics(self, equity_df: pl.DataFrame) -> pl.DataFrame:
        lookback = self.equity_cfg.lookback_days
        return (
            equity_df
            .sort(["ticker", "time"])
            .with_columns(
                (pl.col("close") / pl.col("close").shift(1) - 1)
                    .over("ticker").abs().alias("abs_daily_return"),
                (pl.col("close") / pl.col("close").shift(1) - 1)
                    .over("ticker").alias("returns"),
                pl.col("volume")
                    .rolling_sum(window_size=lookback).over("ticker").alias("vol_22d"),
                (pl.col("close") * pl.col("volume"))
                    .rolling_sum(window_size=lookback).over("ticker").alias("dollar_vol_22d"),
            )
            .with_columns(
                pl.col("abs_daily_return")
                    .rolling_max(window_size=lookback).over("ticker").alias("max_abs_ret"),
                pl.col("returns").pow(2)
                    .rolling_sum(window_size=252).sqrt().alias("RVLT"),
            )
            .drop_nulls()
        )

    # Step 5: Filter equity
    def _filter_equity(self, equity_df: pl.DataFrame) -> pl.DataFrame:
        cfg = self.equity_cfg
        return equity_df.filter(
            (pl.col("close") >= cfg.min_price) &
            (pl.col("vol_22d") >= cfg.min_volume_22d) &
            (pl.col("dollar_vol_22d") >= cfg.min_dollar_volume) &
            (pl.col("max_abs_ret") <= cfg.max_abs_daily_return)
        )

    # Step 6: Add ATM diff, ATM IV, days to expiry
    def _add_options_derived_columns(self, options_df: pl.DataFrame) -> pl.DataFrame:
        return options_df.with_columns(
            ((pl.col("strike") - pl.col("stkPx")).abs()).alias("atm_diff"),
            ((pl.col("cMidIv") + pl.col("pMidIv")) / 2).alias("atm_iv"),
            (pl.col("expirDate").cast(pl.Date) - pl.lit(datetime.now().date()))
                .dt.total_days().alias("days_to_expiry"),
            pl.min_horizontal("cVolu", "pVolu").alias("min_leg_volume"),
            (pl.col("cVolu") + pl.col("pVolu")).alias("total_volume"),
            pl.min_horizontal("cOi", "pOi").alias("min_leg_oi"),
            (pl.col("cOi") + pl.col("pOi")).alias("total_oi"),
        )

    # Step 7: Constant-maturity IV interpolation
    def _compute_constant_maturity_iv(self, options_df: pl.DataFrame) -> pl.DataFrame:
        tenors = self.tenors

        # Pick ATM option per (ticker, trade_date, expiry) — smallest atm_diff
        atm_df = (
            options_df
            .sort("atm_diff")
            .group_by(["ticker", "trade_date", "expirDate"])
            .first()
        )

        def _interpolate_group(group: pl.DataFrame) -> Dict[str, float]:
            x = group["days_to_expiry"].to_numpy().astype(float)
            y = group["atm_iv"].to_numpy().astype(float)

            mask = np.isfinite(x) & np.isfinite(y)
            x, y = x[mask], y[mask]

            if len(x) == 0:
                return {f"IV{t}D": np.nan for t in tenors}
            if len(np.unique(x)) < 2:
                return {f"IV{t}D": float(y[0]) for t in tenors}

            ux, idx = np.unique(x, return_inverse=True)
            uy = np.array([y[idx == i].mean() for i in range(len(ux))])
            return {f"IV{t}D": float(np.interp(t, ux, uy)) for t in tenors}

        results = []
        for (ticker, trade_date), group in atm_df.group_by(["ticker", "trade_date"]):
            group = group.sort("days_to_expiry")
            ivs = _interpolate_group(group)

            # [CHANGED] Find the expiry closest to each tenor (e.g. 30d and 180d)
            # so we can capture the actual short-term and long-term contract names
            days = group["days_to_expiry"].to_numpy().astype(float)  # [CHANGED] extract days_to_expiry as numpy array
            st_idx = int(np.argmin(np.abs(days - tenors[0])))  # [CHANGED] index of expiry closest to short-term tenor (e.g. 30d)
            lt_idx = int(np.argmin(np.abs(days - tenors[1])))  # [CHANGED] index of expiry closest to long-term tenor (e.g. 180d)

            results.append({
                "ticker": ticker,
                "expirDate": group["expirDate"][0],
                "trade_date": trade_date,
                "cOpra": group["cOpra"][0],
                "pOpra": group["pOpra"][0],
                "cOpra_ST": group["cOpra"][st_idx],  # [CHANGED] short-term call contract name (closest to tenors[0])
                "pOpra_ST": group["pOpra"][st_idx],  # [CHANGED] short-term put contract name (closest to tenors[0])
                "cOpra_LT": group["cOpra"][lt_idx],  # [CHANGED] long-term call contract name (closest to tenors[1])
                "pOpra_LT": group["pOpra"][lt_idx],  # [CHANGED] long-term put contract name (closest to tenors[1])
                "cBidPx_ST": group["cBidPx"][st_idx],  # [CHANGED] ST call bid price for ST straddle pricing
                "cAskPx_ST": group["cAskPx"][st_idx],  # [CHANGED] ST call ask price for ST straddle pricing
                "pBidPx_ST": group["pBidPx"][st_idx],  # [CHANGED] ST put bid price for ST straddle pricing
                "pAskPx_ST": group["pAskPx"][st_idx],  # [CHANGED] ST put ask price for ST straddle pricing
                "stkPx": group["stkPx"][0],
                **ivs,
            })

        return pl.DataFrame(results)

    # Step 8: Join options with constant-maturity data
    def _join_options_with_cm(
        self,
        filtered_options_df: pl.DataFrame,
        cm_df: pl.DataFrame,
    ) -> pl.DataFrame:
        tenor_cols = [f"IV{t}D" for t in self.tenors]

        joined = filtered_options_df.join(
            cm_df,
            on=["ticker", "expirDate", "trade_date", "cOpra", "pOpra"],
            how="inner",
        )

        return joined.select([
            "ticker", "expirDate", "trade_date", "cOpra", "pOpra",
            "cOpra_ST", "pOpra_ST", "cOpra_LT", "pOpra_LT",  # [CHANGED] pass through ST/LT contract names from Step 7
            "cBidPx", "cAskPx", "pBidPx", "pAskPx",
            "cBidPx_ST", "cAskPx_ST", "pBidPx_ST", "pAskPx_ST",  # [CHANGED] pass through ST bid/ask prices for ST straddle pricing
            "cVolu", "pVolu", "cOi", "pOi",
            "min_leg_volume", "total_volume", "min_leg_oi", "total_oi",
        ] + tenor_cols)

    # Step 9: Join with equity data
    def _join_with_equity(
        self,
        options_cm_df: pl.DataFrame,
        equity_df: pl.DataFrame,
    ) -> pl.DataFrame:
        equity_df2 = equity_df.select([
            pl.col("time").alias("trade_date"),
            "ticker",
            "returns",
            pl.col("dollar_vol_22d").alias("dollar_vol"),
            "RVLT",
        ])

        return (
            options_cm_df
            .with_columns(pl.col("trade_date").cast(pl.Date))
            .join(equity_df2, on=["ticker", "trade_date"], how="inner")
        )

    # Step 10: Compute derived columns
    def _compute_derived_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        iv_short = f"IV{self.tenors[0]}D"
        iv_long = f"IV{self.tenors[1]}D"

        return df.with_columns(
            ((pl.col(iv_long) - pl.col(iv_short)) / pl.col(iv_long)).alias("SLOPE"),
            ((pl.col("RVLT") - pl.col(iv_short)) / pl.col("RVLT")).alias("IVRV_SLOPE"),
            (pl.col("cBidPx") + pl.col("pBidPx")).alias("straddle_price_bid"),
            (pl.col("cAskPx") + pl.col("pAskPx")).alias("straddle_price_ask"),
            ((pl.col("cBidPx") + pl.col("pBidPx") + pl.col("cAskPx") + pl.col("pAskPx")) / 2).alias("straddle_price_mid"),
            ((pl.col("cAskPx") + pl.col("pAskPx")) - (pl.col("cBidPx") + pl.col("pBidPx"))).alias("spread_abs"),
            (((pl.col("cAskPx") + pl.col("pAskPx")) - (pl.col("cBidPx") + pl.col("pBidPx")))
             / ((pl.col("cBidPx") + pl.col("pBidPx") + pl.col("cAskPx") + pl.col("pAskPx")) / 2)).alias("spread_pct_mid"),
            (pl.col("cBidPx_ST") + pl.col("pBidPx_ST")).alias("straddle_price_bid_ST"),  # [CHANGED] ST straddle bid = ST call bid + ST put bid
            (pl.col("cAskPx_ST") + pl.col("pAskPx_ST")).alias("straddle_price_ask_ST"),  # [CHANGED] ST straddle ask = ST call ask + ST put ask
            ((pl.col("cBidPx_ST") + pl.col("pBidPx_ST") + pl.col("cAskPx_ST") + pl.col("pAskPx_ST")) / 2).alias("straddle_price_mid_ST"),  # [CHANGED] ST straddle mid = avg of ST bid and ask
        )

    # Step 11: Final selection + wret
    def _select_final_and_compute_wret(self, df: pl.DataFrame) -> pl.DataFrame:
        shift_n = self._FREQ_SHIFT_MAP.get(self.target_frequency, 1)

        result = df.select([
            "ticker", "expirDate", "trade_date", "cOpra", "pOpra",
            "cOpra_ST", "pOpra_ST", "cOpra_LT", "pOpra_LT",  # [CHANGED] include ST/LT contract names in final output
            "cVolu", "pVolu", "cOi", "pOi",
            "min_leg_volume", "total_volume", "min_leg_oi", "total_oi",
            "dollar_vol", "SLOPE", "IVRV_SLOPE",
            "straddle_price_bid", "straddle_price_ask", "straddle_price_mid",
            "straddle_price_bid_ST", "straddle_price_ask_ST", "straddle_price_mid_ST",  # [CHANGED] include ST straddle prices in final output
            "spread_abs", "spread_pct_mid",
        ]).with_columns(
            (pl.col("straddle_price_bid_ST").shift(-shift_n).over(["ticker", "cOpra_ST", "pOpra_ST"])  # [CHANGED] wret now computed on ST straddle prices, within each ST option pair contract (aligned with 30d IV signal)
             / pl.col("straddle_price_ask_ST") - 1).alias("wret"),
        )

        return result

    @staticmethod
    def add_ntile(
        df: pl.DataFrame,
        col: str,
        n: int,
        by: str,
        output_col: Optional[str] = None,
    ) -> pl.DataFrame:
        """Add ntile (quantile bucket) column.

        Matches R: weekly_slice[, q_iv := ntile(IV_SLOPE, 10), by = trade_date]
        ntile formula: ceiling(rank / count * n)
        """
        if output_col is None:
            output_col = f"q_{col.lower()}"

        df = df.with_columns(
            (
                (pl.col(col).rank("ordinal").over(by) / pl.col(col).count().over(by) * n)
                .ceil()
                .cast(pl.Int32)
            ).alias(output_col)
        )

        return df

    # Step 12: Trade execution — compute returns across fill grid
    def trade_execution(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Compute next-day straddle prices and returns across an execution quality grid.

        SPREAD_CHOICE (from .env):
            - PARTIAL: slippage from mid toward bid/ask (half spread)
            - FULL: slippage across the full bid-ask spread

        For each fill level f in [0, 1]:
            PARTIAL:
                - f=0: execute at mid, f=1: execute at bid/ask
            FULL:
                - f=0: worst (long buys at ask, sells at next_bid; short sells at bid, buys at next_ask)
                - f=1: best (long buys at bid, sells at next_ask; short sells at ask, buys at next_bid)

        Input:
            df (pl.DataFrame): Must contain columns: ticker, expirDate, trade_date, cOpra, pOpra,
                               straddle_price_bid, straddle_price_ask, straddle_price_mid, q_slope

        Output:
            pl.DataFrame: Original columns plus next_mid, next_bid, next_ask,
                          ret_fill_XX and short_ret_fill_XX for each fill level
        """
        spread_choice = os.getenv("SPREAD_CHOICE", "PARTIAL")

        # select relevant columns
        df = df.select("ticker", "expirDate", "trade_date", "cOpra", "pOpra",
                        "cOpra_ST", "pOpra_ST",  # [CHANGED] include ST contract names for downstream trading
                        "straddle_price_bid", "straddle_price_ask", "straddle_price_mid", "q_slope")

        # Compute next-day straddle prices per ticker
        df = df.with_columns([
            pl.col("straddle_price_mid").shift(-1).over("ticker").alias("next_mid"),
            pl.col("straddle_price_bid").shift(-1).over("ticker").alias("next_bid"),
            pl.col("straddle_price_ask").shift(-1).over("ticker").alias("next_ask"),
        ])

        # Compute returns across execution quality grid
        n_grid = int(os.getenv("N_GRID", "10"))
        fill_grid = np.linspace(0, 1, n_grid)

        ret_cols = []
        short_ret_cols = []

        for f in fill_grid:
            fill_pct = int(round(f * 100))
            ret_col = f"ret_fill_{fill_pct:02d}"
            short_ret_col = f"short_ret_fill_{fill_pct:02d}"

            if spread_choice == "FULL":
                # Long: f=0 buy at ask (worst), f=1 buy at bid (best)
                entry_long = pl.col("straddle_price_ask") - f * (pl.col("straddle_price_ask") - pl.col("straddle_price_bid"))
                # f=0 sell at next_bid (worst), f=1 sell at next_ask (best)
                exit_long = pl.col("next_bid") + f * (pl.col("next_ask") - pl.col("next_bid"))

                # Short: f=0 sell at bid (worst), f=1 sell at ask (best)
                entry_short = pl.col("straddle_price_bid") + f * (pl.col("straddle_price_ask") - pl.col("straddle_price_bid"))
                # f=0 buy back at next_ask (worst), f=1 buy back at next_bid (best)
                exit_short = pl.col("next_ask") - f * (pl.col("next_ask") - pl.col("next_bid"))
            else:
                # Long: buy at mid + slippage toward ask, sell at next_mid - slippage toward next_bid
                entry_long = pl.col("straddle_price_mid") + f * (pl.col("straddle_price_ask") - pl.col("straddle_price_mid"))
                exit_long = pl.col("next_mid") - f * (pl.col("next_mid") - pl.col("next_bid"))

                # Short: sell at mid - slippage toward bid, buy back at next_mid + slippage toward next_ask
                entry_short = pl.col("straddle_price_mid") - f * (pl.col("straddle_price_mid") - pl.col("straddle_price_bid"))
                exit_short = pl.col("next_mid") + f * (pl.col("next_ask") - pl.col("next_mid"))

            ret_cols.append(
                pl.when(entry_long.abs() > 1e-10)
                .then(exit_long / entry_long - 1)
                .otherwise(None)
                .alias(ret_col)
            )

            short_ret_cols.append(
                pl.when(exit_short.abs() > 1e-10)
                .then(entry_short / exit_short - 1)
                .otherwise(None)
                .alias(short_ret_col)
            )

        df = df.with_columns(ret_cols + short_ret_cols)
        return df

    # Step 13: Plot long returns by quantile across fill grid
    def plot_long_returns(self, df: pl.DataFrame) -> None:
        """
        Plot mean long returns grouped by rank column quantile for each fill level.

        Input:
            df (pl.DataFrame): Output of trade_execution(), must contain ret_fill_XX columns
                               and the quantile column (e.g. q_slope)

        Output:
            None (displays matplotlib figure)
        """
        target_rank_column = f"q_{self.rank_column.lower()}"
        n_grid = int(os.getenv("N_GRID", "10"))
        fill_levels = [f"{int(round(f * 100)):02d}" for f in np.linspace(0, 1, n_grid)]

        ncols = 4
        nrows = math.ceil(len(fill_levels) / ncols)

        fig, axes = plt.subplots(nrows, ncols, figsize=(20, 4 * nrows))
        plt.style.use('dark_background')
        fig.suptitle(f"Long Returns by {target_rank_column}", fontsize=14)

        for i, fill in enumerate(fill_levels):
            ax = axes[i // ncols][i % ncols]
            target_return_column = f"ret_fill_{fill}"

            data = df.group_by(target_rank_column).agg(
                pl.col(target_return_column).mean().alias("ret")
            ).sort(target_rank_column)

            bars = ax.bar(data[target_rank_column], data["ret"], color="darkblue")
            ax.bar_label(bars, fmt="%.4f", fontsize=6)
            ax.set_title(target_return_column)
            ax.set_xlabel(target_rank_column)
            ax.set_ylabel("ret")

        # hide unused subplots
        for j in range(len(fill_levels), nrows * ncols):
            axes[j // ncols][j % ncols].set_visible(False)

        plt.tight_layout()
        plt.show()

    # Step 14: Plot short returns by quantile across fill grid
    def plot_short_returns(self, df: pl.DataFrame) -> None:
        """
        Plot mean short returns grouped by rank column quantile for each fill level.

        Input:
            df (pl.DataFrame): Output of trade_execution(), must contain short_ret_fill_XX columns
                               and the quantile column (e.g. q_slope)

        Output:
            None (displays matplotlib figure)
        """
        target_rank_column = f"q_{self.rank_column.lower()}"
        n_grid = int(os.getenv("N_GRID", "10"))
        fill_levels = [f"{int(round(f * 100)):02d}" for f in np.linspace(0, 1, n_grid)]

        ncols = 4
        nrows = math.ceil(len(fill_levels) / ncols)

        fig, axes = plt.subplots(nrows, ncols, figsize=(20, 4 * nrows))
        plt.style.use('dark_background')
        fig.suptitle(f"Short Returns by {target_rank_column}", fontsize=14)

        for i, fill in enumerate(fill_levels):
            ax = axes[i // ncols][i % ncols]
            target_return_column = f"short_ret_fill_{fill}"

            data = df.group_by(target_rank_column).agg(
                pl.col(target_return_column).mean().alias("ret")
            ).sort(target_rank_column)

            bars = ax.bar(data[target_rank_column], data["ret"], color="darkgreen")
            ax.bar_label(bars, fmt="%.4f", fontsize=6)
            ax.set_title(target_return_column)
            ax.set_xlabel(target_rank_column)
            ax.set_ylabel("ret")

        # hide unused subplots
        for j in range(len(fill_levels), nrows * ncols):
            axes[j // ncols][j % ncols].set_visible(False)

        plt.tight_layout()
        plt.show()

    # Run the full pipeline for the single ticker set on self.ticker
    def _run_single(self) -> pl.DataFrame:
        # Step 1
        print(f"[{self.ticker}] [1/11] Fetching equity data...")
        equity_df = self._fetch_equity_data()
        print(f"[{self.ticker}]         Equity rows: {len(equity_df)}")

        # Step 2
        print(f"[{self.ticker}] [2/11] Fetching options data...")
        options_df = self._fetch_options_data()
        print(f"[{self.ticker}]         Options rows: {len(options_df)}")

        # Step 3
        print(f"[{self.ticker}] [3/11] Filtering options (Campasano & Linn)...")
        filtered_options_df = self._filter_options(options_df)
        print(f"[{self.ticker}]         After filtering: {len(filtered_options_df)}")

        # Step 4
        print(f"[{self.ticker}] [4/11] Adding equity rolling metrics...")
        equity_df = self._add_equity_rolling_metrics(equity_df)
        print(f"[{self.ticker}]         Equity rows after metrics: {len(equity_df)}")

        # Step 5
        print(f"[{self.ticker}] [5/11] Filtering equity...")
        filtered_equity_df = self._filter_equity(equity_df)
        print(f"[{self.ticker}]         Equity rows after filtering: {len(filtered_equity_df)}")

        # Step 6
        print(f"[{self.ticker}] [6/11] Adding ATM diff, ATM IV, days_to_expiry...")
        filtered_options_df = self._add_options_derived_columns(filtered_options_df)

        # Step 7
        print(f"[{self.ticker}] [7/11] Computing constant-maturity IV interpolation...")
        cm_df = self._compute_constant_maturity_iv(filtered_options_df)
        print(f"[{self.ticker}]         CM rows: {len(cm_df)}")

        # Step 8
        print(f"[{self.ticker}] [8/11] Joining options with constant-maturity data...")
        options_cm_df = self._join_options_with_cm(filtered_options_df, cm_df)
        print(f"[{self.ticker}]         Joined rows: {len(options_cm_df)}")

        # Step 9
        print(f"[{self.ticker}] [9/11] Joining with equity data...")
        consolidated_df = self._join_with_equity(options_cm_df, filtered_equity_df)
        print(f"[{self.ticker}]         Consolidated rows: {len(consolidated_df)}")

        # Step 10
        print(f"[{self.ticker}] [10/11] Computing derived columns...")
        consolidated_df = self._compute_derived_columns(consolidated_df)

        # Step 11
        print(f"[{self.ticker}] [11/11] Final selection + wret...")
        result = self._select_final_and_compute_wret(consolidated_df)
        print(f"[{self.ticker}]         Final rows: {len(result)}")

        self._equity_df = filtered_equity_df
        self._options_df = filtered_options_df

        return result

    # Run multiple tickers in parallel using all CPU cores
    def _run_parallel(self) -> pl.DataFrame:
        n_workers = mp.cpu_count()
        n_tickers = len(self.tickers)
        print(f"Running {n_tickers} tickers in parallel with {n_workers} cores...")

        worker_args = [
            (t, self.massive_api_key, self.db_path,
             self.options_cfg.__dict__, self.equity_cfg.__dict__,
             self.max_history_days, self.tenors,
             self.target_frequency, self.rank_column, self.n_tiles)
            for t in self.tickers
        ]

        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=n_workers, maxtasksperchild=1) as pool:
            results = list(pool.imap_unordered(_worker_run_ticker, worker_args))

        successful = [r for r in results if r is not None]
        if not successful:
            raise ValueError("All tickers failed to process")

        combined = pl.concat(successful)
        print(f"Done. Combined {len(successful)}/{n_tickers} tickers, {len(combined)} total rows.")
        return combined

    # Main entry point
    def run(self) -> pl.DataFrame:
        """Execute the full pipeline. Returns filtered_consolidated_df.

        If ticker is a single string, runs sequentially.
        If ticker is a list, runs in parallel across all CPU cores.
        """
        if self.auto_sync:
            self._sync_orats_cache()

        if len(self.tickers) == 1:
            result = self._run_single()
        else:
            result = self._run_parallel()

        # Rank cross-sectionally (across all tickers per trade_date)
        result = self.add_ntile(
            result,
            col=self.rank_column,
            n=self.n_tiles,
            by="trade_date",
        )
        return result


def _worker_run_ticker(args: tuple) -> Optional[pl.DataFrame]:
    """Module-level worker function for parallel processing (must be picklable)."""
    ticker, api_key, db_path, opts_cfg, eq_cfg, max_hist, tenors, freq, rank_col, n_tiles = args
    try:
        engine = constantIVTermStructureEngine(
            ticker=ticker,
            massive_api_key=api_key,
            db_path=db_path,
            options_filter_config=opts_cfg,
            equity_filter_config=eq_cfg,
            max_history_days=max_hist,
            tenors=tenors,
            target_frequency=freq,
            rank_column=rank_col,
            n_tiles=n_tiles,
            auto_sync=False,  # sync already done in main process
        )
        return engine._run_single()
    except Exception as e:
        print(f"[{ticker}] FAILED: {e}")
        return None


if __name__ == "__main__":
    engine = constantIVTermStructureEngine(ticker="AAPL")
    result = engine.run()
    print(result)
