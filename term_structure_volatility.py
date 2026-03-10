"""
Term Structure Volatility Engine
================================

This module implements the TermStructureVolatilityEngine class for computing
implied volatility term structure metrics and backtesting straddle trading
strategies based on SLOPE quantiles.

Based on Mr. Mislav's methodology:
    Source: https://github.com/MislavSag/alphar/blob/master/R/term_structure_volatility.R
    Paper: Jump Risk and Option Returns (Jim Campasano and Matthew Linn, 2024)

Filters Applied (from the paper):
    (1) The underlying equity has a closing price of at least $10
    (2) The option price must not violate arbitrage conditions (bid-ask spread > 0)
    (3) The option must have a non-zero bid
    (4) The absolute value of the delta must be between 0.35 and 0.65 (ATM range)
    (5) The implied volatility must be between 3% and 200%

Key Definitions:
    - SLOPE = (IVLT - IV1M) / IVLT
        Measures term structure steepness. Positive = normal/contango.
    - RVLT = sqrt(rolling_sum(returns^2, 252))
        252-day realized volatility computed from log returns.
    - IVRV_SLOPE = (RVLT - IV1M) / RVLT
        Compares realized vs implied volatility.
    - wret = lead(straddle_bid) / straddle_ask - 1
        Weekly straddle return (buy at ask, sell at next week's bid).

Data Sources:
    - Options: DuckDB (ORATS_OPTIONS_DB.duckdb)
    - Equity: ClickHouse (primary) or yfinance (fallback)

Output:
    - Results saved to implied_volatility_python.duckdb

Usage:
    from src.term_structure_volatility import TermStructureVolatilityEngine

    engine = TermStructureVolatilityEngine(
        db_path="data/ORATS_OPTIONS_DB.duckdb",
        start_year=2022,
        end_year=2024
    )
    engine.connect()
    results = engine.run_full_pipeline()
    engine.save_results()
    engine.disconnect()

Author: Converted from Mr. Mislav's R code
"""

import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Tuple
from pathlib import Path

import polars as pl
import duckdb
import yfinance as yf
from dotenv import load_dotenv


# =============================================================================
# CONFIGURATION DATACLASSES
# =============================================================================

@dataclass
class FilterConfig:
    """
    Description: Configuration for options filtering criteria based on the
    Campasano & Linn paper methodology.

    These filters ensure we only analyze liquid, fairly-priced ATM options:
    - min_stock_price: Exclude penny stocks (illiquid, high spreads)
    - min/max_delta: Focus on ATM options (delta ~0.5 for calls, ~-0.5 for puts)
    - min/max_iv: Exclude obviously mispriced options
    - short_term_*: Parameters for selecting ~30-day options (IV1M)
    - long_term_min_days: Minimum maturity for long-term IV (IVLT)

    Inputs: N/A (dataclass with defaults)

    Outputs: Configuration object with filter parameters
    """
    min_stock_price: float = 10.0      # Filter 1: stkPx > $10
    min_delta: float = 0.35            # Filter 4: |delta| > 0.35
    max_delta: float = 0.65            # Filter 4: |delta| < 0.65
    min_iv: float = 0.03               # Filter 5: IV > 3%
    max_iv: float = 2.0                # Filter 5: IV < 200%
    short_term_max_days: int = 60      # Max days for short-term IV selection
    short_term_target_days: int = 30   # Target: closest to 30 days
    long_term_min_days: int = 180      # Min days for long-term IV (6 months)


@dataclass
class BacktestConfig:
    """
    Description: Configuration for backtest parameters.

    These parameters control the trading strategy simulation:
    - n_quantiles: How many groups to divide SLOPE into (20 = vigintiles)
    - transaction_cost: Cost per trade (0.003 = 30 basis points)
    - day_of_week: Which day to trade (1=Monday, 3=Wednesday in R's wday)
    - days_to_expiry_range: Filter straddles by days to expiration
    - dollar_vol_rank_threshold: Only trade most liquid stocks

    Inputs: N/A (dataclass with defaults)

    Outputs: Configuration object with backtest parameters
    """
    n_quantiles: int = 20                        # Number of quantile buckets
    transaction_cost: float = 0.003              # 30 basis points per trade
    day_of_week: int = 3                         # Wednesday (Polars weekday: Mon=1...Sun=7)
    days_to_expiry_range: Tuple[int, int] = (15, 45)  # DTE filter for straddles
    dollar_vol_rank_threshold: int = 150         # Top N most liquid stocks


# =============================================================================
# MAIN ENGINE CLASS
# =============================================================================

class TermStructureVolatilityEngine:
    """
    Description: Engine for computing implied volatility term structure metrics
    and backtesting straddle trading strategies based on SLOPE quantiles.

    This class implements Mr. Mislav's methodology from:
    https://github.com/MislavSag/alphar/blob/master/R/term_structure_volatility.R

    The pipeline computes:
    1. Filter raw options data (5 criteria from the paper)
    2. Identify ATM options (strike closest to stock price)
    3. Compute short-term IV (IV1M) - closest to 30 days
    4. Compute long-term IV (IVLT) - shortest >= 180 days
    5. Compute SLOPE = (IVLT - IV1M) / IVLT
    6. Compute realized volatility (RVLT) from equity data
    7. Compute IVRV_SLOPE = (RVLT - IV1M) / RVLT
    8. Compute straddle prices and weekly returns
    9. Rank by SLOPE into quantiles
    10. Construct backtest portfolio

    All numerical results must match the R implementation to 15 decimal places.

    Inputs:
        db_path (str): Path to DuckDB with ORATS options data
        output_db_path (str): Path to save results
        start_year (int): First year of data to process
        end_year (int): Last year of data to process
        filter_config (FilterConfig): Options filtering configuration
        backtest_config (BacktestConfig): Backtest configuration
        env_path (str): Path to .env file for credentials

    Usage:
        engine = TermStructureVolatilityEngine()
        engine.connect()
        results = engine.run_full_pipeline()
        engine.save_results()
        engine.disconnect()
    """

    def __init__(
        self,
        db_path: str = "data/ORATS_OPTIONS_DB.duckdb",
        output_db_path: str = "data/implied_volatility_python.duckdb",
        start_year: int = 2022,
        end_year: int = 2024,
        filter_config: Optional[FilterConfig] = None,
        backtest_config: Optional[BacktestConfig] = None,
        env_path: Optional[str] = None,
    ) -> None:
        """
        Description: Initialize the Term Structure Volatility Engine.

        Inputs:
            db_path (str): Path to DuckDB with ORATS options data
            output_db_path (str): Path to save results
            start_year (int): First year of data to process (default: 2022)
            end_year (int): Last year of data to process (default: 2024)
            filter_config (FilterConfig): Options filtering configuration
            backtest_config (BacktestConfig): Backtest configuration
            env_path (str): Path to .env file for credentials

        Outputs: None (initializes instance attributes)
        """
        # Store configuration
        self.db_path = db_path
        self.output_db_path = output_db_path
        self.start_year = start_year
        self.end_year = end_year
        self.filter_config = filter_config or FilterConfig()
        self.backtest_config = backtest_config or BacktestConfig()

        # Database connections (initialized on connect())
        self._duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None
        self._output_conn: Optional[duckdb.DuckDBPyConnection] = None

        # Results storage
        self._results: Optional[pl.DataFrame] = None
        self._portfolio: Optional[pl.DataFrame] = None

        # Load environment variables for ClickHouse (if available)
        self._load_env(env_path)

        print(f"[INFO] TermStructureVolatilityEngine initialized")
        print(f"       db_path: {db_path}")
        print(f"       output_db_path: {output_db_path}")
        print(f"       years: {start_year}-{end_year}")

    def _load_env(self, env_path: Optional[str] = None) -> None:
        """
        Description: Load environment variables from .env file.

        Inputs:
            env_path (str): Path to .env file. If None, searches current/parent dirs.

        Outputs: None (loads variables into os.environ)
        """
        if env_path:
            path = Path(env_path)
        else:
            # Try current directory, then parent
            path = Path(".env")
            if not path.exists():
                path = Path("..") / ".env"

        if path.exists():
            load_dotenv(path)
            print(f"[INFO] Loaded environment from {path.resolve()}")

    # =========================================================================
    # DATABASE CONNECTION METHODS
    # =========================================================================

    def connect(self) -> "TermStructureVolatilityEngine":
        """
        Description: Establish database connections.

        Opens read-only connection to source DuckDB (options data) and
        read-write connection to output DuckDB (for saving results).

        Inputs: None

        Outputs:
            TermStructureVolatilityEngine: Self for method chaining

        Example:
            engine = TermStructureVolatilityEngine()
            engine.connect()
        """
        # Connect to source database (read-only)
        self._duckdb_conn = duckdb.connect(self.db_path, read_only=True)
        print(f"[INFO] Connected to source DuckDB: {self.db_path}")

        return self

    def disconnect(self) -> None:
        """
        Description: Close all database connections.

        Inputs: None

        Outputs: None (closes connections)
        """
        if self._duckdb_conn is not None:
            self._duckdb_conn.close()
            self._duckdb_conn = None
            print("[INFO] Disconnected from source DuckDB")

        if self._output_conn is not None:
            self._output_conn.close()
            self._output_conn = None
            print("[INFO] Disconnected from output DuckDB")

    def _get_available_year_tables(self) -> List[int]:
        """
        Description: Get list of years for which options_data_{year} tables exist.

        This prevents SQL errors when querying non-existent tables.

        Inputs: None

        Outputs:
            List[int]: Sorted list of years with available data

        R-Python Parity Note:
            R: tables <- dbGetQuery(con, "SHOW TABLES;")[[1]]
            Python: Same query, extract table names
        """
        if self._duckdb_conn is None:
            raise ConnectionError("DuckDB not connected. Call connect() first.")

        # Get all table names
        tables = [row[0] for row in self._duckdb_conn.execute("SHOW TABLES").fetchall()]

        # Extract years from options_data_{year} tables
        years = []
        for t in tables:
            if t.startswith("options_data_"):
                try:
                    year = int(t.replace("options_data_", ""))
                    if self.start_year <= year <= self.end_year:
                        years.append(year)
                except ValueError:
                    continue

        return sorted(years)

    # =========================================================================
    # DATA LOADING METHODS
    # =========================================================================

    def load_options_data(self, ticker: str) -> pl.DataFrame:
        """
        Description: Load and filter options data for a specific ticker.

        Queries options_data_{year} tables for the specified years,
        applying all 5 filters from the Campasano & Linn paper.

        Inputs:
            ticker (str): Stock symbol (e.g., "AAPL")

        Outputs:
            pl.DataFrame: Filtered options data with columns:
                ticker, trade_date, expirDate, yte, strike, stkPx,
                cBidPx, cAskPx, cMidIv, pBidPx, pAskPx, pMidIv, delta

        R-Python Parity Note:
            R: Uses data.table with filters in SQL WHERE clause
            Python: Same SQL filters, returns Polars DataFrame
        """
        if self._duckdb_conn is None:
            raise ConnectionError("DuckDB not connected. Call connect() first.")

        # Sanitize ticker (alphanumeric only)
        safe_ticker = "".join(c for c in ticker.upper() if c.isalnum())
        if not safe_ticker:
            raise ValueError(f"Invalid ticker: {ticker}")

        # Get available years
        years = self._get_available_year_tables()
        if not years:
            print(f"[WARN] No options data tables found for years {self.start_year}-{self.end_year}")
            return pl.DataFrame()

        # Build SQL with filters
        cfg = self.filter_config
        columns = """
            ticker, trade_date, expirDate, yte, strike, stkPx,
            cBidPx, cAskPx, cMidIv, pBidPx, pAskPx, pMidIv,
            cBidIv, pBidIv, delta
        """

        # Apply all 5 filters from the paper in the SQL query
        # Filter 1: stkPx > $10
        # Filter 2: bid-ask spread > 0 (no stale quotes)
        # Filter 3: bid prices > 0 (liquid options)
        # Filter 4: |delta| between 0.35 and 0.65 (ATM range)
        # Filter 5: IV between 3% and 200%
        filters = f"""
            ticker = '{safe_ticker}'
            AND stkPx > {cfg.min_stock_price}
            AND ABS(cBidPx - cAskPx) > 0
            AND ABS(pBidPx - pAskPx) > 0
            AND cBidPx > 0
            AND pBidPx > 0
            AND ABS(delta) > {cfg.min_delta}
            AND ABS(delta) < {cfg.max_delta}
            AND cBidIv > {cfg.min_iv}
            AND cBidIv < {cfg.max_iv}
            AND pBidIv > {cfg.min_iv}
            AND pBidIv < {cfg.max_iv}
        """

        # Build UNION ALL query across years
        union_query = " UNION ALL ".join([
            f"SELECT {columns} FROM options_data_{year} WHERE {filters}"
            for year in years
        ])

        try:
            df = self._duckdb_conn.execute(union_query).pl()
        except Exception as e:
            print(f"[ERROR] Query failed for {safe_ticker}: {e}")
            return pl.DataFrame()

        return df

    def load_equity_data(self, ticker: str, trade_dates: List) -> pl.DataFrame:
        """
        Description: Load equity data using yfinance and compute RVLT.

        Downloads daily OHLCV data from Yahoo Finance, then computes:
        - Log returns: ln(close / shift(close, 1))
        - RVLT: sqrt(rolling_sum(returns^2, 252))

        Inputs:
            ticker (str): Stock symbol (e.g., "AAPL")
            trade_dates (List): List of trade dates to filter by

        Outputs:
            pl.DataFrame: Equity data with columns:
                symbol, date, close, open, RVLT

        R-Python Parity Note:
            R: log(close / shift(close, 1L)) for returns
            R: sqrt(frollsum(returns_sq, n=252L)) for RVLT
            Python: Same formulas using Polars
        """
        if not trade_dates:
            return pl.DataFrame()

        # Get date range from trade dates
        min_date = min(trade_dates)
        max_date = max(trade_dates)

        # Extend start date by 300 days for RVLT warm-up (need 252 days of history)
        # Convert to string format for yfinance
        import datetime as dt
        if isinstance(min_date, str):
            min_date = dt.datetime.strptime(min_date, "%Y-%m-%d").date()
        if isinstance(max_date, str):
            max_date = dt.datetime.strptime(max_date, "%Y-%m-%d").date()

        start_date = min_date - dt.timedelta(days=400)  # Extra buffer for RVLT calculation
        end_date = max_date + dt.timedelta(days=1)

        try:
            # Download from yfinance
            # Use auto_adjust=False to get unadjusted prices (matching R's tidyquant)
            # This is critical for R-Python parity
            yf_ticker = yf.Ticker(ticker)
            hist = yf_ticker.history(
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                auto_adjust=False  # Use unadjusted prices to match R
            )

            if hist.empty:
                print(f"[WARN] No equity data from yfinance for {ticker}")
                return pl.DataFrame()

            # Convert to Polars
            df = pl.from_pandas(hist.reset_index())

            # Select and rename columns
            # Note: With auto_adjust=False, 'Close' is unadjusted close
            df = df.select([
                pl.col("Date").cast(pl.Date).alias("date"),
                pl.col("Close").cast(pl.Float64).alias("close"),
                pl.col("Open").cast(pl.Float64).alias("open"),
            ]).with_columns(
                pl.lit(ticker).alias("symbol")
            )

            # Sort by date (required for shift operations)
            df = df.sort("date")

            # Compute log returns: ln(close / shift(close, 1))
            # R equivalent: log(close / shift(close, 1L))
            df = df.with_columns(
                (pl.col("close") / pl.col("close").shift(1)).log().alias("returns")
            )

            # Compute RVLT: sqrt(rolling_sum(returns^2, 252))
            # R equivalent: sqrt(frollsum(returns_sq, n=252L, align="right"))
            df = df.with_columns(
                pl.col("returns").pow(2).rolling_sum(window_size=252).sqrt().alias("RVLT")
            )

            # Drop NAs (first 252 rows won't have RVLT)
            df = df.drop_nulls(subset=["RVLT"])

            # Select final columns
            df = df.select(["symbol", "date", "close", "open", "RVLT"])

            return df

        except Exception as e:
            print(f"[ERROR] Failed to load equity data for {ticker}: {e}")
            return pl.DataFrame()

    # =========================================================================
    # CORE COMPUTATION METHODS
    # =========================================================================

    def filter_options(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Description: Apply all 5 filters from the Campasano & Linn paper.

        Note: Filters are already applied in SQL query (load_options_data),
        but this method can be used for additional filtering if needed.

        Filters:
            (1) stkPx > $10 - no penny stocks
            (2) bid-ask spread > 0 - no stale quotes
            (3) bid prices > 0 - liquid options only
            (4) 0.35 < |delta| < 0.65 - ATM range
            (5) 0.03 < IV < 2.0 - reasonable volatility

        Inputs:
            df (pl.DataFrame): Raw options data

        Outputs:
            pl.DataFrame: Filtered options data

        R-Python Parity Note:
            R: filter_options function in term_structure_volatility.R
            Python: Same filters using Polars expressions
        """
        cfg = self.filter_config

        return df.filter(
            (pl.col("stkPx") > cfg.min_stock_price) &              # Filter 1
            (abs(pl.col("cBidPx") - pl.col("cAskPx")) > 0) &       # Filter 2
            (abs(pl.col("pBidPx") - pl.col("pAskPx")) > 0) &       # Filter 2
            (pl.col("cBidPx") > 0) &                                # Filter 3
            (pl.col("pBidPx") > 0) &                                # Filter 3
            (abs(pl.col("delta")) > cfg.min_delta) &               # Filter 4
            (abs(pl.col("delta")) < cfg.max_delta) &               # Filter 4
            (pl.col("cBidIv") > cfg.min_iv) &                      # Filter 5
            (pl.col("cBidIv") < cfg.max_iv) &                      # Filter 5
            (pl.col("pBidIv") > cfg.min_iv) &                      # Filter 5
            (pl.col("pBidIv") < cfg.max_iv)                        # Filter 5
        )

    def identify_atm_options(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Description: Find ATM options for each ticker/date/expiry combination.

        ATM = strike closest to stock price. For each unique combination of
        (ticker, trade_date, expirDate), selects the option with minimum
        |strike - stkPx|.

        Also computes ATM_IV as average of call and put mid IVs:
            ATM_IV = (cMidIv + pMidIv) / 2

        Inputs:
            df (pl.DataFrame): Filtered options data with columns:
                ticker, trade_date, expirDate, strike, stkPx, cMidIv, pMidIv

        Outputs:
            pl.DataFrame: ATM options with ATM_IV and days_to_expiry columns

        R-Python Parity Note:
            R: setorder(options_df, atm_diff)
               atm_options <- options_df[, .SD[1L], by = .(trade_date, expirDate)]
            Python: .sort("atm_diff").group_by(...).first()

            The key is using first() after sorting, which matches R's .SD[1L]
        """
        # Compute absolute difference between strike and stock price
        df = df.with_columns(
            (pl.col("strike") - pl.col("stkPx")).abs().alias("atm_diff")
        )

        # Sort by atm_diff and take first row per (ticker, trade_date, expirDate)
        # R equivalent: setorder(atm_diff), .SD[1L] by group
        atm_options = (
            df
            .sort("atm_diff")
            .group_by(["ticker", "trade_date", "expirDate"], maintain_order=True)
            .first()
        )

        # Compute ATM IV as average of call and put mid IVs
        # ATM straddle IV = (call IV + put IV) / 2
        atm_options = atm_options.with_columns(
            ((pl.col("cMidIv") + pl.col("pMidIv")) / 2).alias("ATM_IV")
        )

        # Compute days to expiry from yte (years to expiry)
        # yte is in years, multiply by 365 to get days
        atm_options = atm_options.with_columns(
            (pl.col("yte") * 365).alias("days_to_expiry")
        )

        return atm_options.sort(["ticker", "trade_date", "expirDate"])

    def compute_short_term_iv(self, atm_options: pl.DataFrame) -> pl.DataFrame:
        """
        Description: Get IV1M - short-term IV closest to 30 days.

        Selection criteria:
        - 0 < days_to_expiry <= 60 (short_term_max_days)
        - Minimum distance to 30 days (short_term_target_days)

        For each (ticker, trade_date), selects the ATM option expiration
        closest to 30 days.

        Inputs:
            atm_options (pl.DataFrame): ATM options with ATM_IV, days_to_expiry

        Outputs:
            pl.DataFrame: One row per (ticker, trade_date) with IV1M

        R-Python Parity Note:
            R: short_term <- atm_options[days_to_expiry > 0 & days_to_expiry <= 60]
               short_term[, distance_to_30 := abs(days_to_expiry - 30)]
               setorder(short_term, distance_to_30)
               short_term_iv <- short_term[, .SD[1L], by = .(trade_date)]
            Python: Filter, compute distance, sort, group_by().first()
        """
        cfg = self.filter_config

        # Filter: 0 < days_to_expiry <= short_term_max_days (60)
        short_term = atm_options.filter(
            (pl.col("days_to_expiry") > 0) &
            (pl.col("days_to_expiry") <= cfg.short_term_max_days)
        )

        # Compute distance to target (30 days)
        short_term = short_term.with_columns(
            (pl.col("days_to_expiry") - cfg.short_term_target_days).abs().alias("distance_to_30")
        )

        # Sort by distance and take first row per (ticker, trade_date)
        # This gives the expiration closest to 30 days
        short_term_iv = (
            short_term
            .sort("distance_to_30")
            .group_by(["ticker", "trade_date"], maintain_order=True)
            .first()
            .drop("distance_to_30")
        )

        return short_term_iv.sort(["ticker", "trade_date"])

    def compute_long_term_iv(self, atm_options: pl.DataFrame) -> pl.DataFrame:
        """
        Description: Get IVLT - long-term IV with shortest maturity >= 180 days.

        Selection criteria:
        - days_to_expiry >= 180 (long_term_min_days)
        - Shortest available maturity (minimum days_to_expiry within filter)

        For each (ticker, trade_date), selects the ATM option with the
        shortest maturity that is at least 6 months out.

        Inputs:
            atm_options (pl.DataFrame): ATM options with ATM_IV, days_to_expiry

        Outputs:
            pl.DataFrame: One row per (ticker, trade_date) with IVLT

        R-Python Parity Note:
            R: long_term <- atm_options[days_to_expiry >= 180]
               setorder(long_term, trade_date, expirDate, days_to_expiry)
               long_term_iv <- long_term[, .SD[1L], by = .(trade_date)]
            Python: Filter, sort by days_to_expiry, group_by().first()
        """
        cfg = self.filter_config

        # Filter: days_to_expiry >= long_term_min_days (180)
        long_term = atm_options.filter(
            pl.col("days_to_expiry") >= cfg.long_term_min_days
        )

        # Sort by days_to_expiry (shortest first) and take first row per (ticker, trade_date)
        # This gives the shortest maturity >= 180 days
        long_term_iv = (
            long_term
            .sort(["ticker", "trade_date", "days_to_expiry"])
            .group_by(["ticker", "trade_date"], maintain_order=True)
            .first()
        )

        return long_term_iv.sort(["ticker", "trade_date"])

    def compute_slope(self, iv_data: pl.DataFrame) -> pl.DataFrame:
        """
        Description: Compute SLOPE = (IVLT - IV1M) / IVLT for term structure analysis.

        SLOPE measures the steepness of the implied volatility term structure:
        - SLOPE > 0: Normal/contango (long-term vol > short-term vol)
        - SLOPE < 0: Inverted/backwardation (near-term fear elevated)
        - SLOPE = 0: Flat term structure

        Inputs:
            iv_data (pl.DataFrame): DataFrame with columns IV1M and IVLT

        Outputs:
            pl.DataFrame: Input DataFrame with SLOPE column added

        R-Python Parity Note:
            R: iv_data_simplified[, SLOPE := (IVLT - IV1M) / IVLT]
            Python: Uses pl.when() for division-by-zero protection

            Division by zero protection: Returns None if |IVLT| < 1e-10

        Example:
            >>> result = engine.compute_slope(iv_data)
            >>> print(result.select(["ticker", "trade_date", "SLOPE"]).head())
        """
        # Formula: SLOPE = (IVLT - IV1M) / IVLT
        # Protect against division by zero when IVLT is near zero
        return iv_data.with_columns(
            pl.when(pl.col("IVLT").abs() > 1e-10)
            .then((pl.col("IVLT") - pl.col("IV1M")) / pl.col("IVLT"))
            .otherwise(None)
            .alias("SLOPE")
        )

    def compute_ivrv_slope(self, iv_data: pl.DataFrame) -> pl.DataFrame:
        """
        Description: Compute IVRV_SLOPE = (RVLT - IV1M) / RVLT.

        IVRV_SLOPE compares realized vs implied volatility:
        - IVRV_SLOPE > 0: Realized vol > Implied vol (options cheap)
        - IVRV_SLOPE < 0: Implied vol > Realized vol (options expensive)

        Inputs:
            iv_data (pl.DataFrame): DataFrame with columns RVLT and IV1M

        Outputs:
            pl.DataFrame: Input DataFrame with IVRV_SLOPE column added

        R-Python Parity Note:
            R: iv_data[, IVRV_SLOPE := fifelse(abs(RVLT) > 1e-10, (RVLT - IV1M) / RVLT, NA_real_)]
            Python: Uses pl.when() for division-by-zero protection
        """
        # Formula: IVRV_SLOPE = (RVLT - IV1M) / RVLT
        # Protect against division by zero when RVLT is near zero
        return iv_data.with_columns(
            pl.when(pl.col("RVLT").abs() > 1e-10)
            .then((pl.col("RVLT") - pl.col("IV1M")) / pl.col("RVLT"))
            .otherwise(None)
            .alias("IVRV_SLOPE")
        )

    # =========================================================================
    # STRADDLE AND BACKTEST METHODS
    # =========================================================================

    def compute_straddle_prices(self, options: pl.DataFrame) -> pl.DataFrame:
        """
        Description: Calculate straddle bid, ask, and mid prices.

        A straddle = Buy Call + Buy Put at same strike and expiry.

        Prices:
        - straddle_price_bid = cBidPx + pBidPx (what market makers bid)
        - straddle_price_ask = cAskPx + pAskPx (what market makers ask)
        - straddle_price_mid = (bid + ask) / 2 (midpoint)

        Inputs:
            options (pl.DataFrame): ATM options with cBidPx, cAskPx, pBidPx, pAskPx

        Outputs:
            pl.DataFrame: Options with straddle price columns added

        R-Python Parity Note:
            R: atm_options_30d[, straddle_price_bid := cBidPx + pBidPx]
               atm_options_30d[, straddle_price_ask := cAskPx + pAskPx]
            Python: Same formulas using Polars with_columns
        """
        return options.with_columns([
            (pl.col("cBidPx") + pl.col("pBidPx")).alias("straddle_price_bid"),
            (pl.col("cAskPx") + pl.col("pAskPx")).alias("straddle_price_ask"),
            ((pl.col("cBidPx") + pl.col("pBidPx") + pl.col("cAskPx") + pl.col("pAskPx")) / 2)
            .alias("straddle_price_mid"),
        ])

    def compute_weekly_returns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Description: Compute weekly straddle returns.

        Formula: wret = lead(straddle_price_bid) / straddle_price_ask - 1

        This represents a realistic trading scenario:
        - Entry: Buy straddle at the ask (what market maker charges)
        - Exit: Sell straddle at the bid (what market maker pays)
        - Return = next_week_bid / current_week_ask - 1

        The lead() looks forward, so wret at time t is the return from
        buying at t and selling at t+1 week.

        Inputs:
            df (pl.DataFrame): Data with straddle_price_bid, straddle_price_ask

        Outputs:
            pl.DataFrame: Data with wret column added

        R-Python Parity Note:
            R: back[, wret := shift(straddle_price_bid, 1, type = "lead") / straddle_price_ask - 1, by = ticker]
            Python: shift(-1) is equivalent to R's shift(type="lead")
        """
        return df.with_columns(
            # shift(-1) = next value (forward-looking), same as R's shift(type="lead")
            (pl.col("straddle_price_bid").shift(-1).over("ticker") / pl.col("straddle_price_ask") - 1)
            .alias("wret")
        )

    @staticmethod
    def add_ntile(
        df: pl.DataFrame,
        col: str,
        n: int,
        by: str
    ) -> pl.DataFrame:
        """
        Description: Rank rows into n approximately equal quantiles within groups.
        Replicates R's dplyr::ntile() function exactly.

        The ntile algorithm:
            1. Rank values within each group using ordinal ranking (no ties)
            2. Divide ranks into n buckets: q = floor((rank - 1) * n / count) + 1
            3. Result: integers from 1 to n (inclusive)

        This is critical for matching R output because:
        - R's ntile uses "first" tie-breaking (same as Polars "ordinal")
        - The formula ensures approximately equal bucket sizes

        Inputs:
            df (pl.DataFrame): Input DataFrame
            col (str): Column name to rank (e.g., "SLOPE")
            n (int): Number of quantiles (e.g., 20 for vigintiles, 10 for deciles)
            by (str): Column to partition by (e.g., "trade_date")

        Outputs:
            pl.DataFrame: DataFrame with 'q' column added (Int32, values 1 to n)

        R-Python Parity:
            R:      back[, q := dplyr::ntile(SLOPE, 20), by = trade_date]
            Python: add_ntile(df, col="SLOPE", n=20, by="trade_date")

            The key to matching is using rank("ordinal") which assigns
            unique ranks to tied values in order of appearance.

        Example:
            >>> df = pl.DataFrame({
            ...     "trade_date": ["2024-01-01"]*10,
            ...     "SLOPE": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
            ... })
            >>> result = TermStructureVolatilityEngine.add_ntile(df, "SLOPE", 5, "trade_date")
            >>> # Result: q = [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]
        """
        # Check required columns exist
        missing = {col, by} - set(df.columns)
        if missing:
            raise ValueError(f"DataFrame is missing required columns: {missing}")

        # ntile formula: q = floor((rank - 1) * n / count) + 1
        # rank("ordinal") gives unique ranks (no ties), matching R's frank(ties.method="first")
        return df.with_columns(
            (
                ((pl.col(col).rank("ordinal").over(by) - 1) * n)
                // pl.col(col).count().over(by)
                + 1
            )
            .cast(pl.Int32)
            .alias("q")
        )

    def generate_quantiles(
        self,
        df: pl.DataFrame,
        n_quantiles: Optional[int] = None,
        day_of_week: Optional[int] = None
    ) -> pl.DataFrame:
        """
        Description: Full ranking pipeline for backtest.

        Steps:
        1. Filter to specified weekday (default: Wednesday)
        2. Filter by days_to_expiry range
        3. Filter by dollar_vol_rank threshold
        4. Compute lead prices and weekly returns
        5. Keep only consecutive weeks (7-day gap)
        6. Rank by SLOPE into quantiles within each trade_date

        Inputs:
            df (pl.DataFrame): IV term structure data with columns:
                ticker, trade_date, SLOPE, close, stkPx_short, open,
                straddle_price_bid, straddle_price_ask, days_to_expiry, dollar_vol_rank
            n_quantiles (int): Number of quantile buckets (default from config: 20)
            day_of_week (int): Weekday to filter (1=Mon...7=Sun, default from config: 3=Wed)

        Outputs:
            pl.DataFrame: Data with wret and q columns

        R-Python Parity Note:
            R: data.table::wday(trade_date, week_start=1L) gives Mon=1...Sun=7
            Python: pl.col("trade_date").dt.weekday() gives Mon=1...Sun=7

            IMPORTANT: Polars weekday() matches R's wday(week_start=1)!
        """
        cfg = self.backtest_config
        n_quantiles = n_quantiles or cfg.n_quantiles
        day_of_week = day_of_week or cfg.day_of_week

        # Step 1: Filter to specified weekday
        # Polars weekday: Mon=1, Tue=2, Wed=3, Thu=4, Fri=5, Sat=6, Sun=7
        # R wday(week_start=1): Same mapping
        result = df.filter(pl.col("trade_date").dt.weekday() == day_of_week)

        if len(result) == 0:
            raise ValueError(f"No rows after filtering to weekday={day_of_week}")

        # Step 2: Filter by days_to_expiry range
        dte_min, dte_max = cfg.days_to_expiry_range
        result = result.filter(
            pl.col("days_to_expiry").is_between(dte_min, dte_max)
        )

        # Step 3: Filter by dollar_vol_rank threshold (most liquid stocks only)
        if "dollar_vol_rank" in result.columns:
            result = result.filter(
                pl.col("dollar_vol_rank") < cfg.dollar_vol_rank_threshold
            )

        # Sort by ticker and date for correct shifting
        result = result.sort(["ticker", "trade_date"])

        # Step 4: Compute lead columns for weekly return calculation
        result = result.with_columns([
            pl.col("straddle_price_bid").shift(-1).over("ticker").alias("straddle_bid_lead"),
            pl.col("trade_date").shift(-1).over("ticker").alias("trade_date_lead"),
        ])

        # Step 5: Compute days gap and filter to consecutive weeks (7 days)
        result = result.with_columns(
            (pl.col("trade_date_lead") - pl.col("trade_date")).dt.total_days().alias("days_gap")
        )

        # Only keep rows where next observation is exactly 7 days later
        result = result.with_columns(
            pl.when(pl.col("days_gap") == 7)
            .then(pl.col("straddle_bid_lead") / pl.col("straddle_price_ask") - 1)
            .otherwise(None)
            .alias("wret")
        )

        # Drop rows without valid weekly return
        result = result.drop_nulls(subset=["wret"])

        if len(result) == 0:
            raise ValueError("No valid weekly returns computed")

        # Step 6: Assign quantile buckets based on SLOPE
        result = self.add_ntile(result, col="SLOPE", n=n_quantiles, by="trade_date")

        return result

    def compute_backtest_portfolio(
        self,
        df: pl.DataFrame,
        quantile: int = 1,
        transaction_cost: Optional[float] = None
    ) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """
        Description: Construct equal-weighted portfolio for selected quantile.

        Steps:
        1. Filter to specified quantile (default: 1 = lowest SLOPE)
        2. Compute equal weights: w = 1 / n_stocks per trade_date
        3. Compute portfolio return: sum(w * wret) - transaction_cost

        Inputs:
            df (pl.DataFrame): Data with wret, q columns from generate_quantiles()
            quantile (int): Which quantile to select (1 = lowest SLOPE)
            transaction_cost (float): Cost per trade (default from config: 0.003)

        Outputs:
            Tuple[pl.DataFrame, pl.DataFrame]:
                - back_q: Filtered data for selected quantile with weights
                - portfolio: Aggregated portfolio returns by trade_date

        R-Python Parity Note:
            R: back_q = back[q == 1]
               back_q[, weights := 1 / .N, by = trade_date]
               portfolio = back_q[, .(ret = sum(weights * (wret) - 0.003)), by = trade_date]
            Python: Same logic using Polars
        """
        cfg = self.backtest_config
        transaction_cost = transaction_cost if transaction_cost is not None else cfg.transaction_cost

        # Step 1: Filter to selected quantile
        back_q = df.filter(pl.col("q") == quantile)

        if len(back_q) == 0:
            print(f"[WARN] No rows in quantile {quantile}")
            return pl.DataFrame(), pl.DataFrame()

        # Step 2: Compute equal weights within each trade_date
        # R equivalent: back_q[, weights := 1 / .N, by = trade_date]
        back_q = back_q.with_columns(
            (1.0 / pl.col("ticker").count().over("trade_date")).alias("weights")
        )

        # Sort by trade_date for time series
        back_q = back_q.sort("trade_date")

        # Step 3: Compute portfolio returns
        # R equivalent: portfolio = back_q[, .(ret = sum(weights * wret) - 0.003), by = trade_date]
        portfolio = (
            back_q
            .group_by("trade_date", maintain_order=True)
            .agg(
                ((pl.col("weights") * pl.col("wret")).sum() - transaction_cost).alias("ret")
            )
            .sort("trade_date")
        )

        return back_q, portfolio

    # =========================================================================
    # FULL PIPELINE
    # =========================================================================

    def run_full_pipeline(self, tickers: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Description: Run the complete term structure volatility pipeline.

        Pipeline steps:
        1. Get list of tickers (from equity data or specified list)
        2. For each ticker:
           a. Load and filter options data
           b. Load equity data (yfinance)
           c. Identify ATM options
           d. Compute short-term IV (IV1M) and long-term IV (IVLT)
           e. Compute SLOPE
           f. Merge with equity data
           g. Compute IVRV_SLOPE
           h. Compute straddle prices
        3. Combine all ticker results
        4. Generate quantiles
        5. Compute backtest portfolio

        Inputs:
            tickers (List[str]): Optional list of tickers to process.
                                 If None, extracts tickers from options data.

        Outputs:
            pl.DataFrame: Complete term structure data for all tickers

        Example:
            engine = TermStructureVolatilityEngine()
            engine.connect()
            results = engine.run_full_pipeline(tickers=["AAPL", "MSFT", "GOOGL"])
            engine.save_results()
            engine.disconnect()
        """
        if self._duckdb_conn is None:
            raise ConnectionError("DuckDB not connected. Call connect() first.")

        # Get tickers if not provided
        if tickers is None:
            # Extract unique tickers from options data
            years = self._get_available_year_tables()
            if not years:
                raise ValueError("No options data tables found")

            # Get tickers from first year (they should be consistent)
            tickers = self._duckdb_conn.execute(
                f"SELECT DISTINCT ticker FROM options_data_{years[0]} LIMIT 1000"
            ).fetchall()
            tickers = [t[0] for t in tickers]
            print(f"[INFO] Found {len(tickers)} tickers in options data")

        all_results = []
        successful = 0
        failed = 0

        for idx, ticker in enumerate(tickers, 1):
            try:
                # Progress indicator
                print(f"\r[INFO] Processing {idx}/{len(tickers)}: {ticker}    ", end="", flush=True)

                # Step 2a: Load filtered options data
                options_df = self.load_options_data(ticker)
                if len(options_df) == 0:
                    failed += 1
                    continue

                # Step 2b: Load equity data
                trade_dates = options_df["trade_date"].to_list()
                equity_df = self.load_equity_data(ticker, trade_dates)
                if len(equity_df) == 0:
                    failed += 1
                    continue

                # Step 2c: Identify ATM options
                atm_options = self.identify_atm_options(options_df)
                if len(atm_options) == 0:
                    failed += 1
                    continue

                # Step 2d: Compute short-term and long-term IV
                short_term_iv = self.compute_short_term_iv(atm_options)
                long_term_iv = self.compute_long_term_iv(atm_options)

                if len(short_term_iv) == 0 or len(long_term_iv) == 0:
                    failed += 1
                    continue

                # Rename columns for merge
                short_cols = [c for c in short_term_iv.columns if c not in ["ticker", "trade_date"]]
                short_term_iv = short_term_iv.rename({c: f"{c}_short" for c in short_cols})

                long_cols = [c for c in long_term_iv.columns if c not in ["ticker", "trade_date"]]
                long_term_iv = long_term_iv.rename({c: f"{c}_long" for c in long_cols})

                # Merge short and long term
                iv_data = short_term_iv.join(
                    long_term_iv,
                    on=["ticker", "trade_date"],
                    how="inner"
                )

                if len(iv_data) == 0:
                    failed += 1
                    continue

                # Select and rename IV columns
                iv_data = iv_data.select([
                    "ticker",
                    "trade_date",
                    pl.col("stkPx_short"),
                    pl.col("ATM_IV_short").alias("IV1M"),
                    pl.col("ATM_IV_long").alias("IVLT"),
                    pl.col("days_to_expiry_short").alias("days_to_expiry"),
                    # Keep straddle price components
                    pl.col("cBidPx_short").alias("cBidPx"),
                    pl.col("cAskPx_short").alias("cAskPx"),
                    pl.col("pBidPx_short").alias("pBidPx"),
                    pl.col("pAskPx_short").alias("pAskPx"),
                ])

                # Step 2e: Compute SLOPE
                iv_data = self.compute_slope(iv_data)

                # Step 2f: Merge with equity data
                equity_df = equity_df.select([
                    pl.col("symbol").str.to_uppercase().alias("ticker"),
                    pl.col("date").cast(pl.Date).alias("trade_date"),
                    pl.col("RVLT"),
                    pl.col("close"),
                    pl.col("open"),
                ])

                iv_data = iv_data.join(
                    equity_df,
                    on=["ticker", "trade_date"],
                    how="left"
                )

                # Compute dollar volume rank
                iv_data = iv_data.with_columns(
                    (pl.col("close") * 1e6)  # Simplified; real would use actual volume
                    .rank(method="ordinal", descending=True)
                    .over("trade_date")
                    .alias("dollar_vol_rank")
                )

                # Step 2g: Compute IVRV_SLOPE
                iv_data = self.compute_ivrv_slope(iv_data)

                # Step 2h: Compute straddle prices
                iv_data = self.compute_straddle_prices(iv_data)

                # Drop nulls
                iv_data = iv_data.drop_nulls()

                if len(iv_data) > 0:
                    all_results.append(iv_data)
                    successful += 1
                else:
                    failed += 1

            except Exception as e:
                print(f"\n[ERROR] Failed for {ticker}: {e}")
                failed += 1
                continue

        print(f"\n[INFO] Pipeline complete: {successful} successful, {failed} failed")

        if not all_results:
            print("[ERROR] No results to combine")
            return pl.DataFrame()

        # Combine all results
        self._results = pl.concat(all_results)
        print(f"[INFO] Combined results: {len(self._results)} rows")

        return self._results

    def save_results(self, table_name: str = "term_structure_data") -> None:
        """
        Description: Save results to output DuckDB database.

        Inputs:
            table_name (str): Name of table to create (default: "term_structure_data")

        Outputs: None (writes to database)

        R-Python Parity Note:
            R: dbWriteTable(con, "term_structure_data", results)
            Python: Register Polars DataFrame with DuckDB and create table from it
        """
        if self._results is None or len(self._results) == 0:
            print("[WARN] No results to save")
            return

        # Connect to output database (read-write)
        self._output_conn = duckdb.connect(self.output_db_path, read_only=False)

        # Drop existing table if exists
        self._output_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Register the Polars DataFrame with DuckDB and create table
        # DuckDB can directly query Polars DataFrames when registered
        results_df = self._results  # Local variable for registration
        self._output_conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM results_df"
        )

        # Verify
        row_count = self._output_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[INFO] Saved {row_count} rows to {self.output_db_path}::{table_name}")

        self._output_conn.close()
        self._output_conn = None

    # =========================================================================
    # CONTEXT MANAGER SUPPORT
    # =========================================================================

    def __enter__(self) -> "TermStructureVolatilityEngine":
        """Context manager entry - connects to databases."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit - disconnects from databases."""
        self.disconnect()
        return False


# =============================================================================
# CONVENIENCE FUNCTION
# =============================================================================

def run_term_structure_analysis(
    tickers: Optional[List[str]] = None,
    start_year: int = 2022,
    end_year: int = 2024,
    **kwargs
) -> pl.DataFrame:
    """
    Description: Convenience function to run term structure analysis.

    Inputs:
        tickers (List[str]): List of tickers to analyze (None = all from data)
        start_year (int): First year of data
        end_year (int): Last year of data
        **kwargs: Additional arguments passed to TermStructureVolatilityEngine

    Outputs:
        pl.DataFrame: Term structure analysis results

    Example:
        results = run_term_structure_analysis(
            tickers=["AAPL", "MSFT", "GOOGL"],
            start_year=2022,
            end_year=2024
        )
    """
    with TermStructureVolatilityEngine(
        start_year=start_year,
        end_year=end_year,
        **kwargs
    ) as engine:
        results = engine.run_full_pipeline(tickers=tickers)
        engine.save_results()
        return results


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("Term Structure Volatility Engine")
    print("=" * 70)

    # Run with a few test tickers
    test_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]

    engine = TermStructureVolatilityEngine(
        db_path="data/ORATS_OPTIONS_DB.duckdb",
        output_db_path="data/implied_volatility_python.duckdb",
        start_year=2022,
        end_year=2024
    )

    engine.connect()

    try:
        results = engine.run_full_pipeline(tickers=test_tickers)
        print(f"\nResults shape: {results.shape}")
        print(f"\nColumns: {results.columns}")
        print(f"\nSample data:\n{results.head(10)}")

        engine.save_results()
    finally:
        engine.disconnect()

    print("\n" + "=" * 70)
    print("Done!")
    print("=" * 70)
