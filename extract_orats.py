"""
ORATS Options Data Extraction Engine

This module provides the ExtractOratsEngine class for downloading and processing
ORATS SMV Strikes data from an FTP server into a DuckDB database.

GETS ALL THE DATA FROM ORATS ITERATIVELY ACROSS ALL YEARS (2007-CURRENT YEAR). 
Note:
    Except for holidays and weekends
    Enforces all data type in ORATS
Warning:
    If run the code in kernel, the kernel might crash due to running for too long (need to change periodically).
    Estimated Time to Complete all years (2007-2026): 6 hours (occasional monitoring)

Saves the file in a duckdb under /data

Usage:
    # ==================================================================================
    # Sync Required Years Only (empty years + current year)
    # RUN THIS FOR BOTH DAILY UPDATES AND IF STARTING FRESH
    # ==================================================================================
    # Skips years with partial data (weekends/holidays)
    # Only downloads years that are completely empty OR current year

    import importlib
    import src.extract_orats
    importlib.reload(src.extract_orats)
    from src.extract_orats import ExtractOratsEngine

    with ExtractOratsEngine() as engine:
        stats = engine.sync_required_years_sequential(verbose=True)
"""

from ftplib import FTP, error_perm
from datetime import date, timedelta, datetime
from io import BytesIO
import zipfile
import os
import time
import os
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
import duckdb
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm


class ExtractOratsEngine:
    """
    Engine for extracting ORATS options data from FTP and storing in DuckDB.

    Attributes:
        host: FTP server hostname
        username: FTP login username
        password: FTP login password
        remote_path: Base path on FTP server
        db_path: Path to DuckDB database file
    """

    # ORATS CSV Schema
    ORATS_SCHEMA = {
        "ticker": pl.Utf8,
        "cOpra": pl.Utf8, # OPRA symbol for call option
        "pOpra": pl.Utf8, # OPRA symbol for put option
        "stkPx": pl.Float64,
        "expirDate": pl.Utf8,
        "yte": pl.Float64,
        "strike": pl.Float64,
        "cVolu": pl.Float64,
        "cOi": pl.Float64,
        "pVolu": pl.Float64,
        "pOi": pl.Float64,
        "cBidPx": pl.Float64,
        "cValue": pl.Float64,
        "cAskPx": pl.Float64,
        "pBidPx": pl.Float64,
        "pValue": pl.Float64,
        "pAskPx": pl.Float64,
        "cBidIv": pl.Float64,
        "cMidIv": pl.Float64,
        "cAskIv": pl.Float64,
        "smoothSmvVol": pl.Float64,
        "pBidIv": pl.Float64,
        "pMidIv": pl.Float64,
        "pAskIv": pl.Float64,
        "iRate": pl.Float64,
        "divRate": pl.Float64,
        "residualRateData": pl.Float64,
        "delta": pl.Float64,
        "gamma": pl.Float64,
        "theta": pl.Float64,
        "vega": pl.Float64,
        "rho": pl.Float64,
        "phi": pl.Float64,
        "driftlessTheta": pl.Float64,
        "extVol": pl.Float64,
        "extCTheo": pl.Float64,
        "extPTheo": pl.Float64,
        "spot_px": pl.Utf8,
        "trade_date": pl.Utf8,
    }

    def __init__(
        self,
        host: str = os.getenv("ORATS_FTP_HOST", ""),
        username: str = os.getenv("ORATS_FTP_USERNAME", ""),
        password: str = os.getenv("ORATS_FTP_PASSWORD", ""),
        remote_path: str = "/smvstrikes",
        db_path: str = "data/ORATS_OPTIONS_DB.duckdb",
        start_year: int = 2007,
        end_year: Optional[int] = None,
    ):
        """
        Initialize the ORATS extraction engine.

        Args:
            host: FTP server hostname
            username: FTP login username
            password: FTP login password
            remote_path: Base path on FTP server
            db_path: Path to DuckDB database file
            start_year: First year of data to extract
            end_year: Last year of data to extract (defaults to current year)
        """
        self.host = host
        self.username = username
        self.password = password
        self.remote_path = remote_path
        self.db_path = db_path
        self.start_year = start_year
        self.end_year = end_year or datetime.now().year

        # Connection objects (initialized on connect)
        self.ftp: Optional[FTP] = None
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self._current_year: Optional[int] = None

    def connect(self) -> None:
        """Establish FTP and DuckDB connections."""
        self._connect_ftp()
        self._connect_duckdb()

    def disconnect(self) -> None:
        """Close FTP and DuckDB connections."""
        self._disconnect_ftp()
        self._disconnect_duckdb()

    def _connect_ftp(self) -> None:
        """Establish FTP connection."""
        if self.ftp is not None:
            self._disconnect_ftp()

        self.ftp = FTP(self.host, timeout=30)
        response = self.ftp.login(user=self.username, passwd=self.password)
        print(f"✓ FTP Login successful: {response}")

    def _disconnect_ftp(self) -> None:
        """Close FTP connection."""
        if self.ftp is not None:
            try:
                self.ftp.quit()
            except Exception:
                pass
            self.ftp = None
            self._current_year = None
            print("✓ FTP connection closed")

    def _connect_duckdb(self) -> None:
        """Establish DuckDB connection."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        print(f"✓ Connected to database: {self.db_path}")

    def _disconnect_duckdb(self) -> None:
        """Close DuckDB connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            print("✓ DuckDB connection closed")

    def _change_directory_year(self, year: int) -> None:
        """
        Change FTP directory to the specified year.

        Args:
            year: Year to navigate to
        """
        if self.ftp is None:
            raise ConnectionError("FTP not connected. Call connect() first.")

        if self._current_year == year:
            return  # Already in correct directory

        if year <= 2012:
            path = f"{self.remote_path}_2007_2012/{year}"
        else:
            path = f"{self.remote_path}/{year}"

        self.ftp.cwd(path)
        self._current_year = year
        print(f"✓ Changed to directory: {self.ftp.pwd()}")

    @staticmethod
    def _get_dates_for_year(year: int) -> List[str]:
        """
        Generate all date strings for a given year.

        Args:
            year: Year to generate dates for

        Returns:
            List of date strings in YYYYMMDD format
        """
        start = date(year, 1, 1)
        end = date(year, 12, 31)
        return [
            (start + timedelta(days=i)).strftime("%Y%m%d")
            for i in range((end - start).days + 1)
        ]

    @staticmethod
    def _get_us_market_holidays(year: int) -> set:
        """
        Compute US stock market (NYSE/NASDAQ) holidays for a given year.

        Covers: New Year's Day, MLK Day, Presidents' Day, Good Friday,
        Memorial Day, Juneteenth (2021+), Independence Day, Labor Day,
        Thanksgiving Day, Christmas Day. Observance rules shift
        Saturday holidays to Friday and Sunday holidays to Monday.

        Args:
            year: Year to compute holidays for

        Returns:
            Set of date objects representing market holidays
        """
        holidays = set()

        def _observe(d: date) -> date:
            """Apply NYSE observed-holiday rule."""
            if d.weekday() == 5:   # Saturday -> Friday
                return d - timedelta(days=1)
            elif d.weekday() == 6: # Sunday -> Monday
                return d + timedelta(days=1)
            return d

        # New Year's Day (Jan 1)
        holidays.add(_observe(date(year, 1, 1)))

        # Martin Luther King Jr. Day - 3rd Monday of January
        d = date(year, 1, 1)
        mondays = 0
        while True:
            if d.weekday() == 0:
                mondays += 1
                if mondays == 3:
                    holidays.add(d)
                    break
            d += timedelta(days=1)

        # Presidents' Day - 3rd Monday of February
        d = date(year, 2, 1)
        mondays = 0
        while True:
            if d.weekday() == 0:
                mondays += 1
                if mondays == 3:
                    holidays.add(d)
                    break
            d += timedelta(days=1)

        # Good Friday - 2 days before Easter (Anonymous Gregorian algorithm)
        a = year % 19
        b = year // 100
        c = year % 100
        d_val = b // 4
        e = b % 4
        f = (b + 8) // 25
        g = (b - f + 1) // 3
        h = (19 * a + b - d_val - g + 15) % 30
        i = c // 4
        k = c % 4
        l_val = (32 + 2 * e + 2 * i - h - k) % 7
        m = (a + 11 * h + 22 * l_val) // 451
        month = (h + l_val - 7 * m + 114) // 31
        day = ((h + l_val - 7 * m + 114) % 31) + 1
        easter = date(year, month, day)
        holidays.add(easter - timedelta(days=2))

        # Memorial Day - last Monday of May
        d = date(year, 5, 31)
        while d.weekday() != 0:
            d -= timedelta(days=1)
        holidays.add(d)

        # Juneteenth National Independence Day (June 19) - observed since 2021
        if year >= 2021:
            holidays.add(_observe(date(year, 6, 19)))

        # Independence Day (July 4)
        holidays.add(_observe(date(year, 7, 4)))

        # Labor Day - 1st Monday of September
        d = date(year, 9, 1)
        while d.weekday() != 0:
            d += timedelta(days=1)
        holidays.add(d)

        # Thanksgiving Day - 4th Thursday of November
        d = date(year, 11, 1)
        thursdays = 0
        while True:
            if d.weekday() == 3:
                thursdays += 1
                if thursdays == 4:
                    holidays.add(d)
                    break
            d += timedelta(days=1)

        # Christmas Day (Dec 25)
        holidays.add(_observe(date(year, 12, 25)))

        return holidays

    @staticmethod
    def _get_trading_dates_for_year(year: int) -> List[str]:
        """
        Generate expected US equity trading-day date strings for a given year.

        Excludes weekends (Sat/Sun) and US market holidays.

        Args:
            year: Year to generate trading dates for

        Returns:
            Sorted list of date strings in YYYYMMDD format
        """
        holidays = ExtractOratsEngine._get_us_market_holidays(year)
        start = date(year, 1, 1)
        end = date(year, 12, 31)

        trading_dates = []
        current = start
        while current <= end:
            if current.weekday() < 5 and current not in holidays:
                trading_dates.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)

        return trading_dates

    def _extract_single_file(self, date_str: str) -> Optional[pl.DataFrame]:
        """
        Extract data from a single date's zip file.

        Args:
            date_str: Date string in YYYYMMDD format

        Returns:
            Polars DataFrame or None if file doesn't exist or is empty
        """
        if self.ftp is None:
            raise ConnectionError("FTP not connected. Call connect() first.")

        file_name = f"ORATS_SMV_Strikes_{date_str}.zip"

        try:
            buffer = BytesIO()
            self.ftp.retrbinary(f"RETR {file_name}", buffer.write)
            buffer.seek(0)

            with zipfile.ZipFile(buffer) as z:
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as f:
                    df = pl.read_csv(
                        f,
                        schema_overrides=self.ORATS_SCHEMA,
                        null_values=["NULL", "null", ""],
                        ignore_errors=True,
                    )

            # Check if DataFrame is empty
            if df.is_empty():
                return None

            # Cast date columns and reorder
            df = df.with_columns([
                pl.col("expirDate").str.to_date("%m/%d/%Y"),
                pl.col("trade_date").str.to_date("%m/%d/%Y"),
                pl.col("spot_px").cast(pl.Float64, strict=False),
            ]).select(
                "ticker",
                "expirDate",
                "trade_date",
                pl.exclude(["ticker", "expirDate", "trade_date"])
            )

            return df

        except error_perm:
            # File doesn't exist (e.g., weekend/holiday)
            return None
        except pl.exceptions.NoDataError:
            # Empty CSV file
            return None
        except Exception as e:
            # Log unexpected errors but don't crash
            print(f"✗ Error extracting {date_str}: {e}")
            return None

    def extract_year(
        self,
        year: int,
        limit: Optional[int] = None,
        drop_existing: bool = True,
        verbose: bool = True,
    ) -> pl.DataFrame:
        """
        Extract all data for a given year.

        Args:
            year: Year to extract
            limit: Maximum number of days to extract (None for all)
            drop_existing: Whether to drop existing table before inserting
            verbose: Whether to print progress messages

        Returns:
            Combined Polars DataFrame of all extracted data
        """
        if self.ftp is None or self.conn is None:
            raise ConnectionError("Not connected. Call connect() first.")

        table_name = f"options_data_{year}"

        if drop_existing:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Navigate to year directory
        self._change_directory_year(year)

        # Get dates to process
        dates = self._get_dates_for_year(year)
        if limit:
            dates = dates[:limit]

        # Extract each file
        all_dfs = []
        extracted_count = 0

        for date_str in dates:
            df = self._extract_single_file(date_str)
            if df is not None:
                all_dfs.append(df)
                extracted_count += 1
                if verbose:
                    print(f"✓ Extracted {date_str} ({len(df):,} rows)")

        if not all_dfs:
            print(f"✗ No data extracted for {year}")
            return pl.DataFrame()

        # Combine all DataFrames
        combined_df = pl.concat(all_dfs)

        # Store in DuckDB
        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM combined_df")

        print(f"\n✓ Created table '{table_name}' with {len(combined_df):,} rows from {extracted_count} files")

        return combined_df

    def _create_ftp_connection(self, year: int) -> FTP:
        """
        Create a new FTP connection and navigate to the year directory.

        Args:
            year: Year to navigate to

        Returns:
            Connected FTP object
        """
        ftp = FTP(self.host, timeout=60)
        ftp.login(user=self.username, passwd=self.password)

        # Navigate to year directory
        if year <= 2012:
            path = f"{self.remote_path}_2007_2012/{year}"
        else:
            path = f"{self.remote_path}/{year}"

        ftp.cwd(path)
        return ftp

    def _download_single_ftp(
        self,
        ftp: FTP,
        date_str: str,
    ) -> Tuple[str, Optional[pl.DataFrame]]:
        """
        Download and parse a single file using an existing FTP connection.

        Args:
            ftp: FTP connection to use
            date_str: Date string in YYYYMMDD format

        Returns:
            Tuple of (date_str, DataFrame or None)
        """
        file_name = f"ORATS_SMV_Strikes_{date_str}.zip"

        try:
            buffer = BytesIO()
            ftp.retrbinary(f"RETR {file_name}", buffer.write)
            buffer.seek(0)

            with zipfile.ZipFile(buffer) as z:
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as f:
                    df = pl.read_csv(
                        f,
                        schema_overrides=self.ORATS_SCHEMA,
                        null_values=["NULL", "null", ""],
                        ignore_errors=True,
                    )

            # Check if DataFrame is empty
            if df.is_empty():
                return (date_str, None)

            # Cast date columns and reorder
            df = df.with_columns([
                pl.col("expirDate").str.to_date("%m/%d/%Y"),
                pl.col("trade_date").str.to_date("%m/%d/%Y"),
                pl.col("spot_px").cast(pl.Float64, strict=False),
            ]).select(
                "ticker",
                "expirDate",
                "trade_date",
                pl.exclude(["ticker", "expirDate", "trade_date"])
            )

            return (date_str, df)

        except error_perm:
            # File doesn't exist (weekend/holiday)
            return (date_str, None)
        except pl.exceptions.NoDataError:
            # Empty CSV file
            return (date_str, None)
        except Exception as e:
            print(f"✗ Error processing {date_str}: {e}")
            return (date_str, None)

    def _get_http_url(self, year: int, date_str: str) -> str:
        """
        Construct the HTTP URL for a given file.

        Args:
            year: Year of the data
            date_str: Date string in YYYYMMDD format

        Returns:
            Full HTTP URL to the zip file
        """
        # HTTP URL structure varies by year range
        if year <= 2012:
            # 2007-2012: /files/path/smvstrikes_2007_2012/{year}/
            path = f"/path/smvstrikes_2007_2012/{year}"
        elif year <= 2025:
            # 2013-2025: /files/path/smvstrikes/{year}/
            path = f"/path/smvstrikes/{year}"
        else:
            # 2026+: /files/path/{year}/
            path = f"/path/{year}"

        file_name = f"ORATS_SMV_Strikes_{date_str}.zip"
        return f"https://{self.host}/files{path}/{file_name}"

    def _download_and_parse_http(
        self,
        session: requests.Session,
        url: str,
        date_str: str,
    ) -> Tuple[str, Optional[pl.DataFrame]]:
        """
        Download a single file via HTTP and parse it.

        Args:
            session: Requests session to use
            url: URL to download
            date_str: Date string for logging

        Returns:
            Tuple of (date_str, DataFrame or None)
        """
        try:
            response = session.get(url, timeout=60)

            if response.status_code == 404:
                # File doesn't exist (weekend/holiday)
                return (date_str, None)

            response.raise_for_status()

            # Parse the zip file
            buffer = BytesIO(response.content)
            with zipfile.ZipFile(buffer) as z:
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as f:
                    df = pl.read_csv(f, schema_overrides=self.ORATS_SCHEMA)

            # Cast date columns and reorder
            df = df.with_columns([
                pl.col("expirDate").str.to_date("%m/%d/%Y"),
                pl.col("trade_date").str.to_date("%m/%d/%Y"),
                pl.col("spot_px").cast(pl.Float64),
            ]).select(
                "ticker",
                "expirDate",
                "trade_date",
                pl.exclude(["ticker", "expirDate", "trade_date"])
            )

            return (date_str, df)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return (date_str, None)
            print(f"✗ HTTP error for {date_str}: {e}")
            return (date_str, None)
        except Exception as e:
            print(f"✗ Error processing {date_str}: {e}")
            return (date_str, None)

    def extract_year_parallel(
        self,
        year: int,
        limit: Optional[int] = None,
        max_workers: int = 2,
        drop_existing: bool = True,
        verbose: bool = True,
        delay_between_files: float = 0.1,
    ) -> pl.DataFrame:
        """
        Extract all data for a given year using parallel FTP connections.

        Uses a small pool of FTP connections to avoid server blocking.
        Each worker maintains its own FTP connection.

        Args:
            year: Year to extract
            limit: Maximum number of days to extract (None for all)
            max_workers: Number of parallel FTP connections (recommended: 2, max: 3)
            drop_existing: Whether to drop existing table before inserting
            verbose: Whether to print progress messages
            delay_between_files: Seconds to wait between downloads per worker

        Returns:
            Combined Polars DataFrame of all extracted data

        Warning:
            Keep max_workers at 2 to avoid getting blocked by the FTP server.
            If you get blocked, wait 15-30 minutes and try again.
        """
        if self.conn is None:
            raise ConnectionError("DuckDB not connected. Call connect() first.")

        # Limit workers to avoid getting blocked
        max_workers = min(max_workers, 3)

        table_name = f"options_data_{year}"

        if drop_existing:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Get dates to process
        dates = self._get_dates_for_year(year)
        if limit:
            dates = dates[:limit]

        all_dfs = []
        extracted_count = 0
        failed_count = 0

        print(f"Downloading {len(dates)} files with {max_workers} FTP connections...")

        def worker_task(date_batch: List[str], worker_id: int) -> List[Tuple[str, Optional[pl.DataFrame]]]:
            """Worker function that processes a batch of dates with its own FTP connection."""
            results = []
            try:
                ftp = self._create_ftp_connection(year)
                for date_str in date_batch:
                    result = self._download_single_ftp(ftp, date_str)
                    results.append(result)
                    time.sleep(delay_between_files)
                ftp.quit()
            except Exception as e:
                print(f"✗ Worker {worker_id} error: {e}")
            return results

        # Split dates into batches for each worker
        batch_size = (len(dates) + max_workers - 1) // max_workers
        date_batches = [dates[i:i + batch_size] for i in range(0, len(dates), batch_size)]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit batch tasks
            futures = {
                executor.submit(worker_task, batch, i): i
                for i, batch in enumerate(date_batches)
            }

            # Collect results with progress bar
            with tqdm(total=len(dates), desc=f"Extracting {year}", disable=not verbose) as pbar:
                for future in as_completed(futures):
                    results = future.result()
                    for date_str, df in results:
                        if df is not None:
                            all_dfs.append(df)
                            extracted_count += 1
                        else:
                            failed_count += 1
                        pbar.update(1)

        if not all_dfs:
            print(f"✗ No data extracted for {year}")
            return pl.DataFrame()

        # Combine all DataFrames
        combined_df = pl.concat(all_dfs)

        # Store in DuckDB
        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM combined_df")

        print(f"\n✓ Created table '{table_name}' with {len(combined_df):,} rows")
        print(f"  Files extracted: {extracted_count}, Skipped (no data): {failed_count}")

        return combined_df

    def extract_all_years_parallel(
        self,
        limit_per_year: Optional[int] = None,
        max_workers: int = 4,
        verbose: bool = True,
    ) -> None:
        """
        Extract data for all years using parallel HTTP downloads.

        Args:
            limit_per_year: Maximum number of days to extract per year (None for all)
            max_workers: Number of parallel download threads
            verbose: Whether to print progress messages
        """
        for year in range(self.start_year, self.end_year + 1):
            print(f"\n{'='*50}")
            print(f"Processing year: {year}")
            print('='*50)
            self.extract_year_parallel(
                year,
                limit=limit_per_year,
                max_workers=max_workers,
                verbose=verbose,
            )

    def extract_all_years(
        self,
        limit_per_year: Optional[int] = None,
        verbose: bool = True,
    ) -> None:
        """
        Extract data for all years in the configured range.

        Args:
            limit_per_year: Maximum number of days to extract per year (None for all)
            verbose: Whether to print progress messages
        """
        for year in range(self.start_year, self.end_year + 1):
            print(f"\n{'='*50}")
            print(f"Processing year: {year}")
            print('='*50)
            self.extract_year(year, limit=limit_per_year, verbose=verbose)

    def query(self, sql: str) -> pl.DataFrame:
        """
        Execute a SQL query against the DuckDB database.

        Args:
            sql: SQL query string

        Returns:
            Query results as a Polars DataFrame
        """
        if self.conn is None:
            raise ConnectionError("DuckDB not connected. Call connect() first.")

        return self.conn.execute(sql).pl()

    def list_tables(self) -> List[str]:
        """List all tables in the database."""
        if self.conn is None:
            raise ConnectionError("DuckDB not connected. Call connect() first.")

        result = self.conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]

    def get_table_info(self, table_name: str) -> pl.DataFrame:
        """Get schema information for a table."""
        return self.query(f"DESCRIBE {table_name}")

    def get_existing_dates(self, year: int) -> set:
        """
        Get the set of trade dates already in the database for a given year.

        Args:
            year: Year to check

        Returns:
            Set of date strings in YYYYMMDD format that exist in the database
        """
        if self.conn is None:
            raise ConnectionError("DuckDB not connected. Call connect() first.")

        table_name = f"options_data_{year}"

        # Check if table exists
        tables = self.list_tables()
        if table_name not in tables:
            return set()

        # Get distinct trade dates from the table
        try:
            result = self.conn.execute(f"""
                SELECT DISTINCT trade_date FROM {table_name}
            """).fetchall()

            # Convert dates to YYYYMMDD format strings
            existing_dates = set()
            for row in result:
                if row[0] is not None:
                    # Handle both date objects and strings
                    if isinstance(row[0], str):
                        existing_dates.add(row[0].replace("-", ""))
                    else:
                        existing_dates.add(row[0].strftime("%Y%m%d"))

            return existing_dates
        except Exception as e:
            print(f"✗ Error reading existing dates for {year}: {e}")
            return set()

    def get_missing_dates(self, year: int, trading_days_only: bool = True) -> List[str]:
        """
        Get list of dates that are missing from the database for a given year.

        Only considers dates up to yesterday (not today, as data may not be available yet).

        Args:
            year: Year to check
            trading_days_only: If True (default), only considers expected trading days
                               (weekdays minus US market holidays). If False, considers
                               all calendar days (legacy behaviour).

        Returns:
            List of missing date strings in YYYYMMDD format
        """
        # Get the candidate dates for the year
        if trading_days_only:
            all_dates = set(self._get_trading_dates_for_year(year))
        else:
            all_dates = set(self._get_dates_for_year(year))

        # Don't include today or future dates
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        # Filter to only dates up to yesterday
        all_dates = {d for d in all_dates if d <= yesterday.strftime("%Y%m%d")}

        # Get existing dates
        existing_dates = self.get_existing_dates(year)

        # Calculate missing dates
        missing_dates = sorted(all_dates - existing_dates)

        return missing_dates

    def sync_year(
        self,
        year: int,
        max_workers: int = 2,
        delay_between_files: float = 0.1,
        verbose: bool = True,
    ) -> pl.DataFrame:
        """
        Sync a single year by downloading only missing dates.

        Checks the database for existing dates and only downloads missing ones.
        Appends new data to the existing table.

        Args:
            year: Year to sync
            max_workers: Number of parallel FTP connections (recommended: 2)
            delay_between_files: Seconds to wait between downloads
            verbose: Whether to print progress messages

        Returns:
            DataFrame of newly extracted data (empty if nothing new)
        """
        if self.conn is None:
            raise ConnectionError("DuckDB not connected. Call connect() first.")

        table_name = f"options_data_{year}"

        # Get missing dates
        missing_dates = self.get_missing_dates(year)

        if not missing_dates:
            if verbose:
                print(f"✓ Year {year}: Already up to date (no missing dates)")
            return pl.DataFrame()

        if verbose:
            print(f"Year {year}: {len(missing_dates)} missing dates to download")

        # Limit workers
        max_workers = min(max_workers, 3)

        all_dfs = []
        extracted_count = 0
        failed_count = 0

        def worker_task(date_batch: List[str], worker_id: int) -> List[Tuple[str, Optional[pl.DataFrame]]]:
            """Worker function that processes a batch of dates."""
            results = []
            try:
                ftp = self._create_ftp_connection(year)
                for date_str in date_batch:
                    result = self._download_single_ftp(ftp, date_str)
                    results.append(result)
                    time.sleep(delay_between_files)
                ftp.quit()
            except Exception as e:
                print(f"✗ Worker {worker_id} error: {e}")
            return results

        # Split dates into batches for each worker
        batch_size = max(1, (len(missing_dates) + max_workers - 1) // max_workers)
        date_batches = [missing_dates[i:i + batch_size] for i in range(0, len(missing_dates), batch_size)]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(worker_task, batch, i): i
                for i, batch in enumerate(date_batches)
            }

            with tqdm(total=len(missing_dates), desc=f"Syncing {year}", disable=not verbose) as pbar:
                for future in as_completed(futures):
                    results = future.result()
                    for date_str, df in results:
                        if df is not None:
                            all_dfs.append(df)
                            extracted_count += 1
                        else:
                            failed_count += 1
                        pbar.update(1)

        if not all_dfs:
            if verbose:
                print(f"  No new data extracted (all {failed_count} dates were weekends/holidays)")
            return pl.DataFrame()

        # Combine new data
        new_df = pl.concat(all_dfs)

        # Check if table exists
        tables = self.list_tables()
        if table_name in tables:
            # Append to existing table
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM new_df")
            if verbose:
                print(f"\n✓ Appended {len(new_df):,} rows to '{table_name}'")
        else:
            # Create new table
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM new_df")
            if verbose:
                print(f"\n✓ Created table '{table_name}' with {len(new_df):,} rows")

        if verbose:
            print(f"  Files extracted: {extracted_count}, Skipped (no data): {failed_count}")

        return new_df

    def sync_all(
        self,
        max_workers: int = 2,
        delay_between_files: float = 0.1,
        verbose: bool = True,
    ) -> dict:
        """
        Sync all years from 2007 to current year.

        Checks each year for missing dates and downloads only what's needed.
        This is the main "cache update" function.

        Args:
            max_workers: Number of parallel FTP connections (recommended: 2)
            delay_between_files: Seconds to wait between downloads
            verbose: Whether to print progress messages

        Returns:
            Dictionary with sync statistics per year
        """
        if self.conn is None:
            raise ConnectionError("DuckDB not connected. Call connect() first.")

        # Set year range: 2007 to current year
        start_year = 2007
        end_year = datetime.now().year

        print(f"{'='*60}")
        print(f"ORATS Data Sync: {start_year} - {end_year}")
        print(f"{'='*60}")

        # Get overview of what needs to be done
        total_missing = 0
        years_to_sync = []

        for year in range(start_year, end_year + 1):
            missing = self.get_missing_dates(year)
            if missing:
                years_to_sync.append((year, len(missing)))
                total_missing += len(missing)

        if not years_to_sync:
            print("\n✓ All years are up to date!")
            return {}

        print(f"\nYears with missing data:")
        for year, count in years_to_sync:
            print(f"  {year}: {count} dates missing")
        print(f"\nTotal: {total_missing} dates to download\n")

        # Sync each year
        stats = {}
        for year in range(start_year, end_year + 1):
            print(f"\n{'-'*40}")
            df = self.sync_year(
                year,
                max_workers=max_workers,
                delay_between_files=delay_between_files,
                verbose=verbose,
            )
            stats[year] = {
                "rows_added": len(df),
                "was_synced": len(df) > 0,
            }

        # Summary
        print(f"\n{'='*60}")
        print("SYNC COMPLETE")
        print(f"{'='*60}")

        total_rows = sum(s["rows_added"] for s in stats.values())
        years_updated = sum(1 for s in stats.values() if s["was_synced"])

        print(f"Years updated: {years_updated}")
        print(f"Total rows added: {total_rows:,}")

        return stats

    def get_sync_status(self) -> pl.DataFrame:
        """
        Get a summary of sync status for all years.

        Returns:
            DataFrame with columns: year, existing_dates, expected_trading_days,
            missing_trading_days, missing_calendar_days, total_rows
        """
        if self.conn is None:
            raise ConnectionError("DuckDB not connected. Call connect() first.")

        start_year = 2007
        end_year = datetime.now().year
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        rows = []
        for year in range(start_year, end_year + 1):
            existing = len(self.get_existing_dates(year))
            missing_trading = len(self.get_missing_dates(year, trading_days_only=True))
            missing_calendar = len(self.get_missing_dates(year, trading_days_only=False))

            # Expected trading days up to yesterday for this year
            all_trading = self._get_trading_dates_for_year(year)
            expected_trading = len([
                d for d in all_trading if d <= yesterday.strftime("%Y%m%d")
            ])

            # Get row count if table exists
            table_name = f"options_data_{year}"
            tables = self.list_tables()
            if table_name in tables:
                result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                total_rows = result[0] if result else 0
            else:
                total_rows = 0

            rows.append({
                "year": year,
                "existing_dates": existing,
                "expected_trading_days": expected_trading,
                "missing_trading_days": missing_trading,
                "missing_calendar_days": missing_calendar,
                "total_rows": total_rows,
            })

        return pl.DataFrame(rows)

    def sync_year_sequential(
        self,
        year: int,
        verbose: bool = True,
    ) -> pl.DataFrame:
        """
        Sync a single year by downloading only missing dates (sequential/single connection).

        Uses a single FTP connection - slower but safest option to avoid blocking.

        Args:
            year: Year to sync
            verbose: Whether to print progress messages

        Returns:
            DataFrame of newly extracted data (empty if nothing new)
        """
        if self.ftp is None or self.conn is None:
            raise ConnectionError("Not connected. Call connect() first.")

        table_name = f"options_data_{year}"

        # Get missing dates
        missing_dates = self.get_missing_dates(year)

        if not missing_dates:
            if verbose:
                print(f"✓ Year {year}: Already up to date (no missing dates)")
            return pl.DataFrame()

        if verbose:
            print(f"Year {year}: {len(missing_dates)} missing dates to download")

        # Navigate to year directory
        self._change_directory_year(year)

        # Extract each missing date sequentially
        all_dfs = []
        extracted_count = 0
        failed_count = 0

        for date_str in tqdm(missing_dates, desc=f"Syncing {year}", disable=not verbose):
            df = self._extract_single_file(date_str)
            if df is not None:
                all_dfs.append(df)
                extracted_count += 1
            else:
                failed_count += 1

        if not all_dfs:
            if verbose:
                print(f"  No new data extracted (all {failed_count} dates were weekends/holidays)")
            return pl.DataFrame()

        # Combine new data
        new_df = pl.concat(all_dfs)

        # Check if table exists
        tables = self.list_tables()
        if table_name in tables:
            # Append to existing table
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM new_df")
            if verbose:
                print(f"\n✓ Appended {len(new_df):,} rows to '{table_name}'")
        else:
            # Create new table
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM new_df")
            if verbose:
                print(f"\n✓ Created table '{table_name}' with {len(new_df):,} rows")

        if verbose:
            print(f"  Files extracted: {extracted_count}, Skipped (no data): {failed_count}")

        return new_df

    def sync_all_sequential(
        self,
        verbose: bool = True,
    ) -> dict:
        """
        Sync all years from 2007 to current year (sequential/single connection).

        Uses a single FTP connection - slower but safest option to avoid blocking.
        This is the main "cache update" function for sequential mode.

        Args:
            verbose: Whether to print progress messages

        Returns:
            Dictionary with sync statistics per year
        """
        if self.ftp is None or self.conn is None:
            raise ConnectionError("Not connected. Call connect() first.")

        # Set year range: 2007 to current year
        start_year = 2007
        end_year = datetime.now().year

        print(f"{'='*60}")
        print(f"ORATS Data Sync (Sequential): {start_year} - {end_year}")
        print(f"{'='*60}")

        # Get overview of what needs to be done
        total_missing = 0
        years_to_sync = []

        for year in range(start_year, end_year + 1):
            missing = self.get_missing_dates(year)
            if missing:
                years_to_sync.append((year, len(missing)))
                total_missing += len(missing)

        if not years_to_sync:
            print("\n✓ All years are up to date!")
            return {}

        print(f"\nYears with missing data:")
        for year, count in years_to_sync:
            print(f"  {year}: {count} dates missing")
        print(f"\nTotal: {total_missing} dates to download\n")

        # Sync each year
        stats = {}
        for year in range(start_year, end_year + 1):
            print(f"\n{'-'*40}")
            df = self.sync_year_sequential(
                year,
                verbose=verbose,
            )
            stats[year] = {
                "rows_added": len(df),
                "was_synced": len(df) > 0,
            }

        # Summary
        print(f"\n{'='*60}")
        print("SYNC COMPLETE")
        print(f"{'='*60}")

        total_rows = sum(s["rows_added"] for s in stats.values())
        years_updated = sum(1 for s in stats.values() if s["was_synced"])

        print(f"Years updated: {years_updated}")
        print(f"Total rows added: {total_rows:,}")

        return stats

    def sync_required_years_sequential(
        self,
        verbose: bool = True,
    ) -> dict:
        """
        Sync only years that require full download or the current year.

        Uses trading-day awareness to skip weekends/holidays and only download
        dates that are expected to have data. This skips years where all trading
        days are present and only downloads:
        1. Years with 0 existing dates (completely empty)
        2. The current year – only genuinely missing trading days

        Args:
            verbose: Whether to print progress messages

        Returns:
            Dictionary with sync statistics per year
        """
        if self.ftp is None or self.conn is None:
            raise ConnectionError("Not connected. Call connect() first.")

        start_year = 2007
        end_year = datetime.now().year
        current_year = datetime.now().year

        print(f"{'='*60}")
        print(f"ORATS Data Sync (Required Years Only): {start_year} - {end_year}")
        print(f"{'='*60}")

        # Identify years that need syncing
        years_to_sync = []
        skipped_years = []

        for year in range(start_year, end_year + 1):
            existing_count = len(self.get_existing_dates(year))

            # Use trading-day-aware missing dates (excludes weekends & holidays)
            missing_trading = self.get_missing_dates(year, trading_days_only=True)
            missing_trading_count = len(missing_trading)

            # Also get calendar-based missing for context display
            missing_calendar_count = len(self.get_missing_dates(year, trading_days_only=False))

            # Sync if: completely empty OR current year with genuinely missing trading days
            is_empty = (existing_count == 0 and missing_trading_count >= 200)
            is_current = (year == current_year and missing_trading_count > 0)

            if is_empty or is_current:
                reason = "current year" if year == current_year else "empty"
                years_to_sync.append((year, missing_trading_count, reason))
            elif missing_trading_count > 0:
                skipped_years.append((year, missing_trading_count, existing_count, missing_calendar_count))
            else:
                # Year is complete (0 missing trading days)
                if missing_calendar_count > 0:
                    skipped_years.append((year, 0, existing_count, missing_calendar_count))

        if skipped_years:
            print(f"\nSkipping complete years:")
            for entry in skipped_years:
                year, missing_td, existing, missing_cal = entry
                if missing_td == 0:
                    print(f"  {year}: {existing} trading dates present, "
                          f"{missing_cal} calendar gaps are weekends/holidays (OK)")
                else:
                    print(f"  {year}: {existing} dates exist, "
                          f"{missing_td} trading days missing, "
                          f"{missing_cal} total calendar gaps (OK)")

        if not years_to_sync:
            print("\n✓ All required years are up to date!")
            return {}

        print(f"\nYears to sync:")
        for year, count, reason in years_to_sync:
            print(f"  {year}: {count} trading days missing ({reason})")

        total_missing = sum(count for _, count, _ in years_to_sync)
        print(f"\nTotal: {total_missing} trading days to download\n")

        # Sync each required year
        stats = {}
        for year, _, _ in years_to_sync:
            print(f"\n{'-'*40}")
            df = self.sync_year_sequential(year, verbose=verbose)
            stats[year] = {
                "rows_added": len(df),
                "was_synced": len(df) > 0,
            }

        # Summary
        print(f"\n{'='*60}")
        print("SYNC COMPLETE")
        print(f"{'='*60}")

        total_rows = sum(s["rows_added"] for s in stats.values())
        years_updated = sum(1 for s in stats.values() if s["was_synced"])

        print(f"Years updated: {years_updated}")
        print(f"Total rows added: {total_rows:,}")

        return stats

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False


# Convenience function for quick usage
def extract_orats_data(
    years: Optional[List[int]] = None,
    limit_per_year: Optional[int] = None,
    **kwargs
) -> None:
    """
    Convenience function to extract ORATS data.

    Args:
        years: List of years to extract (defaults to all years)
        limit_per_year: Maximum days per year (None for all)
        **kwargs: Additional arguments passed to ExtractOratsEngine
    """
    with ExtractOratsEngine(**kwargs) as engine:
        if years:
            for year in years:
                engine.extract_year(year, limit=limit_per_year)
        else:
            engine.extract_all_years(limit_per_year=limit_per_year)


if __name__ == "__main__":
    # Example usage when run directly
    print("ORATS Data Extraction Engine")
    print("="*50)

    with ExtractOratsEngine() as engine:
        # Extract first 10 days of 2007 as a test
        df = engine.extract_year(2007, limit=10)
        print(f"\nSample data:\n{df.head()}")

        # Show tables
        print(f"\nTables in database: {engine.list_tables()}")
