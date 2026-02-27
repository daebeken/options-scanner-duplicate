"""Run full options workflow end-to-end.

Steps:
1) Filter stocks -> save to data/stocks.csv
2) Run scanner (main.py)
3) Analyze scanned options (analyze_scanned_options.py)
4) Export QC contracts CSV and upload to Azure Blob (export_qc_contracts.py)
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

DEFAULTS = {
    "db": r"C:\Users\Mislav\qc_snp\data\all_stocks_daily",
    "stocks_csv": "data/stocks.csv",
    "top_n_dollar_volume": 1000,
    "analysis_output": "data/straddle_signal_table.csv",
    "container": "qc-backtest",
    "where": None,
    "minutes_in_future": 5,
}


def _run(cmd: list[str]) -> None:
    print(f"\n> {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def _load_symbols_target_from_config(config_path: str = "config.yaml") -> Path:
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    symbols_file = cfg.get("symbols_file", "symbols.txt")
    return Path(symbols_file)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run full options pipeline and upload QC contracts CSV.")
    parser.add_argument("--db", default=DEFAULTS["db"], help="Path to stock parquet DB for filter_symbols.")
    parser.add_argument(
        "--stocks-csv",
        default=DEFAULTS["stocks_csv"],
        help="Output file for filtered symbols (line-separated tickers).",
    )
    parser.add_argument(
        "--top-n-dollar-volume",
        type=int,
        default=DEFAULTS["top_n_dollar_volume"],
        help="Top N symbols by dollar volume after filters.",
    )
    parser.add_argument(
        "--analysis-output",
        default=DEFAULTS["analysis_output"],
        help="Output path for analyze_scanned_options table.",
    )
    parser.add_argument("--container", default=DEFAULTS["container"], help="Azure blob container name.")
    parser.add_argument("--endpoint", default=None, help="Azure blob endpoint URL (optional; env vars also work).")
    parser.add_argument("--key", default=None, help="Azure blob key (optional; env vars also work).")
    parser.add_argument(
        "--where",
        default=DEFAULTS["where"],
        help="Optional pandas query filter for export_qc_contracts.",
    )
    parser.add_argument(
        "--minutes-in-future",
        type=int,
        default=DEFAULTS["minutes_in_future"],
        help="Datetime offset used in exported QC CSV.",
    )
    parser.add_argument("--skip-filter", action="store_true", help="Skip filter_symbols step.")
    parser.add_argument("--skip-scan", action="store_true", help="Skip main.py scan step.")
    parser.add_argument("--skip-analyze", action="store_true", help="Skip analyze_scanned_options step.")
    parser.add_argument("--skip-export", action="store_true", help="Skip export_qc_contracts step.")
    args = parser.parse_args()

    py = sys.executable
    stocks_csv = Path(args.stocks_csv)
    stocks_csv.parent.mkdir(parents=True, exist_ok=True)

    # 1) Filter symbols
    if not args.skip_filter:
        cmd = [
            py,
            "filter_symbols.py",
            "--db",
            args.db,
            "--output",
            str(stocks_csv),
            "--top-n-dollar-volume",
            str(args.top_n_dollar_volume),
        ]
        _run(cmd)
    else:
        print("\nSkipping filter step.")

    # Sync filtered symbols file to scanner-config symbols path.
    symbols_target = _load_symbols_target_from_config()
    if stocks_csv.resolve() != symbols_target.resolve():
        symbols_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(stocks_csv, symbols_target)
        print(f"Copied filtered symbols to scanner input: {symbols_target}")

    # 2) Scan options
    if not args.skip_scan:
        _run([py, "main.py"])
    else:
        print("\nSkipping scan step.")

    # 3) Analyze scanned options
    if not args.skip_analyze:
        _run([py, "analyze_scanned_options.py", "--output", args.analysis_output])
    else:
        print("\nSkipping analysis step.")

    # 4) Export QC contracts + upload to Azure Blob
    if not args.skip_export:
        export_cmd = [
            py,
            "export_qc_contracts.py",
            "--input-csv",
            args.analysis_output,
            "--container",
            args.container,
            "--minutes-in-future",
            str(args.minutes_in_future),
        ]
        if args.where:
            export_cmd.extend(["--where", args.where])
        if args.endpoint:
            export_cmd.extend(["--endpoint", args.endpoint])
        if args.key:
            export_cmd.extend(["--key", args.key])
        _run(export_cmd)
    else:
        print("\nSkipping export step.")

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
