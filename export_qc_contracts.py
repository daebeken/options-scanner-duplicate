"""Build QC option contract weights from analyzed signal table and upload to Azure Blob.

Input: analyze_scanned_options output (e.g. data/straddle_signal_table.csv)
Output CSV columns:
  - date: timezone-aware datetime set slightly in the future
  - symbol: OCC-like option contract id, e.g. "NVDA 260320C00195000"
  - weight: equal weights across selected contracts (short side gets negative sign)
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import pandas as pd

DEFAULTS = {
    "input_csv": "data/straddle_signal_table.csv",
    "output_dir": "data",
    "container": "qc-backtest",
    "env_file": ".env",
    "time_zone": "America/New_York",
    "minutes_in_future": 5,
    "include_sides": "long_straddle,short_straddle",
    "where": None,
}


def _occ_contract_id(symbol: str, expiry_yyyymmdd: str, right: str, strike: float) -> str:
    exp = datetime.strptime(str(expiry_yyyymmdd), "%Y%m%d").strftime("%y%m%d")
    strike_int = int(round(float(strike) * 1000))
    return f"{symbol} {exp}{right}{strike_int:08d}"


def _parse_include_sides(value: str) -> set[str]:
    return {s.strip() for s in value.split(",") if s.strip()}


def _build_qc_contract_table(
    input_path: str,
    tz_name: str,
    minutes_in_future: int,
    include_sides: set[str],
    where_query: str | None = None,
) -> pd.DataFrame:
    df = pd.read_csv(input_path)
    required = {"symbol", "expiry", "strike", "side"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in input table: {missing}")

    out = df[df["side"].isin(include_sides)].copy()
    if where_query:
        out = out.query(where_query, engine="python")
    if out.empty:
        return pd.DataFrame(columns=["date", "symbol", "weight"])

    now = datetime.now(ZoneInfo(tz_name))
    dt = (now + timedelta(minutes=minutes_in_future)).replace(microsecond=0)
    dt_text = dt.isoformat(sep=" ")

    rows: list[dict] = []
    for _, row in out.iterrows():
        side = str(row["side"])
        weight_sign = 1.0 if side == "long_straddle" else -1.0
        sym = str(row["symbol"]).strip().upper()
        exp = str(row["expiry"]).strip()
        strike = float(row["strike"])

        rows.append(
            {
                "date": dt_text,
                "symbol": _occ_contract_id(sym, exp, "C", strike),
                "weight_sign": weight_sign,
            }
        )
        rows.append(
            {
                "date": dt_text,
                "symbol": _occ_contract_id(sym, exp, "P", strike),
                "weight_sign": weight_sign,
            }
        )

    qc = pd.DataFrame(rows)
    if qc.empty:
        return pd.DataFrame(columns=["date", "symbol", "weight"])

    abs_equal_weight = 1.0 / len(qc)
    qc["weight"] = qc["weight_sign"] * abs_equal_weight
    qc = qc.drop(columns=["weight_sign"])
    return qc


def _env_or(value: str | None, *keys: str) -> str | None:
    if value:
        return value
    for k in keys:
        v = os.getenv(k)
        if v:
            return v
    return None


def _load_dotenv(path: str) -> None:
    """Load KEY=VALUE pairs from .env file if present."""
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value and ((value[0] == value[-1]) and value[0] in ("'", '"')):
            value = value[1:-1]

        # Do not override vars already present in environment.
        if key not in os.environ:
            os.environ[key] = value


def _account_name_from_endpoint(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    host = parsed.netloc or parsed.path
    host = host.split("/")[0]
    if not host:
        raise ValueError("Invalid blob endpoint.")
    return host.split(".")[0]


def _upload_to_blob(local_path: Path, endpoint: str, key: str, container: str, blob_name: str) -> str:
    try:
        from azure.core.credentials import AzureNamedKeyCredential
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency azure-storage-blob. Install with: pip install azure-storage-blob"
        ) from exc

    account_name = _account_name_from_endpoint(endpoint)
    credential = AzureNamedKeyCredential(account_name, key)
    service = BlobServiceClient(account_url=endpoint, credential=credential)
    container_client = service.get_container_client(container)
    try:
        container_client.create_container()
    except Exception:
        # Ignore if already exists or creation not permitted.
        pass

    blob_client = container_client.get_blob_client(blob_name)
    with local_path.open("rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    return f"{endpoint.rstrip('/')}/{container}/{blob_name}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export QC contracts CSV from analyze_scanned_options output and upload to Azure Blob.",
    )
    parser.add_argument("--input-csv", default=DEFAULTS["input_csv"], help="Input analyzed signal CSV.")
    parser.add_argument("--output-dir", default=DEFAULTS["output_dir"], help="Local output directory for CSV.")
    parser.add_argument("--container", default=DEFAULTS["container"], help="Azure blob container name.")
    parser.add_argument("--blob-name", default=None, help="Blob filename. Default is auto-generated.")
    parser.add_argument("--endpoint", default=None, help="Azure blob endpoint URL.")
    parser.add_argument("--key", default=None, help="Azure blob account key.")
    parser.add_argument(
        "--env-file",
        default=DEFAULTS["env_file"],
        help="Path to .env file (default: .env).",
    )
    parser.add_argument("--time-zone", default=DEFAULTS["time_zone"], help="Timezone for date column.")
    parser.add_argument(
        "--minutes-in-future",
        type=int,
        default=DEFAULTS["minutes_in_future"],
        help="Set date column this many minutes in the future.",
    )
    parser.add_argument(
        "--include-sides",
        default=DEFAULTS["include_sides"],
        help="Comma-separated sides to include (e.g. long_straddle,short_straddle).",
    )
    parser.add_argument(
        "--where",
        default=DEFAULTS["where"],
        help="Optional pandas query filter against the analyzed table.",
    )
    args = parser.parse_args()

    _load_dotenv(args.env_file)

    include_sides = _parse_include_sides(args.include_sides)
    qc = _build_qc_contract_table(
        input_path=args.input_csv,
        tz_name=args.time_zone,
        minutes_in_future=args.minutes_in_future,
        include_sides=include_sides,
        where_query=args.where,
    )

    if qc.empty:
        raise RuntimeError("No contracts selected after side/filter conditions.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    blob_name = args.blob_name or f"qc_contracts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    local_csv = output_dir / blob_name
    qc.to_csv(local_csv, index=False)

    endpoint = _env_or(args.endpoint, "BLOB-ENDPOINT-SNP", "BLOB_ENDPOINT_SNP")
    key = _env_or(args.key, "BLOB-KEY-SNP", "BLOB_KEY_SNP")
    if not endpoint or not key:
        raise RuntimeError(
            "Missing Azure credentials. Provide --endpoint/--key or env vars "
            "BLOB-ENDPOINT-SNP/BLOB-KEY-SNP (or underscore variants)."
        )

    blob_url = _upload_to_blob(local_csv, endpoint=endpoint, key=key, container=args.container, blob_name=blob_name)

    print(f"Created local CSV: {local_csv}")
    print(f"Rows: {len(qc)}")
    print(f"Uploaded blob: {blob_url}")


if __name__ == "__main__":
    main()
