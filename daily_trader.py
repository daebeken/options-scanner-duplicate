# =============================================================================
# TODO: Decile 1 (short straddle) strategy is NOT implemented.
# My IBKR paper trading account does not allow me to go on margin, so I cannot
# short sell options. When margin is available, add a parallel short straddle
# flow for q_slope == 1: SELL call + SELL put at 9:30 AM, BUY call + BUY put
# at 3:59 PM to close.
# =============================================================================
#
# Updates (search for # [CHANGED] in the code):
#
# run_pipeline:
#   - Displays cOpra_ST/pOpra_ST (30d tenor contracts) instead of cOpra/pOpra (nearest expiry)
#
# open_straddles:
#   - Now buys cOpra_ST/pOpra_ST instead of cOpra/pOpra, so the traded contract
#     is aligned with the 30d IV signal (SLOPE, IVRV_SLOPE)
#
# close_straddles:
#   - Sells the same ST contracts that were opened
# =============================================================================
#
# Usage:
#   Production (full day schedule):  python daily_trader.py
#     - 9:20 AM ET: runs data pipeline (~5 min)
#     - 9:30 AM ET: opens long straddles (BUY call + put) for decile 10
#     - 3:59 PM ET: closes all straddles (SELL call + put)
#     - Saves results to data/daily_trades/trades_YYYYMMDD.csv
#
#   Test (immediate, no schedule):   python daily_trader.py --test
#     - Runs pipeline immediately, opens straddles, sleeps 30s, cancels/closes all
# tmux new -s trader -d 'cd /Users/arthurgoh/Desktop/algo-trading/options-scanner-duplicate && caffeinate -i python daily_trader.py'

import os
import time
import threading
import yaml
import polars as pl
from datetime import datetime, date
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order

from constant_iv_term_structure_engine import constantIVTermStructureEngine

# ── Configuration ─────────────────────────────────────────────────────────────
QUANTITY = 1                    # Number of contracts per leg (call + put)
CLIENT_ID = 2                   # Distinct from scanner's client_id=1
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7497             # Paper trading
TARGET_DECILE = 10              # q_slope decile to trade (long straddle)
LOG_DIR = "data/daily_trades"

ET = ZoneInfo("America/New_York")


# ── Utilities ─────────────────────────────────────────────────────────────────

def now_et():
    return datetime.now(ET)


def now_str():
    return now_et().strftime("%H:%M:%S ET")


def load_config(path="config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def parse_orats_symbol(opra: str) -> dict:
    """Parse ORATS cOpra/pOpra symbol into IBKR contract fields.

    Format: TICKER + YYMMDD + C/P + 8-digit strike (strike * 1000)
    Example: PLTR260313C00152500 -> PLTR, 20260313, C, 152.5
    """
    ticker = opra[:-15]
    date_str = opra[-15:-9]       # YYMMDD
    right = opra[-9]              # C or P
    strike_raw = opra[-8:]        # 8-digit strike * 1000

    expiry = f"20{date_str}"      # IBKR wants YYYYMMDD
    strike = int(strike_raw) / 1000.0

    return {
        "symbol": ticker,
        "expiry": expiry,
        "right": right,
        "strike": strike,
    }


def wait_until(hour, minute):
    """Sleep until a specific Eastern Time. Returns immediately if already past."""
    while True:
        current = now_et()
        target = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if current >= target:
            return
        delta = (target - current).total_seconds()
        print(f"[{now_str()}] Waiting {delta:.0f}s until {hour}:{minute:02d} ET...")
        time.sleep(min(delta, 60))


# ── TradingClient ─────────────────────────────────────────────────────────────

class TradingClient(EWrapper, EClient):
    """Persistent IBKR client for placing and managing option orders."""

    def __init__(self):
        EClient.__init__(self, self)
        self._next_order_id = None
        self._connected = threading.Event()
        self._lock = threading.Lock()

        # Per-order tracking
        self._order_events: dict[int, threading.Event] = {}
        self._order_fills: dict[int, dict] = {}

    def nextValidId(self, orderId):
        self._next_order_id = orderId
        self._connected.set()

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice,
                    permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        if orderId in self._order_fills:
            self._order_fills[orderId].update({
                "status": status,
                "filled": filled,
                "remaining": remaining,
                "avg_price": avgFillPrice,
            })
        print(f"  [{now_str()}] Order {orderId}: {status} | "
              f"filled={filled} remaining={remaining} avgPrice={avgFillPrice}")
        if status in ("Filled", "Cancelled", "Inactive"):
            if orderId in self._order_events:
                self._order_events[orderId].set()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode in (2104, 2106, 2158, 2119, 2176, 10349, 399):
            return
        print(f"  [{now_str()}] TWS Error {errorCode}: {errorString}")

    def get_next_order_id(self) -> int:
        with self._lock:
            oid = self._next_order_id
            self._next_order_id += 1
            return oid

    def connect_and_start(self, host, port, client_id, timeout=10):
        self.connect(host, port, client_id)
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        if not self._connected.wait(timeout=timeout):
            raise TimeoutError("Could not connect to TWS. Is it running?")

    def submit_option_order(self, opra: str, action: str, quantity: int) -> int:
        """Submit an option order without waiting. Returns the order ID."""
        parsed = parse_orats_symbol(opra)

        contract = Contract()
        contract.symbol = parsed["symbol"]
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = parsed["expiry"]
        contract.strike = parsed["strike"]
        contract.right = parsed["right"]
        contract.multiplier = "100"

        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = "MKT"
        order.eTradeOnly = ""
        order.firmQuoteOnly = ""

        oid = self.get_next_order_id()
        self._order_events[oid] = threading.Event()
        self._order_fills[oid] = {
            "opra": opra, "action": action, "status": "Pending",
            "filled": 0, "remaining": quantity, "avg_price": 0.0,
        }

        print(f"  [{now_str()}] Submitting {action} MKT #{oid}: {quantity}x "
              f"{parsed['symbol']} {parsed['expiry']} {parsed['strike']} {parsed['right']}")
        self.placeOrder(oid, contract, order)
        return oid

    def wait_for_orders(self, order_ids: list[int], timeout: int = 30):
        """Wait for all submitted orders to reach a terminal state."""
        for oid in order_ids:
            if oid in self._order_events:
                self._order_events[oid].wait(timeout=timeout)

    def get_order_result(self, oid: int) -> dict:
        """Get the fill result for a submitted order."""
        result = self._order_fills[oid].copy()
        result["order_id"] = oid
        return result

    def cancel_all_orders(self):
        """Cancel all open (non-filled) orders."""
        for oid, fill in self._order_fills.items():
            if fill["status"] not in ("Filled", "Cancelled", "Inactive"):
                print(f"  [{now_str()}] Cancelling order #{oid} ({fill['opra']})")
                self.cancelOrder(oid, "")


# ── DailyTrader ───────────────────────────────────────────────────────────────

class DailyTrader:
    """Orchestrates the daily long straddle trading strategy."""

    def __init__(self, host, port, client_id, quantity, log_dir):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.quantity = quantity
        self.log_dir = log_dir

        self.client: TradingClient | None = None
        self.decile10_df: pl.DataFrame | None = None
        self.open_positions: list[dict] = []

    def run_pipeline(self):
        """Run the constantIVTermStructureEngine pipeline and filter for decile 10."""
        print(f"[{now_str()}] Loading tickers from symbols.txt...")
        tickers = open("symbols.txt").read().splitlines()
        print(f"[{now_str()}] Running engine for {len(tickers)} tickers...")

        auto_sync = os.getenv("AUTO_SYNC", "true").lower() == "true"
        env_date = os.getenv("AS_OF_DATE", "").strip()
        as_of_date = date.fromisoformat(env_date) if env_date else None
        engine = constantIVTermStructureEngine(ticker=tickers, auto_sync=auto_sync, as_of_date=as_of_date)
        result = engine.run()

        # Filter for latest trade_date, decile 10 only
        self.decile10_df = result.filter(
            (pl.col("trade_date") == pl.col("trade_date").max()) &
            (pl.col("q_slope") == TARGET_DECILE)
        )

        n = len(self.decile10_df)
        print(f"[{now_str()}] Pipeline complete. {n} decile-10 straddles to trade:")
        for row in self.decile10_df.iter_rows(named=True):
            print(f"  {row['ticker']}: call={row['cOpra_ST']}  put={row['pOpra_ST']}")  # [CHANGED] display ST contracts instead of nearest-expiry

    def connect_tws(self):
        """Establish persistent TWS connection."""
        self.client = TradingClient()
        self.client.connect_and_start(self.host, self.port, self.client_id)
        print(f"[{now_str()}] Connected to TWS at {self.host}:{self.port}")

    def open_straddles(self):
        """BUY ATM call + ATM put for each decile-10 symbol (all at once)."""
        if self.decile10_df is None or len(self.decile10_df) == 0:
            print(f"[{now_str()}] No decile-10 straddles to open.")
            return

        # Submit all orders first
        pending = []  # (ticker, cOpra, pOpra, call_oid, put_oid)
        all_oids = []

        for row in self.decile10_df.iter_rows(named=True):
            ticker = row["ticker"]
            c_opra = row["cOpra_ST"]  # [CHANGED] trade ST call contract (aligned with 30d IV signal)
            p_opra = row["pOpra_ST"]  # [CHANGED] trade ST put contract (aligned with 30d IV signal)

            call_oid = self.client.submit_option_order(c_opra, "BUY", self.quantity)
            put_oid = self.client.submit_option_order(p_opra, "BUY", self.quantity)

            pending.append((ticker, c_opra, p_opra, call_oid, put_oid))
            all_oids.extend([call_oid, put_oid])

        print(f"\n[{now_str()}] Submitted {len(all_oids)} orders. Waiting for fills...")
        self.client.wait_for_orders(all_oids)

        # Collect results
        for ticker, c_opra, p_opra, call_oid, put_oid in pending:
            call_result = self.client.get_order_result(call_oid)
            put_result = self.client.get_order_result(put_oid)

            position = {
                "ticker": ticker,
                "cOpra": c_opra,
                "pOpra": p_opra,
                "call_buy": call_result,
                "put_buy": put_result,
                "call_sell": None,
                "put_sell": None,
            }
            self.open_positions.append(position)

            if call_result["status"] == "Filled" and put_result["status"] == "Filled":
                print(f"  {ticker} OPENED: call@{call_result['avg_price']:.2f} "
                      f"+ put@{put_result['avg_price']:.2f}")
            else:
                print(f"  WARNING: {ticker} not fully filled — "
                      f"call={call_result['status']}, put={put_result['status']}")

    def close_straddles(self):
        """SELL all open straddle positions (all at once)."""
        if not self.open_positions:
            print(f"[{now_str()}] No open positions to close.")
            return

        # Submit all sell orders first
        sell_tasks = []  # (pos, "call"|"put", oid)
        all_oids = []

        for pos in self.open_positions:
            if pos["call_buy"]["status"] == "Filled":
                oid = self.client.submit_option_order(pos["cOpra"], "SELL", self.quantity)  # [CHANGED] cOpra now stores cOpra_ST
                sell_tasks.append((pos, "call", oid))
                all_oids.append(oid)

            if pos["put_buy"]["status"] == "Filled":
                oid = self.client.submit_option_order(pos["pOpra"], "SELL", self.quantity)  # [CHANGED] pOpra now stores pOpra_ST
                sell_tasks.append((pos, "put", oid))
                all_oids.append(oid)

        print(f"\n[{now_str()}] Submitted {len(all_oids)} sell orders. Waiting for fills...")
        self.client.wait_for_orders(all_oids)

        # Collect results
        for pos, leg, oid in sell_tasks:
            result = self.client.get_order_result(oid)
            pos[f"{leg}_sell"] = result
            print(f"  {pos['ticker']} {leg.upper()} SELL: {result['status']} @ {result['avg_price']:.2f}")

    def cancel_and_close_all(self):
        """Cancel unfilled orders, then close all filled positions."""
        print(f"\n[{now_str()}] === Cancelling unfilled orders ===")
        if self.client:
            self.client.cancel_all_orders()
            time.sleep(2)  # let cancellations propagate

        print(f"\n[{now_str()}] === Closing filled positions ===")
        self.close_straddles()

    def compute_returns(self) -> list[dict]:
        """Compute P&L for each straddle and print summary."""
        results = []
        total_pnl = 0.0

        for pos in self.open_positions:
            entry_cost = 0.0
            exit_value = 0.0

            if pos["call_buy"] and pos["call_buy"]["status"] == "Filled":
                entry_cost += pos["call_buy"]["avg_price"]
            if pos["call_sell"] and pos["call_sell"]["status"] == "Filled":
                exit_value += pos["call_sell"]["avg_price"]

            if pos["put_buy"] and pos["put_buy"]["status"] == "Filled":
                entry_cost += pos["put_buy"]["avg_price"]
            if pos["put_sell"] and pos["put_sell"]["status"] == "Filled":
                exit_value += pos["put_sell"]["avg_price"]

            ret = (exit_value / entry_cost - 1) if entry_cost > 0 else None
            dollar_pnl = (exit_value - entry_cost) * 100 * self.quantity

            result = {
                "date": date.today().isoformat(),
                "ticker": pos["ticker"],
                "cOpra": pos["cOpra"],
                "pOpra": pos["pOpra"],
                "call_entry": pos["call_buy"]["avg_price"] if pos["call_buy"] else None,
                "put_entry": pos["put_buy"]["avg_price"] if pos["put_buy"] else None,
                "call_exit": pos["call_sell"]["avg_price"] if pos["call_sell"] else None,
                "put_exit": pos["put_sell"]["avg_price"] if pos["put_sell"] else None,
                "entry_cost": entry_cost,
                "exit_value": exit_value,
                "return_pct": ret,
                "dollar_pnl": dollar_pnl,
            }
            results.append(result)
            total_pnl += dollar_pnl

            ret_str = f"{ret * 100:.2f}%" if ret is not None else "N/A"
            print(f"  {pos['ticker']}: entry={entry_cost:.2f} exit={exit_value:.2f} "
                  f"ret={ret_str} PnL=${dollar_pnl:.2f}")

        print(f"\n{'=' * 60}")
        print(f"DAILY TOTAL P&L: ${total_pnl:.2f}")
        print(f"{'=' * 60}")

        return results

    def save_trade_log(self, results: list[dict]):
        """Save trade results to CSV."""
        if not results:
            print(f"[{now_str()}] No trades to log.")
            return
        os.makedirs(self.log_dir, exist_ok=True)
        filepath = os.path.join(self.log_dir, f"trades_{date.today().strftime('%Y%m%d')}.csv")
        pl.DataFrame(results).write_csv(filepath)
        print(f"[{now_str()}] Trade log saved to {filepath}")

    def disconnect(self):
        if self.client:
            self.client.disconnect()
            print(f"[{now_str()}] Disconnected from TWS.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    config = load_config()
    tws = config.get("tws", {})
    host = tws.get("host", DEFAULT_HOST)
    port = tws.get("port", DEFAULT_PORT)

    trader = DailyTrader(
        host=host, port=port, client_id=CLIENT_ID,
        quantity=QUANTITY, log_dir=LOG_DIR,
    )

    today = now_et().strftime("%Y-%m-%d (%A)")
    print(f"{'=' * 60}")
    print(f"DAILY TRADER — {today}")
    print(f"Strategy: Long straddle on decile {TARGET_DECILE} (q_slope)")
    print(f"Quantity: {QUANTITY} contract(s) per leg")
    print(f"TWS: {host}:{port} (client_id={CLIENT_ID})")
    print(f"{'=' * 60}")

    # Phase 1: 9:15 AM — Run pipeline
    wait_until(9, 15)
    print(f"\n[{now_str()}] === PHASE 1: Running data pipeline ===")
    trader.run_pipeline()

    # Phase 2: Connect to TWS
    print(f"\n[{now_str()}] === PHASE 2: Connecting to TWS ===")
    trader.connect_tws()

    # Phase 3: 9:30 AM — Open straddles
    wait_until(9, 30)
    print(f"\n[{now_str()}] === PHASE 3: Opening straddles ===")
    trader.open_straddles()

    # Phase 4: 3:59 PM — Close straddles
    wait_until(15, 59)
    print(f"\n[{now_str()}] === PHASE 4: Closing straddles ===")
    trader.close_straddles()

    # Phase 5: Compute returns
    print(f"\n[{now_str()}] === PHASE 5: Computing returns ===")
    results = trader.compute_returns()
    trader.save_trade_log(results)

    trader.disconnect()
    print(f"\n[{now_str()}] Daily trading session complete.")


def main_test():
    """Test mode: run pipeline, open straddles, wait 30s, cancel/close all."""
    config = load_config()
    tws = config.get("tws", {})
    host = tws.get("host", DEFAULT_HOST)
    port = tws.get("port", DEFAULT_PORT)

    trader = DailyTrader(
        host=host, port=port, client_id=CLIENT_ID,
        quantity=QUANTITY, log_dir=LOG_DIR,
    )

    today = now_et().strftime("%Y-%m-%d (%A)")
    print(f"{'=' * 60}")
    print(f"DAILY TRADER — TEST MODE — {today}")
    print(f"Strategy: Long straddle on decile {TARGET_DECILE} (q_slope)")
    print(f"Quantity: {QUANTITY} contract(s) per leg")
    print(f"TWS: {host}:{port} (client_id={CLIENT_ID})")
    print(f"{'=' * 60}")

    # Step 1: Run pipeline (no wait)
    print(f"\n[{now_str()}] === STEP 1: Running data pipeline ===")
    trader.run_pipeline()

    # Step 2: Connect to TWS
    print(f"\n[{now_str()}] === STEP 2: Connecting to TWS ===")
    trader.connect_tws()

    # Step 3: Open straddles immediately
    print(f"\n[{now_str()}] === STEP 3: Opening straddles ===")
    trader.open_straddles()

    # Step 4: Sleep 30s for visual verification
    print(f"\n[{now_str()}] === STEP 4: Sleeping 30s — check TWS Activity Monitor ===")
    time.sleep(30)

    # Step 5: Cancel unfilled orders and close all filled positions
    print(f"\n[{now_str()}] === STEP 5: Cancelling & closing all positions ===")
    trader.cancel_and_close_all()

    # Step 6: Summary
    print(f"\n[{now_str()}] === STEP 6: Summary ===")
    results = trader.compute_returns()
    trader.save_trade_log(results)

    trader.disconnect()
    print(f"\n[{now_str()}] Test complete.")


if __name__ == "__main__":
    import sys
    if "--test" in sys.argv:
        try:
            main_test()
        except KeyboardInterrupt:
            print("\n[INTERRUPTED] Shutting down...")
        except Exception as e:
            print(f"\n[FATAL ERROR] {e}")
            import traceback
            traceback.print_exc()
    else:
        try:
            main()
        except KeyboardInterrupt:
            print("\n[INTERRUPTED] Shutting down...")
        except Exception as e:
            print(f"\n[FATAL ERROR] {e}")
            import traceback
            traceback.print_exc()
