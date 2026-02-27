"""IBKR TWS API wrapper for options data collection."""

import sys
import threading
import time
from datetime import datetime

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper

UNSET_DOUBLE = -sys.float_info.max


class OptionsScanner(EWrapper, EClient):
    """Connects to TWS and collects options data via streaming subscriptions."""

    def __init__(self):
        EClient.__init__(self, self)
        self._next_req_id = 1
        self._lock = threading.Lock()

        # Connection state
        self._connected = threading.Event()

        # Contract details: symbol -> conId
        self.con_ids: dict[str, int] = {}
        self._contract_events: dict[int, threading.Event] = {}
        self._conid_req_symbols: dict[int, str] = {}

        # Option chains: reqId -> chain data
        self._chains: dict[int, dict] = {}
        self._chain_events: dict[int, threading.Event] = {}

        # Option contract prevalidation: reqId -> contract details/event
        self._option_contract_events: dict[int, threading.Event] = {}
        self._option_contract_matches: dict[int, list[int]] = {}
        self._option_contract_errors: dict[int, tuple[int, str]] = {}
        self._validated_option_conids: dict[tuple[str, str, float, str], int] = {}

        # Market data: reqId -> dict of fields
        self._mkt_data: dict[int, dict] = {}
        # Signals when model IV computation arrives (the last piece of data)
        self._data_ready: dict[int, threading.Event] = {}
        # Option request errors: reqId -> (errorCode, errorString)
        self._option_req_errors: dict[int, tuple[int, str]] = {}

        # Underlying snapshot market data: reqId -> fields/event
        self._stock_mkt_data: dict[int, dict] = {}
        self._stock_events: dict[int, threading.Event] = {}
        self._stock_req_symbol: dict[int, str] = {}
        self._stock_req_errors: dict[int, tuple[int, str]] = {}

        # Historical stock close fallback: reqId -> close/event
        self._hist_stock_data: dict[int, dict] = {}
        self._hist_events: dict[int, threading.Event] = {}
        self._hist_req_symbol: dict[int, str] = {}
        self._hist_req_errors: dict[int, tuple[int, str]] = {}

        # Stock contract hints discovered from contract details
        self._primary_exchanges: dict[str, str] = {}

        # Mapping: reqId -> (symbol, expiry, strike, right)
        self._req_map: dict[int, tuple] = {}

        # Track reqIds that failed (e.g. no security definition)
        self._failed_reqs: set[int] = set()

        # Last requested market data type:
        # 1=live, 2=frozen, 3=delayed, 4=delayed-frozen
        self._market_data_type: int = 1

    def _get_req_id(self) -> int:
        with self._lock:
            rid = self._next_req_id
            self._next_req_id += 1
            return rid

    # ── Connection ────────────────────────────────────────────────────

    def nextValidId(self, orderId: int):
        self._next_order_id = orderId
        self._connected.set()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if reqId in self._data_ready:
            self._option_req_errors[reqId] = (errorCode, errorString)
        if reqId in self._option_contract_events:
            self._option_contract_errors[reqId] = (errorCode, errorString)
        if reqId in self._stock_events:
            self._stock_req_errors[reqId] = (errorCode, errorString)
        if reqId in self._hist_events:
            self._hist_req_errors[reqId] = (errorCode, errorString)

        # Ignore non-critical warnings
        if errorCode in (2104, 2106, 2158, 2119, 2176):  # data farm/version warnings
            return
        if errorCode == 200:  # No security definition found
            self._failed_reqs.add(reqId)
            if reqId in self._contract_events:
                print(f"  Warning: No contract found for reqId {reqId}: {errorString}")
                self._contract_events[reqId].set()
            if reqId in self._option_contract_events:
                self._option_contract_events[reqId].set()
            if reqId in self._chain_events:
                print(f"  Warning: No chain found for reqId {reqId}: {errorString}")
                self._chain_events[reqId].set()
            if reqId in self._data_ready:
                self._data_ready[reqId].set()
            if reqId in self._stock_events:
                symbol = self._stock_req_symbol.get(reqId, "?")
                print(f"  Warning: No stock contract/quote for {symbol} (reqId {reqId}): {errorString}")
                self._stock_events[reqId].set()
            if reqId in self._hist_events:
                symbol = self._hist_req_symbol.get(reqId, "?")
                print(f"  Warning: No historical contract data for {symbol} (reqId {reqId}): {errorString}")
                self._hist_events[reqId].set()
            return
        if errorCode == 354:  # No subscription for market data
            print(f"  Warning: No market data subscription for reqId {reqId}")
            if reqId in self._data_ready:
                self._data_ready[reqId].set()
            if reqId in self._stock_events:
                self._stock_events[reqId].set()
            return
        if errorCode in (10167, 10168):  # Live not subscribed / delayed available variants
            if reqId in self._stock_events:
                self._stock_events[reqId].set()
                return
        if errorCode == 300:  # Can't find EId (follow-up from failed requests)
            return
        if errorCode == 321:  # Error validating request
            if reqId in self._data_ready:
                self._data_ready[reqId].set()
            if reqId in self._stock_events:
                self._stock_events[reqId].set()
            if reqId in self._hist_events:
                self._hist_events[reqId].set()
            if reqId in self._option_contract_events:
                self._option_contract_events[reqId].set()
            return
        if errorCode == 162:  # Historical market data service error
            if reqId in self._hist_events:
                self._hist_events[reqId].set()
            return
        print(f"  Error {reqId}: [{errorCode}] {errorString}")

    def connectAndStart(self, host: str, port: int, client_id: int, timeout: float = 10):
        """Connect to TWS and start the message processing thread."""
        self.connect(host, port, client_id)
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        if not self._connected.wait(timeout=timeout):
            raise TimeoutError("Could not connect to TWS")
        print(f"Connected to TWS at {host}:{port}")

    def setMarketDataType(self, market_data_type: int, settle: float = 0.1):
        """Set IB market data mode and cache it locally."""
        self.reqMarketDataType(market_data_type)
        self._market_data_type = market_data_type
        if settle > 0:
            time.sleep(settle)

    # ── Contract Details ──────────────────────────────────────────────

    def startConIdRequest(self, symbol: str) -> int:
        """Send conId request (non-blocking). Returns reqId."""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        rid = self._get_req_id()
        self._contract_events[rid] = threading.Event()
        self._conid_req_symbols[rid] = symbol
        self.reqContractDetails(rid, contract)
        return rid

    def awaitConId(self, symbol: str, rid: int, timeout: float = 10) -> int | None:
        """Wait for a previously started conId request."""
        event = self._contract_events.get(rid)
        if event and not event.wait(timeout=timeout):
            print(f"  Timeout getting conId for {symbol}")
            return None
        return self.con_ids.get(symbol)

    def getConId(self, symbol: str, timeout: float = 10) -> int | None:
        """Look up the conId for a stock symbol (blocking convenience)."""
        rid = self.startConIdRequest(symbol)
        return self.awaitConId(symbol, rid, timeout)

    def _option_key(
        self, symbol: str, expiry: str, strike: float, right: str
    ) -> tuple[str, str, float, str]:
        return (symbol, expiry, round(float(strike), 8), right)

    def startOptionContractCheck(
        self,
        symbol: str,
        expiry: str,
        strike: float,
        right: str,
        trading_class: str = "",
        multiplier: str | None = "100",
    ) -> int:
        """Request contract details for one option contract candidate."""
        strike = round(float(strike), 4)
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = expiry
        contract.strike = float(strike)
        contract.right = right
        if multiplier:
            contract.multiplier = multiplier
        if trading_class:
            contract.tradingClass = trading_class

        rid = self._get_req_id()
        self._option_contract_events[rid] = threading.Event()
        self._option_contract_matches[rid] = []
        self._option_contract_errors.pop(rid, None)
        self.reqContractDetails(rid, contract)
        return rid

    def awaitOptionContractCheck(
        self,
        symbol: str,
        expiry: str,
        strike: float,
        right: str,
        rid: int,
        timeout: float = 2.5,
    ) -> int | None:
        """Wait for option contract details check and return conId if valid."""
        event = self._option_contract_events.get(rid)
        if event and not event.wait(timeout=timeout):
            self._option_contract_events.pop(rid, None)
            self._option_contract_matches.pop(rid, None)
            print(f"  Timeout prevalidating option contract {symbol} {expiry} {strike} {right}")
            return None

        matches = self._option_contract_matches.get(rid, [])
        self._option_contract_events.pop(rid, None)
        self._option_contract_matches.pop(rid, None)
        if matches:
            return matches[0]
        return None

    def prevalidateOptionContracts(
        self,
        symbol: str,
        expiries: list[str],
        strikes_per_expiry: dict[str, list[float]],
        trading_class: str = "",
        multiplier: str = "100",
        timeout_per_contract: float = 2.5,
        rate_limit: float = 0.0,
    ) -> tuple[dict[str, list[float]], dict]:
        """Keep only expiry/strike pairs with valid C and P contracts."""
        validated: dict[str, list[float]] = {}
        stats = {
            "expiries_in": len(expiries),
            "expiries_out": 0,
            "strikes_in": sum(len(strikes_per_expiry.get(e, [])) for e in expiries),
            "strikes_out": 0,
            "legs_checked": 0,
            "legs_cached": 0,
            "legs_valid": 0,
            "legs_invalid": 0,
            "invalid_code_counts": {},
        }

        for expiry in expiries:
            valid_strikes = []
            for strike in strikes_per_expiry.get(expiry, []):
                both_sides_valid = True
                for right in ("C", "P"):
                    key = self._option_key(symbol, expiry, strike, right)
                    if key in self._validated_option_conids:
                        stats["legs_cached"] += 1
                        continue

                    stats["legs_checked"] += 1
                    con_id = None
                    attempts = []
                    attempts.append((trading_class, multiplier))
                    if trading_class:
                        attempts.append(("", multiplier))
                    attempts.append(("", None))

                    seen = set()
                    for tc, mult in attempts:
                        sig = (tc or "", mult or "")
                        if sig in seen:
                            continue
                        seen.add(sig)

                        rid = self.startOptionContractCheck(
                            symbol,
                            expiry,
                            strike,
                            right,
                            trading_class=tc,
                            multiplier=mult,
                        )
                        con_id = self.awaitOptionContractCheck(
                            symbol,
                            expiry,
                            strike,
                            right,
                            rid,
                            timeout=timeout_per_contract,
                        )
                        if con_id is not None:
                            self._option_contract_errors.pop(rid, None)
                            break

                    if con_id is None:
                        stats["legs_invalid"] += 1
                        err = self._option_contract_errors.pop(rid, None)
                        if err:
                            code = err[0]
                            counts = stats["invalid_code_counts"]
                            counts[code] = counts.get(code, 0) + 1
                        both_sides_valid = False
                        break

                    self._validated_option_conids[key] = con_id
                    self._option_contract_errors.pop(rid, None)
                    stats["legs_valid"] += 1
                    if rate_limit > 0:
                        time.sleep(rate_limit)

                if both_sides_valid:
                    valid_strikes.append(strike)

            if valid_strikes:
                validated[expiry] = valid_strikes

        stats["expiries_out"] = len(validated)
        stats["strikes_out"] = sum(len(v) for v in validated.values())
        return validated, stats

    def contractDetails(self, reqId, contractDetails):
        if reqId in self._option_contract_matches:
            self._option_contract_matches[reqId].append(contractDetails.contract.conId)
            return

        symbol = self._conid_req_symbols.get(reqId, contractDetails.contract.symbol)
        self.con_ids[symbol] = contractDetails.contract.conId
        primary = contractDetails.contract.primaryExchange
        if primary:
            self._primary_exchanges[symbol] = primary

    def contractDetailsEnd(self, reqId):
        if reqId in self._contract_events:
            self._contract_events[reqId].set()
        if reqId in self._option_contract_events:
            self._option_contract_events[reqId].set()

    # ── Option Chains ─────────────────────────────────────────────────

    def startChainRequest(self, symbol: str, con_id: int) -> int:
        """Send option chain request (non-blocking). Returns reqId."""
        rid = self._get_req_id()
        self._chain_events[rid] = threading.Event()
        self._chains[rid] = {
            "tradingClass": "",
            "multiplier": "100",
            "expirations": set(),
            "strikes": set(),
        }
        self.reqSecDefOptParams(rid, symbol, "", "STK", con_id)
        return rid

    def awaitChain(self, symbol: str, rid: int, timeout: float = 10) -> dict | None:
        """Wait for a previously started chain request."""
        event = self._chain_events.get(rid)
        if event and not event.wait(timeout=timeout):
            print(f"  Timeout getting option chain for {symbol}")
            return None
        chain = self._chains.get(rid)
        if chain and chain["expirations"]:
            chain["expirations"] = sorted(chain["expirations"])
            chain["strikes"] = sorted(chain["strikes"])
            return chain
        return None

    def getOptionChain(self, symbol: str, con_id: int, timeout: float = 10) -> dict | None:
        """Get option chain (blocking convenience)."""
        rid = self.startChainRequest(symbol, con_id)
        return self.awaitChain(symbol, rid, timeout)

    def requestStockPrice(self, symbol: str, con_id: int | None = None) -> int:
        """Request one-shot snapshot market data for underlying stock."""
        contract = Contract()
        resolved_con_id = con_id if con_id is not None else self.con_ids.get(symbol)
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        if resolved_con_id is not None:
            contract.conId = resolved_con_id
        primary = self._primary_exchanges.get(symbol)
        if primary:
            contract.primaryExchange = primary

        rid = self._get_req_id()
        self._stock_mkt_data[rid] = {}
        self._stock_events[rid] = threading.Event()
        self._stock_req_symbol[rid] = symbol
        self._stock_req_errors.pop(rid, None)
        # Snapshot requests cannot use generic ticks (e.g. 221 mark) on some API versions.
        self.reqMktData(rid, contract, "", True, False, [])
        return rid

    def requestHistoricalClose(self, symbol: str, con_id: int | None = None) -> int:
        """Request recent daily bars and extract latest valid close."""
        contract = Contract()
        resolved_con_id = con_id if con_id is not None else self.con_ids.get(symbol)
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        if resolved_con_id is not None:
            contract.conId = resolved_con_id
        primary = self._primary_exchanges.get(symbol)
        if primary:
            contract.primaryExchange = primary

        rid = self._get_req_id()
        self._hist_stock_data[rid] = {}
        self._hist_events[rid] = threading.Event()
        self._hist_req_symbol[rid] = symbol
        self._hist_req_errors.pop(rid, None)

        self.reqHistoricalData(
            rid,
            contract,
            "",
            "5 D",
            "1 day",
            "TRADES",
            1,
            1,
            False,
            [],
        )
        return rid

    def _clean_price(self, value) -> float | None:
        """Return a normalized positive price value or None."""
        if value is None:
            return None
        if value <= UNSET_DOUBLE + 1:
            return None
        return value if value > 0 else None

    def _best_stock_price(self, rid: int) -> float | None:
        """Pick best available underlying price for a stock quote request."""
        d = self._stock_mkt_data.get(rid, {})
        bid = d.get("bid")
        ask = d.get("ask")

        # Prefer last trade, then mark, close, midpoint, then single-sided quote.
        price = d.get("last")
        if price is None:
            price = d.get("mark")
        if price is None:
            price = d.get("close")
        if price is None and bid is not None and ask is not None:
            price = (bid + ask) / 2
        if price is None:
            price = bid if bid is not None else ask
        return price

    def awaitStockPrice(self, symbol: str, rid: int, timeout: float = 15) -> float | None:
        """Wait for stock snapshot and return best-available underlying price."""
        event = self._stock_events.get(rid)
        deadline = time.time() + timeout

        # For snapshots, tickSnapshotEnd may arrive after several seconds.
        # Poll for any usable tick so we can return immediately once price is present.
        while time.time() < deadline:
            price = self._best_stock_price(rid)
            if price is not None:
                return price
            if event and event.is_set():
                break
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            if event:
                event.wait(timeout=min(0.25, remaining))
            else:
                time.sleep(min(0.25, remaining))

        price = self._best_stock_price(rid)
        if price is None:
            err = self._stock_req_errors.get(rid)
            if err:
                code, msg = err
                print(f"  Timeout getting stock price for {symbol} (last error [{code}] {msg})")
            else:
                print(f"  Timeout getting stock price for {symbol}")
        self.cancelMktData(rid)
        return price

    def getUnderlyingPrice(
        self, symbol: str, timeout: float = 15, con_id: int | None = None
    ) -> float | None:
        """Blocking convenience method to fetch underlying stock price."""
        rid = self.requestStockPrice(symbol, con_id=con_id)
        return self.awaitStockPrice(symbol, rid, timeout=timeout)

    def awaitHistoricalClose(self, symbol: str, rid: int, timeout: float = 12) -> float | None:
        """Wait for historical bar response and return latest close if available."""
        event = self._hist_events.get(rid)
        if event and not event.wait(timeout=timeout):
            err = self._hist_req_errors.get(rid)
            if err:
                code, msg = err
                print(f"  Timeout getting historical close for {symbol} (last error [{code}] {msg})")
            else:
                print(f"  Timeout getting historical close for {symbol}")
            return None

        close = self._hist_stock_data.get(rid, {}).get("close")
        if close is None:
            err = self._hist_req_errors.get(rid)
            if err:
                code, msg = err
                print(f"  No historical close for {symbol} (last error [{code}] {msg})")
            else:
                print(f"  No historical close for {symbol}")
        return close

    def getHistoricalClose(
        self, symbol: str, timeout: float = 12, con_id: int | None = None
    ) -> float | None:
        """Blocking convenience method to fetch latest daily close from historical bars."""
        rid = self.requestHistoricalClose(symbol, con_id=con_id)
        return self.awaitHistoricalClose(symbol, rid, timeout=timeout)

    def getUnderlyingPriceAuto(
        self,
        symbol: str,
        con_id: int | None = None,
        preferred_type: int = 1,
        fallback_types: tuple[int, ...] = (2, 4, 3),
        timeout_per_type: float = 8,
    ) -> tuple[float | None, int | None]:
        """Fetch underlying price with automatic market-data-type fallback."""
        candidates = []
        for mkt_type in (preferred_type, *fallback_types):
            if mkt_type not in candidates:
                candidates.append(mkt_type)

        for mkt_type in candidates:
            if self._market_data_type != mkt_type:
                self.setMarketDataType(mkt_type)
            price = self.getUnderlyingPrice(
                symbol,
                timeout=timeout_per_type,
                con_id=con_id,
            )
            if price is not None and price > 0:
                return price, mkt_type
        # Last fallback: historical daily close (works for market-closed use cases).
        close = self.getHistoricalClose(
            symbol,
            timeout=max(10, timeout_per_type),
            con_id=con_id,
        )
        if close is not None and close > 0:
            return close, 0
        return None, None

    def securityDefinitionOptionParameter(
        self, reqId, exchange, underlyingConId, tradingClass,
        multiplier, expirations, strikes,
    ):
        if reqId in self._chains and self._chains[reqId] is not None:
            self._chains[reqId]["expirations"].update(expirations)
            self._chains[reqId]["strikes"].update(strikes)
            if not self._chains[reqId]["tradingClass"]:
                self._chains[reqId]["tradingClass"] = tradingClass
                self._chains[reqId]["multiplier"] = multiplier

    def securityDefinitionOptionParameterEnd(self, reqId):
        if reqId in self._chain_events:
            self._chain_events[reqId].set()

    # ── Market Data (Streaming) ───────────────────────────────────────

    def requestOptionData(
        self, symbol: str, expiry: str, strike: float, right: str,
    ) -> int:
        """Subscribe to streaming market data for one option. Returns reqId."""
        contract = Contract()
        opt_key = self._option_key(symbol, expiry, strike, right)
        cached_conid = self._validated_option_conids.get(opt_key)
        # If prevalidated, identify by conId to avoid field-mismatch ambiguity.
        if cached_conid is not None:
            contract.conId = cached_conid
            contract.secType = "OPT"
            contract.exchange = "SMART"
            contract.currency = "USD"
        else:
            contract.symbol = symbol
            contract.secType = "OPT"
            contract.exchange = "SMART"
            contract.currency = "USD"
            contract.lastTradeDateOrContractMonth = expiry
            contract.strike = strike
            contract.right = right
            contract.multiplier = "100"

        rid = self._get_req_id()
        self._req_map[rid] = (symbol, expiry, strike, right)
        self._mkt_data[rid] = {}
        self._data_ready[rid] = threading.Event()
        self._option_req_errors.pop(rid, None)

        # Request IV plus option volume/open interest for downstream filtering.
        # 100=Option Volume, 101=Option Open Interest, 106=Option IV.
        self.reqMktData(rid, contract, "100,101,106", False, False, [])
        return rid

    def requestBatch(
        self,
        symbol: str,
        expiries: list[str],
        strikes_per_expiry: dict[str, list[float]],
        rate_limit: float = 0.03,
    ) -> list[int]:
        """Subscribe to streaming data for calls and puts across expiries/strikes."""
        req_ids = []
        for expiry in expiries:
            strikes = strikes_per_expiry.get(expiry, [])
            for strike in strikes:
                for right in ("C", "P"):
                    rid = self.requestOptionData(symbol, expiry, strike, right)
                    req_ids.append(rid)
                    time.sleep(rate_limit)
        return req_ids

    def waitForData(
        self, req_ids: list[int], timeout: float = 30, done_pct: float = 0.95,
    ):
        """Wait until done_pct of requests have data (or timeout)."""
        if not req_ids:
            return
        target = int(len(req_ids) * done_pct)
        deadline = time.time() + timeout
        while time.time() < deadline:
            done = sum(
                1 for rid in req_ids
                if rid in self._failed_reqs
                or (rid in self._data_ready and self._data_ready[rid].is_set())
            )
            if done >= target:
                return
            time.sleep(0.2)

    def summarizeOptionBatch(self, req_ids: list[int]) -> dict:
        """Summarize option batch readiness/data coverage."""
        total = len(req_ids)
        failed = sum(1 for rid in req_ids if rid in self._failed_reqs)
        eligible = total - failed
        ready = 0
        has_quote = 0
        has_any_iv = 0
        has_data = 0
        error_counts: dict[int, int] = {}

        for rid in req_ids:
            if rid in self._failed_reqs:
                continue
            evt = self._data_ready.get(rid)
            if evt and evt.is_set():
                ready += 1

            d = self._mkt_data.get(rid, {})
            quote = d.get("bid") is not None and d.get("ask") is not None
            iv = (
                d.get("bid_iv") is not None
                or d.get("ask_iv") is not None
                or d.get("model_iv") is not None
            )
            if quote:
                has_quote += 1
            if iv:
                has_any_iv += 1
            if quote or iv:
                has_data += 1
            err = self._option_req_errors.get(rid)
            if err:
                code = err[0]
                error_counts[code] = error_counts.get(code, 0) + 1

        denom = eligible if eligible > 0 else 1
        top_error_code = None
        top_error_count = 0
        if error_counts:
            top_error_code = max(error_counts.items(), key=lambda kv: kv[1])[0]
            top_error_count = error_counts[top_error_code]
        return {
            "total": total,
            "failed": failed,
            "eligible": eligible,
            "ready": ready,
            "has_quote": has_quote,
            "has_any_iv": has_any_iv,
            "has_data": has_data,
            "ready_pct": ready / denom if eligible > 0 else 1.0,
            "has_data_pct": has_data / denom if eligible > 0 else 1.0,
            "top_error_code": top_error_code,
            "top_error_count": top_error_count,
        }

    def cancelBatch(self, req_ids: list[int]):
        """Cancel all streaming market data subscriptions."""
        for rid in req_ids:
            if rid not in self._failed_reqs:
                self.cancelMktData(rid)

    def tickPrice(self, reqId, tickType, price, attrib):
        if reqId in self._stock_mkt_data:
            mapping = {
                1: "bid", 2: "ask", 4: "last", 9: "close", 37: "mark",
                66: "bid", 67: "ask", 68: "last", 75: "close",
            }
            key = mapping.get(tickType)
            if key:
                clean_price = self._clean_price(price)
                if clean_price is not None:
                    self._stock_mkt_data[reqId][key] = clean_price
                    # Don't wait for tickSnapshotEnd if we already have a usable price.
                    if reqId in self._stock_events:
                        self._stock_events[reqId].set()
            return

        if reqId not in self._mkt_data:
            return
        # Handle both live (1,2,4,9) and delayed (66,67,68,75) tick types
        mapping = {1: "bid", 2: "ask", 4: "last", 9: "close",
                   66: "bid", 67: "ask", 68: "last", 75: "close"}
        key = mapping.get(tickType)
        if key:
            self._mkt_data[reqId][key] = price

    def historicalData(self, reqId, bar):
        if reqId not in self._hist_stock_data:
            return
        close = self._clean_price(bar.close)
        if close is not None:
            self._hist_stock_data[reqId]["close"] = close

    def historicalDataEnd(self, reqId, start, end):
        if reqId in self._hist_events:
            self._hist_events[reqId].set()

    def tickSize(self, reqId, tickType, size):
        if reqId not in self._mkt_data:
            return
        if tickType in (8, 86):  # VOLUME or DELAYED_VOLUME
            self._mkt_data[reqId]["volume"] = size
        elif tickType in (27, 28):  # OPTION_CALL/PUT_OPEN_INTEREST
            self._mkt_data[reqId]["open_interest"] = size
        elif tickType in (29, 30):  # OPTION_CALL/PUT_VOLUME
            self._mkt_data[reqId]["option_volume"] = size

    def tickOptionComputation(
        self, reqId, tickType, tickAttrib,
        impliedVol, delta, optPrice, pvDividend,
        gamma, vega, theta, undPrice,
    ):
        if reqId not in self._mkt_data:
            return

        def _val(v):
            if v is None:
                return None
            return v if v > UNSET_DOUBLE + 1 else None

        # Handle both live (10-13) and delayed (53-56) computation ticks
        prefix_map = {
            10: "bid_", 11: "ask_", 12: "last_", 13: "model_",
            53: "bid_", 54: "ask_", 55: "last_", 56: "model_",
        }
        prefix = prefix_map.get(tickType)
        if prefix is None:
            return

        d = self._mkt_data[reqId]
        d[f"{prefix}iv"] = _val(impliedVol)
        d[f"{prefix}delta"] = _val(delta)
        d[f"{prefix}gamma"] = _val(gamma)
        d[f"{prefix}vega"] = _val(vega)
        d[f"{prefix}theta"] = _val(theta)
        d[f"{prefix}opt_price"] = _val(optPrice)
        d["und_price"] = _val(undPrice)

        # Signal readiness when model computation arrives
        if tickType in (13, 56):
            if reqId in self._data_ready:
                self._data_ready[reqId].set()

    def tickSnapshotEnd(self, reqId):
        if reqId in self._stock_events:
            self._stock_events[reqId].set()
        if reqId in self._data_ready:
            self._data_ready[reqId].set()

    def tickGeneric(self, reqId, tickType, value):
        pass

    def tickString(self, reqId, tickType, value):
        pass

    def marketDataType(self, reqId, marketDataType):
        pass

    # ── Data Assembly ─────────────────────────────────────────────────

    def collectResults(self, req_ids: list[int]) -> list[dict]:
        """Assemble collected data into a list of row dicts."""
        rows = []
        for rid in req_ids:
            meta = self._req_map.get(rid)
            data = self._mkt_data.get(rid, {})
            if meta is None:
                continue
            symbol, expiry, strike, right = meta
            row = {
                "symbol": symbol,
                "expiry": expiry,
                "strike": strike,
                "right": right,
                "stock_price": data.get("und_price"),
                "bid": data.get("bid"),
                "ask": data.get("ask"),
                "volume": data.get("option_volume", data.get("volume")),
                "open_interest": data.get("open_interest"),
                "bid_iv": data.get("bid_iv"),
                "ask_iv": data.get("ask_iv"),
                "mid_iv": None,
                "model_iv": data.get("model_iv"),
                "delta": data.get("model_delta") or data.get("bid_delta"),
                "gamma": data.get("model_gamma"),
                "vega": data.get("model_vega"),
                "theta": data.get("model_theta"),
            }
            # Compute mid IV from bid/ask IV
            if row["bid_iv"] is not None and row["ask_iv"] is not None:
                row["mid_iv"] = (row["bid_iv"] + row["ask_iv"]) / 2
            rows.append(row)
        return rows

    def selectStrikes(
        self,
        chain: dict,
        max_strikes: int = 4,
        underlying_price: float | None = None,
    ) -> tuple[float, dict[str, list[float]]]:
        """Select near-ATM strikes per expiry.

        Uses real underlying price as ATM anchor and picks the closest
        ``max_strikes`` strikes.
        Returns (anchor_price, strikes_per_expiry).
        """
        all_strikes = chain["strikes"]
        if not all_strikes:
            return 0.0, {}

        anchor_price = underlying_price
        if anchor_price is None or anchor_price <= 0:
            return 0.0, {}

        # Sort by distance to estimated price, keep closest
        by_dist = sorted(all_strikes, key=lambda s: abs(s - anchor_price))
        strikes = by_dist[:max_strikes]

        today = datetime.now().date()
        result = {}
        for exp_str in chain["expirations"]:
            exp_date = datetime.strptime(exp_str, "%Y%m%d").date()
            dte = (exp_date - today).days
            if dte < 7:  # skip very near-term
                continue
            result[exp_str] = strikes
        return anchor_price, result
