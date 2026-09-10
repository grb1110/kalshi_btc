"""
Live Kalshi BTC 15m orderbook collector.

Difference from the original recording collector
------------------------------------------------
The collector you wrote previously writes every orderbook update to CSV
segment files for OFFLINE analysis. This one is for LIVE trading: it
maintains the orderbook state in memory and exposes a thread-safe
`get_snapshot()` method that returns the dict your strategy needs.

Same websocket protocol, same auth, same snapshot/delta reconstruction
logic — just no disk I/O on the hot path.

Snapshot shape returned to strategy
-----------------------------------
    {
        "best_yes_bid":   float,   # best YES bid  (price to SELL YES)  — COLLECTED
        "best_yes_ask":   float,   # best YES ask  (price to BUY  YES)  — DERIVED: 1 - best_no_bid
        "best_no_bid":    float,   # best NO  bid  (price to SELL NO)   — COLLECTED
        "best_no_ask":    float,   # best NO  ask  (price to BUY  NO)   — DERIVED: 1 - best_yes_bid
        "yes_depth_3":    float,   # sum of top-3 YES bid sizes
        "no_depth_3":     float,   # sum of top-3 NO  bid sizes
        "time_elapsed":   float,   # seconds since window_start
        "coinbase_price": float,   # latest BTC-USD trade price (0.0 until first tick)
        "s0_price":       float,   # window threshold (static), from market.threshold
        # ── extra fields included for free, ignore if not needed ──
        "secs_to_close":  float,
        "spread_yes":     float,
        "ts_ms":          int,
        "ready":          bool,    # False until BOTH Kalshi snapshot AND
                                   # first Coinbase tick have arrived
    }

Note on the "ask" derivation
----------------------------
Kalshi's WS only streams BIDS on both sides, so the two bids are read
straight from orderbook state while both asks are DERIVED (never
collected) via the reciprocal relationship:
    YES_ask = 1.00 - NO_bid
    NO_ask  = 1.00 - YES_bid
(because buying YES at price p is economically equivalent to selling NO
at price 1-p, so the YES ask must equal 1 minus the best NO bid, and
symmetrically for the NO ask).
This matches what your original collector does in `_orderbook_summary`.

Usage
-----
    from kalshi_live_data_collector import KalshiLiveCollector

    collector = KalshiLiveCollector(
        market_info  = market,
        window_start = window_start,
        window_close = window_close,
        key_id       = key_id,
        private_key  = private_key,
    )
    collector.start()

    while time.time() < window_close.timestamp():
        snap = collector.get_snapshot()
        if snap["ready"]:
            executor.try_trade(snap)
        time.sleep(0.05)   # 20 Hz polling — adjust to taste

    collector.stop()

Or, if you prefer event-driven instead of polling, pass an `on_update`
callback and the collector will fire it on every orderbook change.
"""

# ── Imports ───────────────────────────────────────────────────────────────────

import json
import time
import threading
import websocket
from datetime import datetime, timezone
from typing import Optional, Callable, Dict, Any

# Reuse the auth helper from your existing collector. If you have it in a
# different module, change this import path.
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
import base64


# ── Constants ────────────────────────────────────────────────────────────────

KALSHI_WS_URL   = "wss://api.elections.kalshi.com/trade-api/ws/v2"
COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"


# ── Auth (duplicated here so this file is self-contained) ────────────────────

def _build_ws_auth_headers(key_id, private_key):
    ts_ms     = str(int(time.time() * 1000))
    path      = "/trade-api/ws/v2"
    msg_bytes = (ts_ms + "GET" + path).encode("utf-8")
    signature = private_key.sign(
        msg_bytes,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       key_id,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "Content-Type":            "application/json",
    }


# ── Live collector ───────────────────────────────────────────────────────────

class KalshiLiveCollector:
    """
    One instance per market (per 15-minute window). In-memory orderbook
    only — no CSVs, no segment files.

    Thread safety
    -------------
    The orderbook state is mutated by the websocket thread and read by
    the strategy thread. All reads/writes are guarded by `self._lock`.
    `get_snapshot()` returns a fresh dict so the caller never holds a
    reference to internal state.
    """

    def __init__(self, market_info, window_start, window_close,
                 key_id, private_key,
                 on_update: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.market_info  = market_info
        self.ticker       = market_info["ticker"]
        self.threshold    = market_info.get("threshold")
        self.window_start = window_start
        self.window_close = window_close
        self.key_id       = key_id
        self.private_key  = private_key
        self.on_update    = on_update

        # Orderbook state — guarded by _lock
        # Keys are price strings (preserve the exact string Kalshi sends);
        # values are floats (current size at that level).
        self.yes_bids: Dict[str, float] = {}
        self.no_bids:  Dict[str, float] = {}
        self._lock = threading.Lock()

        # Coinbase BTC-USD live price. Read by get_snapshot, written by
        # Coinbase WS thread. Atomic single-value assignment on a float in
        # CPython means a separate lock isn't strictly required, but we
        # use the same _lock for clarity and future-proofing.
        self.coinbase_price: float = 0.0
        self._got_coinbase_tick    = False
        self.coinbase_count        = 0

        # State flags
        self._got_snapshot = False
        self.running       = False
        self.update_count  = 0

        # WS objects — Kalshi
        self._ws            = None
        self._closed_event  = threading.Event()
        self._ws_thread     = None

        # WS objects — Coinbase
        self._cb_ws            = None
        self._cb_closed_event  = threading.Event()
        self._cb_ws_thread     = None

    # ── Public read API (called from strategy thread) ────────────────────────

    def get_snapshot(self) -> Dict[str, Any]:
        """
        Returns the current snapshot dict. Always returns a dict — check
        `snap["ready"]` before acting on the numeric fields, because they
        will be 0.0 until the initial Kalshi snapshot AND the first
        Coinbase tick have arrived.

        This is the hot-path method. It must stay fast: O(k) where k is
        the number of price levels (typically <50 on Kalshi).
        """
        now = datetime.now(timezone.utc)
        ts_ms = int(now.timestamp() * 1000)
        time_elapsed  = (now - self.window_start).total_seconds()
        secs_to_close = (self.window_close - now).total_seconds()
        # s0_price is static for the window — pull from market_info, not lock-guarded.
        s0_price = float(self.threshold) if self.threshold is not None else 0.0

        with self._lock:
            kalshi_ready = (
                self._got_snapshot and bool(self.yes_bids) and bool(self.no_bids)
            )
            cb_ready     = self._got_coinbase_tick
            cb_price     = self.coinbase_price

            if not (kalshi_ready and cb_ready):
                return {
                    "best_yes_bid":   0.0,
                    "best_yes_ask":   0.0,
                    "best_no_bid":    0.0,
                    "best_no_ask":    0.0,
                    "yes_depth_3":    0.0,
                    "no_depth_3":     0.0,
                    "time_elapsed":   time_elapsed,
                    "coinbase_price": cb_price,    # may be 0.0 if no tick yet
                    "s0_price":       s0_price,
                    "secs_to_close":  secs_to_close,
                    "spread_yes":     0.0,
                    "ts_ms":          ts_ms,
                    "ready":          False,
                }

            # Sort once. Lists are short (<50 levels), so this is cheap.
            yes_sorted = sorted(
                self.yes_bids.items(), key=lambda x: float(x[0]), reverse=True
            )
            no_sorted = sorted(
                self.no_bids.items(),  key=lambda x: float(x[0]), reverse=True
            )

            best_yes_bid = float(yes_sorted[0][0])
            best_no_bid  = float(no_sorted[0][0])

            # Asks are DERIVED from the opposite side's bid (never collected):
            #   YES ask = 1 - best NO bid ,  NO ask = 1 - best YES bid
            best_yes_ask = round(1.0 - best_no_bid, 4)
            best_no_ask  = round(1.0 - best_yes_bid, 4)

            yes_depth_3 = round(sum(v for _, v in yes_sorted[:3]), 4)
            no_depth_3  = round(sum(v for _, v in no_sorted[:3]),  4)
            spread_yes  = round(best_yes_ask - best_yes_bid, 4)

        return {
            "best_yes_bid":   round(best_yes_bid, 4),
            "best_yes_ask":   best_yes_ask,
            "best_no_bid":    round(best_no_bid, 4),
            "best_no_ask":    best_no_ask,
            "yes_depth_3":    yes_depth_3,
            "no_depth_3":     no_depth_3,
            "time_elapsed":   time_elapsed,
            "coinbase_price": cb_price,
            "s0_price":       s0_price,
            "secs_to_close":  secs_to_close,
            "spread_yes":     spread_yes,
            "ts_ms":          ts_ms,
            "ready":          True,
        }

    def is_ready(self) -> bool:
        """Have both the Kalshi snapshot and a Coinbase tick been received?"""
        with self._lock:
            return (self._got_snapshot
                    and bool(self.yes_bids)
                    and bool(self.no_bids)
                    and self._got_coinbase_tick)

    # ── WS callbacks ─────────────────────────────────────────────────────────

    def _on_open(self, ws):
        print(f"[LIVE] WS connected — subscribing to {self.ticker}")
        ws.send(json.dumps({
            "id":  1,
            "cmd": "subscribe",
            "params": {
                "channels":       ["orderbook_delta"],
                "market_tickers": [self.ticker],
            },
        }))

    def _on_message(self, ws, message):
        if not self.running:
            return
        try:
            msg = json.loads(message)
        except json.JSONDecodeError:
            return

        msg_type = msg.get("type", "")

        if msg_type == "orderbook_snapshot":
            snap = msg.get("msg", {})
            with self._lock:
                self.yes_bids = {}
                self.no_bids  = {}
                for price, size in snap.get("yes_dollars_fp", []):
                    if float(size) > 0:
                        self.yes_bids[str(price)] = float(size)
                for price, size in snap.get("no_dollars_fp", []):
                    if float(size) > 0:
                        self.no_bids[str(price)] = float(size)
                self._got_snapshot = True
                self.update_count += 1
            print(f"[LIVE] Snapshot received — "
                  f"{len(self.yes_bids)} YES levels, "
                  f"{len(self.no_bids)} NO levels")
            if self.on_update:
                self.on_update(self.get_snapshot())

        elif msg_type == "orderbook_delta":
            delta    = msg.get("msg", {})
            price    = delta.get("price_dollars")
            delta_fp = float(delta.get("delta_fp", 0))
            side     = delta.get("side", "")

            if price is None:
                return

            with self._lock:
                target = self.yes_bids if side == "yes" else self.no_bids
                if price in target:
                    new_size = target[price] + delta_fp
                    if new_size <= 0:
                        del target[price]
                    else:
                        target[price] = new_size
                elif delta_fp > 0:
                    target[price] = delta_fp
                self.update_count += 1

            if self.on_update:
                # Fire callback OUTSIDE the lock to keep the WS thread
                # responsive even if the strategy is slow.
                self.on_update(self.get_snapshot())

    def _on_error(self, ws, error):
        print(f"[LIVE ERROR] {error}")

    def _on_close(self, ws, code, msg):
        print(f"[LIVE CLOSED] code={code}")
        self._closed_event.set()

    # ── Coinbase WS callbacks ────────────────────────────────────────────────

    def _on_cb_open(self, ws):
        print("[LIVE] Coinbase ticker stream connected")
        ws.send(json.dumps({
            "type":        "subscribe",
            "product_ids": ["BTC-USD"],
            "channel":     "ticker",
        }))

    def _on_cb_message(self, ws, message):
        if not self.running:
            return
        try:
            msg = json.loads(message)
        except json.JSONDecodeError:
            return
        if msg.get("channel") != "ticker":
            return
        # Coinbase batches multiple ticks per message — we only need the
        # most recent BTC-USD price, so just overwrite as we iterate.
        for event in msg.get("events", []):
            for ticker in event.get("tickers", []):
                if ticker.get("product_id") != "BTC-USD":
                    continue
                try:
                    price = float(ticker["price"])
                except (KeyError, TypeError, ValueError):
                    continue
                with self._lock:
                    self.coinbase_price = price
                    self._got_coinbase_tick = True
                    self.coinbase_count += 1

    def _on_cb_error(self, ws, error):
        print(f"[LIVE CB ERROR] {error}")

    def _on_cb_close(self, ws, code, msg):
        print(f"[LIVE CB CLOSED] code={code}")
        self._cb_closed_event.set()

    # ── Start / stop ─────────────────────────────────────────────────────────

    def start(self):
        self.running = True
        self._closed_event.clear()
        self._cb_closed_event.clear()

        # Kalshi WS
        auth_headers = _build_ws_auth_headers(self.key_id, self.private_key)
        self._ws = websocket.WebSocketApp(
            KALSHI_WS_URL,
            header=auth_headers,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws_thread = threading.Thread(
            target=self._ws.run_forever, daemon=True
        )
        self._ws_thread.start()

        # Coinbase WS (public, no auth)
        self._cb_ws = websocket.WebSocketApp(
            COINBASE_WS_URL,
            on_open=self._on_cb_open,
            on_message=self._on_cb_message,
            on_error=self._on_cb_error,
            on_close=self._on_cb_close,
        )
        self._cb_ws_thread = threading.Thread(
            target=self._cb_ws.run_forever, daemon=True
        )
        self._cb_ws_thread.start()

        print(f"[LIVE] Started collector for {self.ticker}")

    def wait_until_ready(self, timeout=10.0) -> bool:
        """
        Block until both the initial Kalshi snapshot AND the first Coinbase
        tick have landed (or timeout elapses). Call this before letting
        the strategy start trading so it doesn't see zeros on early ticks.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.is_ready():
                return True
            time.sleep(0.05)
        return False

    def stop(self):
        """
        Sets running=False first so in-flight handlers stop touching state,
        then closes both streams and waits for both to confirm.
        """
        self.running = False

        def close_kalshi():
            if self._ws:
                try:
                    self._ws.close()
                except Exception:
                    pass

        def close_coinbase():
            if self._cb_ws:
                try:
                    self._cb_ws.close()
                except Exception:
                    pass

        t1 = threading.Thread(target=close_kalshi,   daemon=True)
        t2 = threading.Thread(target=close_coinbase, daemon=True)
        t1.start()
        t2.start()

        kalshi_ok   = self._closed_event.wait(timeout=5)
        coinbase_ok = self._cb_closed_event.wait(timeout=5)

        if not kalshi_ok:
            print("[LIVE WARN] Kalshi WS did not confirm close within 5s")
        if not coinbase_ok:
            print("[LIVE WARN] Coinbase WS did not confirm close within 5s")

        print(
            f"[LIVE] Stopped."
            f"  Kalshi updates: {self.update_count:,}"
            f"  Coinbase ticks: {self.coinbase_count:,}"
        )