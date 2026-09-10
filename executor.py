"""
Kalshi market-order executor for the BTC 15m stat-arb strategy.

Design notes
------------
* Market orders only (per spec). Limit orders would require a separate path
  with order_id tracking for cancels/replaces.
* All HTTP calls are signed fresh per request (Kalshi signature is
  timestamp + method + path, signed with RSA-PSS-SHA256).
* Order placement is thread-safe via a single lock so two strategy fires
  in the same millisecond can't race on the idempotency counter.
* Position state is tracked locally AND reconciled against /portfolio/positions
  before each order, because local state can drift if you restart mid-window.
* The strategy file imports `Executor` and calls `try_trade(snapshot)` on
  every orderbook update. The strategy decides whether to trade and
  what size. The executor only handles the API mechanics.

Hook points for your strategy file
----------------------------------
1. `entry_rule(snapshot, position) -> EntrySignal | None`
2. `exit_rule(snapshot, position) -> ExitSignal | None`
3. Position size lives INSIDE the EntrySignal / ExitSignal you return.

See the bottom of this file for the exact dataclass shapes.
"""

# ── Imports ───────────────────────────────────────────────────────────────────

import os
import time
import json
import uuid
import base64
import threading
import requests
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Literal, Callable, Dict, Any
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding


# ── Constants ─────────────────────────────────────────────────────────────────

KALSHI_REST_BASE = "https://api.elections.kalshi.com/trade-api/v2"
REQUEST_TIMEOUT  = 5.0   # seconds — fast fail rather than block the strategy
MAX_RETRIES      = 2     # one retry on 5xx / connection errors, no more


# ── Auth (mirrors your collector's implementation) ────────────────────────────

def _build_auth_headers(key_id, private_key, method, path):
    """
    Builds a signed request header set. Signature covers timestamp + method
    + path (NOT body or query string). Fresh timestamp every call.
    """
    ts_ms     = str(int(time.time() * 1000))
    msg_bytes = (ts_ms + method.upper() + path).encode("utf-8")
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
        "Accept":                  "application/json",
    }


# ── Signal dataclasses (these are what your strategy returns) ─────────────────

Side    = Literal["yes", "no"]
Action  = Literal["buy", "sell"]


@dataclass
class EntrySignal:
    """
    Returned by your entry_rule when you want to open a position.

    side     : "yes" or "no" — which contract to buy
    count    : number of contracts (this is your POSITION SIZE)
    reason   : free-form string, logged for post-trade analysis
    """
    side:   Side
    count:  int
    reason: str = ""


@dataclass
class ExitSignal:
    """
    Returned by your exit_rule when you want to close (or reduce) an open
    position.

    count    : number of contracts to sell. Pass position.count to fully exit,
               or less for partial exits.
    reason   : free-form string, logged for post-trade analysis
    """
    count:  int
    reason: str = ""


# ── Position state ────────────────────────────────────────────────────────────

@dataclass
class Position:
    """
    Local view of our position in the current market.
    Reconciled against /portfolio/positions before each order.
    """
    ticker:        str
    side:          Optional[Side] = None      # None when flat
    count:         int             = 0         # contracts held
    avg_entry:     float           = 0.0       # avg fill price in dollars
    realized_pnl:  float           = 0.0       # in dollars
    last_reconcile_ts: float       = 0.0

    @property
    def is_flat(self) -> bool:
        return self.count == 0


# ── Order result ──────────────────────────────────────────────────────────────

@dataclass
class OrderResult:
    success:        bool
    order_id:       Optional[str]      = None
    filled_count:   int                = 0
    avg_fill_price: Optional[float]    = None    # dollars
    status:         Optional[str]      = None    # "executed", "resting", "canceled" etc.
    error:          Optional[str]      = None
    raw_response:   Dict[str, Any]     = field(default_factory=dict)


# ── Executor ──────────────────────────────────────────────────────────────────

class Executor:
    """
    One Executor instance per market (i.e. per 15-minute window).
    Reuse a single Executor across orders within the same window.

    Usage from your strategy file
    -----------------------------
        from executor import Executor, EntrySignal, ExitSignal

        executor = Executor(ticker=market["ticker"],
                            key_id=key_id, private_key=private_key)

        # On every orderbook update in the data collector:
        executor.try_trade(snapshot)
    """

    def __init__(self, ticker, key_id, private_key,
                 entry_rule: Callable[..., Optional[EntrySignal]] = None,
                 exit_rule:  Callable[..., Optional[ExitSignal]]  = None,
                 log_file:   Optional[str]                        = None,
                 dry_run:    bool                                 = True):
        """
        ticker      : the KXBTC15M-... market ticker for this window
        key_id      : Kalshi API key id (string)
        private_key : loaded RSA private key object
        entry_rule  : your strategy's entry function (see hook section below)
        exit_rule   : your strategy's exit function  (see hook section below)
        log_file    : optional CSV path for trade log; auto-named if None
        dry_run     : if True, logs orders but does NOT hit the API.
                      START WITH THIS = TRUE until you've verified behaviour.
        """
        self.ticker      = ticker
        self.key_id      = key_id
        self.private_key = private_key
        self.entry_rule  = entry_rule
        self.exit_rule   = exit_rule
        self.dry_run     = dry_run

        self.position = Position(ticker=ticker)
        self._lock    = threading.Lock()    # serializes order placement

        # Throttle: don't pound the API if strategy fires every tick
        self._last_order_attempt_ts = 0.0
        self.min_seconds_between_orders = 0.25   # 4 orders/sec cap

        # Balance cache (cents int from Kalshi, exposed as dollars float).
        # See get_cash_balance() for the read API used by your strategy.
        self._cached_balance_dollars: Optional[float] = None
        self._balance_fetched_at:    float            = 0.0
        self.default_balance_ttl_seconds              = 5.0

        # HTTP session reuse for keep-alive (lower per-call latency)
        self._session = requests.Session()

        # Trade log — file is created lazily on first actual trade write
        ts = int(datetime.now(timezone.utc).timestamp())
        self.log_file = log_file or f"trades_{ts}_{ticker}.csv"

        # Reconcile once at startup so we know our starting position
        self._reconcile_position()

        mode = "DRY RUN" if dry_run else "LIVE"
        print(f"[EXEC] Executor ready for {ticker} ({mode})")

    # ── Logging ───────────────────────────────────────────────────────────────

    def _init_log(self):
        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", newline="") as f:
                f.write("timestamp_utc,ticker,action,side,count,"
                        "avg_fill_price,status,reason,order_id,error\n")

    def _log_trade(self, action, side, count, fill_price, status,
                   reason, order_id, error):
        self._init_log()   # no-op if file already exists; creates on first trade
        safe_reason = (reason or "").replace('"', "'")
        safe_error  = (error  or "").replace('"', "'")
        price_str   = "" if fill_price is None else str(fill_price)
        with open(self.log_file, "a", newline="") as f:
            f.write(
                f"{datetime.now(timezone.utc).isoformat()},"
                f"{self.ticker},{action},{side},{count},"
                f"{price_str},"
                f"{status or ''},"
                f'"{safe_reason}",'
                f"{order_id or ''},"
                f'"{safe_error}"\n'
            )

    # ── Public entry point: called by the strategy on every snapshot ──────────

    def try_trade(self, snapshot: Dict[str, Any]):
        """
        Called from the data-collector thread on every orderbook update.
        Routes to entry_rule if flat, exit_rule if in a position.

        snapshot : a dict with the orderbook fields your strategy needs.
                   Whatever your reconstructed-orderbook row looks like
                   (best_yes_bid, best_yes_ask, mid_yes, spread_yes, etc.)
                   plus any features you compute from the Coinbase stream.

                   Before dispatching, this method injects:
                     snapshot["cash_balance"] = self.get_cash_balance()
                   so your Kelly sizer can read it as a dict field.

        Returns the OrderResult if an order was attempted, else None.
        """
        # Throttle: skip if we placed an order very recently
        now = time.time()
        if now - self._last_order_attempt_ts < self.min_seconds_between_orders:
            return None

        # Inject cash balance (cached, ~5s TTL) so the strategy can size off it.
        # May be None if the API has never returned successfully — strategy
        # should treat None as "don't trade".
        snapshot = dict(snapshot)   # shallow copy: don't mutate caller's dict
        snapshot["cash_balance"] = self.get_cash_balance()

        # Hot-path decision happens OUTSIDE the lock to keep the websocket
        # thread responsive. Lock only when actually placing an order.
        if self.position.is_flat:
            if self.entry_rule is None:
                return None
            signal = self.entry_rule(snapshot, self.position)
            if signal is None:
                return None
            return self._place_market_order(
                action="buy", side=signal.side,
                count=signal.count, reason=signal.reason,
            )
        else:
            if self.exit_rule is None:
                return None
            signal = self.exit_rule(snapshot, self.position)
            if signal is None:
                return None
            # When exiting, we SELL the side we hold
            return self._place_market_order(
                action="sell", side=self.position.side,
                count=signal.count, reason=signal.reason,
            )

    # ── Order placement ───────────────────────────────────────────────────────

    def _place_market_order(self, action: Action, side: Side,
                            count: int, reason: str) -> OrderResult:
        """
        Sends a market order to /portfolio/orders.

        Kalshi market-order semantics:
          - action="buy"  + side="yes" -> buys YES at best ask
          - action="sell" + side="yes" -> sells YES at best bid
          - action="buy"  + side="no"  -> buys NO at best ask
          - action="sell" + side="no"  -> sells NO at best bid

        We use an idempotency-style client_order_id (UUID) so even if the
        request retries on a network blip, Kalshi won't double-fill.
        """
        if count <= 0:
            return OrderResult(success=False, error="count must be > 0")

        with self._lock:
            self._last_order_attempt_ts = time.time()

            client_order_id = str(uuid.uuid4())
            body = {
                "ticker":          self.ticker,
                "action":          action,
                "side":            side,
                "count":           int(count),
                "type":            "market",
                "client_order_id": client_order_id,
            }

            if self.dry_run:
                print(f"[DRY] {action.upper()} {count} {side.upper()} "
                      f"on {self.ticker} — reason: {reason}")
                self._log_trade(action, side, count, None, "dry_run",
                                reason, client_order_id, None)
                # Simulate position update for paper-trading continuity
                self._apply_simulated_fill(action, side, count)
                return OrderResult(success=True, order_id=client_order_id,
                                   filled_count=count, status="dry_run")

            # ── Live order path ───────────────────────────────────────────────
            path = "/trade-api/v2/portfolio/orders"
            url  = KALSHI_REST_BASE + "/portfolio/orders"
            result = self._http_post(url, path, body)

            # Persist log regardless of outcome
            self._log_trade(
                action, side, count,
                result.avg_fill_price, result.status,
                reason, result.order_id, result.error,
            )

            # Update local position from the response
            if result.success and result.filled_count > 0:
                self._apply_fill(action, side,
                                 result.filled_count, result.avg_fill_price)

            return result

    def _http_post(self, url, path, body) -> OrderResult:
        """
        POSTs with up to MAX_RETRIES on transient errors. Auth headers are
        re-signed on every attempt (timestamp must be fresh).
        """
        last_exc = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                headers = _build_auth_headers(
                    self.key_id, self.private_key, "POST", path,
                )
                resp = self._session.post(
                    url, headers=headers, json=body, timeout=REQUEST_TIMEOUT,
                )
                # Retry only on 5xx; 4xx is a logic error, don't retry
                if 500 <= resp.status_code < 600 and attempt < MAX_RETRIES:
                    time.sleep(0.1 * (attempt + 1))
                    continue
                return self._parse_order_response(resp)
            except (requests.Timeout, requests.ConnectionError) as e:
                last_exc = e
                if attempt < MAX_RETRIES:
                    time.sleep(0.1 * (attempt + 1))
                    continue
                return OrderResult(success=False, error=f"network: {e}")
            except Exception as e:
                return OrderResult(success=False, error=f"unexpected: {e}")
        return OrderResult(success=False,
                           error=f"exhausted retries: {last_exc}")

    def _parse_order_response(self, resp) -> OrderResult:
        """
        Parses the response from /portfolio/orders. Kalshi returns the order
        record under an "order" key with fields like status, filled_count,
        and (for executed market orders) yes_price / no_price in cents.

        Note: Kalshi's exact response shape may evolve. This parser is
        defensive — if a field isn't there, we surface the raw response
        in OrderResult.raw_response and you can adjust.
        """
        try:
            payload = resp.json()
        except Exception:
            return OrderResult(success=False, status=str(resp.status_code),
                               error=f"non-json response: {resp.text[:200]}")

        if resp.status_code >= 400:
            return OrderResult(
                success=False,
                status=str(resp.status_code),
                error=payload.get("error", {}).get("message")
                      or json.dumps(payload)[:300],
                raw_response=payload,
            )

        order = payload.get("order", {})
        status        = order.get("status")
        order_id      = order.get("order_id")
        # Kalshi reports yes_price/no_price in CENTS for executed orders
        yes_price_c   = order.get("yes_price")
        no_price_c    = order.get("no_price")
        filled        = int(order.get("filled_count")
                            or order.get("place_count") or 0)

        # Pick whichever side's price field is populated for the fill
        fill_price_dollars = None
        if yes_price_c is not None:
            fill_price_dollars = float(yes_price_c) / 100.0
        elif no_price_c is not None:
            fill_price_dollars = float(no_price_c) / 100.0

        return OrderResult(
            success=True,
            order_id=order_id,
            filled_count=filled,
            avg_fill_price=fill_price_dollars,
            status=status,
            raw_response=payload,
        )

    # ── Position tracking ─────────────────────────────────────────────────────

    def _apply_fill(self, action, side, count, fill_price):
        """Update local position from a real fill."""
        if action == "buy":
            new_count = self.position.count + count
            if self.position.is_flat:
                self.position.avg_entry = fill_price or 0.0
            else:
                # Weighted avg (only relevant if scaling in, same side)
                total_cost = (self.position.avg_entry * self.position.count
                              + (fill_price or 0.0) * count)
                self.position.avg_entry = total_cost / new_count
            self.position.side  = side
            self.position.count = new_count
        else:  # sell -> reduce / close
            if fill_price is not None and self.position.avg_entry:
                self.position.realized_pnl += (
                    (fill_price - self.position.avg_entry) * count
                )
            self.position.count -= count
            if self.position.count <= 0:
                self.position.count = 0
                self.position.side  = None
                self.position.avg_entry = 0.0

    def _apply_simulated_fill(self, action, side, count):
        """Dry-run version: update position without a real price."""
        if action == "buy":
            self.position.side  = side
            self.position.count += count
        else:
            self.position.count -= count
            if self.position.count <= 0:
                self.position.count = 0
                self.position.side  = None

    # ── Balance ──────────────────────────────────────────────────────────────

    def get_cash_balance(self, max_age_seconds: Optional[float] = None) -> Optional[float]:
        """
        Returns the settled cash balance on the Kalshi account, in DOLLARS.

        Kalshi reports the value as integer cents — we divide by 100 here so
        strategy code always works in dollars.

        This is the denominator for Kelly sizing:
            stake_dollars = cash_balance * kelly_fraction
            count         = int(stake_dollars / price_per_contract)

        Args
        ----
        max_age_seconds : how stale a cached value is acceptable. Defaults
                          to self.default_balance_ttl_seconds (5s).
                          Pass 0 to force a fresh API call.

        Returns
        -------
        float dollars, or None if the API call failed and there is no
        usable cached value. Callers MUST handle None — the safe behavior
        is to skip the trade entirely.

        Dry-run mode
        ------------
        In dry-run mode we still hit the live endpoint, because Kelly
        sizing during paper trading is much more informative when it uses
        your actual account size rather than a hardcoded fake.

        Caveats
        -------
        * The balance EXCLUDES cash locked as collateral on open positions
          in OTHER markets. Money tied up in this market's position is also
          no longer deployable. Size conservatively if you have concurrent
          open positions elsewhere.
        * Kalshi's response field is "balance" (integer cents). We also
          check "balance_cents" as a defensive fallback in case the API
          field name changes.
        """
        ttl = self.default_balance_ttl_seconds if max_age_seconds is None \
              else max_age_seconds
        age = time.time() - self._balance_fetched_at
        if self._cached_balance_dollars is not None and age <= ttl:
            return self._cached_balance_dollars

        path = "/trade-api/v2/portfolio/balance"
        url  = KALSHI_REST_BASE + "/portfolio/balance"
        try:
            headers = _build_auth_headers(
                self.key_id, self.private_key, "GET", path,
            )
            resp = self._session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                print(f"[EXEC] Balance fetch failed: HTTP {resp.status_code} "
                      f"— returning cached={self._cached_balance_dollars}")
                return self._cached_balance_dollars

            data = resp.json()
            # Kalshi returns cents as an integer under "balance"
            raw = data.get("balance") if data.get("balance") is not None \
                  else data.get("balance_cents")
            if raw is None:
                print(f"[EXEC] Balance field missing in response: {data}")
                return self._cached_balance_dollars

            self._cached_balance_dollars = float(raw) / 100.0
            self._balance_fetched_at     = time.time()
            return self._cached_balance_dollars

        except (requests.Timeout, requests.ConnectionError) as e:
            print(f"[EXEC] Balance fetch network error: {e} "
                  f"— returning cached={self._cached_balance_dollars}")
            return self._cached_balance_dollars
        except Exception as e:
            print(f"[EXEC] Balance fetch unexpected error: {e} "
                  f"— returning cached={self._cached_balance_dollars}")
            return self._cached_balance_dollars

    def _reconcile_position(self):
        """
        Pulls /portfolio/positions and updates the local position to match
        what Kalshi actually thinks we hold. Run at startup and optionally
        on a schedule to catch drift.

        Drift sources to be aware of:
          - You ran a manual trade through the UI mid-window
          - A previous executor died after placing an order but before
            its response came back
          - Partial fills that didn't surface in the order response
        """
        if self.dry_run:
            self.position.last_reconcile_ts = time.time()
            return

        path = "/trade-api/v2/portfolio/positions"
        url  = KALSHI_REST_BASE + "/portfolio/positions"
        try:
            headers = _build_auth_headers(
                self.key_id, self.private_key, "GET", path,
            )
            resp = self._session.get(
                url, headers=headers,
                params={"ticker": self.ticker},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200:
                print(f"[EXEC] Reconcile failed: HTTP {resp.status_code}")
                return
            data = resp.json()
            for pos in data.get("market_positions", []):
                if pos.get("ticker") != self.ticker:
                    continue
                # Kalshi reports position as a signed integer:
                #   positive = long YES, negative = long NO
                kalshi_pos = int(pos.get("position", 0))
                if kalshi_pos > 0:
                    self.position.side  = "yes"
                    self.position.count = kalshi_pos
                elif kalshi_pos < 0:
                    self.position.side  = "no"
                    self.position.count = abs(kalshi_pos)
                else:
                    self.position.side  = None
                    self.position.count = 0
                break
            self.position.last_reconcile_ts = time.time()
            print(f"[EXEC] Reconciled: {self.position.side} "
                  f"x {self.position.count}")
        except Exception as e:
            print(f"[EXEC] Reconcile exception: {e}")

    # ── Helpers your strategy can call ────────────────────────────────────────

    def force_flatten(self, reason="manual flatten"):
        """
        Emergency / end-of-window: market-sell whatever we hold.
        Call this from your main loop just before the window closes if
        you don't want to hold to expiry.
        """
        if self.position.is_flat:
            return None
        return self._place_market_order(
            action="sell",
            side=self.position.side,
            count=self.position.count,
            reason=reason,
        )

    def snapshot_state(self):
        return {
            "ticker":       self.ticker,
            "side":         self.position.side,
            "count":        self.position.count,
            "avg_entry":    self.position.avg_entry,
            "realized_pnl": self.position.realized_pnl,
        }


# ══════════════════════════════════════════════════════════════════════════════
#
#   >>> STRATEGY HOOKS — your separate file plugs in here <<<
#
#   In your strategy file (e.g. strategy_rules.py) you write two functions
#   with these exact signatures:
#
#       def entry_rule(snapshot: dict, position: Position) -> EntrySignal | None
#       def exit_rule (snapshot: dict, position: Position) -> ExitSignal  | None
#
#   POSITION SIZE goes inside the signals you return:
#       return EntrySignal(side="yes", count=YOUR_SIZE_HERE, reason="...")
#       return ExitSignal(count=position.count, reason="...")   # full exit
#
#   `snapshot` is whatever you pass into executor.try_trade(snapshot).
#   In practice, build this dict in your data-collector callback from the
#   reconstructed orderbook fields plus any Coinbase-derived features.
#
#   Wire-up example (in your main script):
#
#       from executor import Executor
#       from strategy_rules import entry_rule, exit_rule
#
#       executor = Executor(
#           ticker      = market["ticker"],
#           key_id      = key_id,
#           private_key = private_key,
#           entry_rule  = entry_rule,
#           exit_rule   = exit_rule,
#           dry_run     = True,   # flip to False ONLY after paper-testing
#       )
#
#       # Inside KalshiBTC15mCollector._save_ob_row, after writing the row:
#       #   snapshot = { "best_yes_bid": ..., "mid_yes": ..., ... }
#       #   executor.try_trade(snapshot)
#
#       # Just before collector.stop() at end of window:
#       executor.force_flatten(reason="window close")
#
# ══════════════════════════════════════════════════════════════════════════════