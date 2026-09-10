"""
Main runner — THIS IS THE FILE YOU EXECUTE.

What this file does, per 15-minute window:
  1. Discovers the active KXBTC15M market via Kalshi REST.
  2. Starts a live in-memory orderbook collector (websocket).
  3. Waits for the initial snapshot to land.
  4. Spins up an Executor wired to your strategy_rules.entry_rule and
     strategy_rules.exit_rule.
  5. Polls the live snapshot every TICK_INTERVAL_SECONDS and feeds it to
     the executor's try_trade() method.
  6. At window close: flattens any open position, stops the collector,
     and rolls to the next window.

Set DRY_RUN = False (and only then) to send real orders. Don't skip the
paper-trading step.

Run it with:
    python main_runner.py
"""

import os
import csv
import json
import time
import threading
import subprocess as _subprocess
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

# Reuse your existing helpers from the data-recording file. If you put
# them in a module with a different name, adjust this import.
from kalshi_btc_15m_data_collector import (
    load_private_key,
    get_current_window_times,
    get_active_btc15m_market,
    normalize_market,
)

from kalshi_live_data_collector import KalshiLiveCollector, COINBASE_WS_URL
from executor                   import Executor
import strategy_rules
from strategy_rules             import entry_rule, exit_rule


# ── Windows toast notifications ───────────────────────────────────────────────

def _fire_toast(title: str, body: str) -> None:
    """Fire a Windows desktop toast without blocking the caller."""
    ps = (
        "[void][Windows.UI.Notifications.ToastNotificationManager,"
        "Windows.UI.Notifications,ContentType=WindowsRuntime];"
        "[void][Windows.Data.Xml.Dom.XmlDocument,"
        "Windows.Data.Xml.Dom.XmlDocument,ContentType=WindowsRuntime];"
        "$d=New-Object Windows.Data.Xml.Dom.XmlDocument;"
        "$d.LoadXml('<toast><visual><binding template=\"ToastGeneric\">"
        f"<text>{title}</text><text>{body}</text>"
        "</binding></visual></toast>');"
        "$t=[Windows.UI.Notifications.ToastNotification]::new($d);"
        "[Windows.UI.Notifications.ToastNotificationManager]::"
        "CreateToastNotifier('Microsoft.Windows.Explorer').Show($t)"
    )
    try:
        _subprocess.Popen(
            ["powershell", "-WindowStyle", "Hidden",
             "-NonInteractive", "-Command", ps],
            creationflags=_subprocess.CREATE_NO_WINDOW,
        )
    except Exception:
        pass


# ── Volatility pre-collection ─────────────────────────────────────────────────

def collect_volatility(duration_seconds: float = 900) -> float:
    """
    Connect to Coinbase's public websocket, collect BTC-USD tick prices for
    `duration_seconds`, aggregate to 0.25-second bars, compute log-return std,
    and return volatility in per-√second units (ready for calculate_p_up).

    Unit conversion:
        vol_0.25s          = std of log returns at 0.25-second bars
        vol_per_sqrt_sec   = vol_0.25s / sqrt(0.25)   (= vol_0.25s * 2)
    This scaling ensures vol * sqrt(time_left_seconds) is dimensionally correct
    in the GBM formula inside calculate_p_up.
    """
    import websocket  # local import — only needed during pre-collection

    ticks = []          # [(unix_timestamp, price), ...]
    ticks_lock = threading.Lock()
    done_event = threading.Event()

    def on_open(ws):
        ws.send(json.dumps({
            "type":        "subscribe",
            "product_ids": ["BTC-USD"],
            "channel":     "ticker",
        }))

    def on_message(ws, message):
        try:
            msg = json.loads(message)
        except json.JSONDecodeError:
            return
        if msg.get("channel") != "ticker":
            return
        for event in msg.get("events", []):
            for ticker_data in event.get("tickers", []):
                if ticker_data.get("product_id") != "BTC-USD":
                    continue
                try:
                    price = float(ticker_data["price"])
                    with ticks_lock:
                        ticks.append((time.time(), price))
                except (KeyError, TypeError, ValueError):
                    continue

    def on_error(ws, error):
        print(f"[VOL] Coinbase error: {error}")

    def on_close(ws, code, msg):
        done_event.set()

    ws_app = websocket.WebSocketApp(
        COINBASE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws_thread = threading.Thread(target=ws_app.run_forever, daemon=True)
    ws_thread.start()

    print(f"[VOL] Collecting {duration_seconds:.0f}s of Coinbase data "
          f"for volatility estimate...")
    time.sleep(duration_seconds)
    ws_app.close()
    done_event.wait(timeout=5)

    with ticks_lock:
        snapshot = list(ticks)

    if len(snapshot) < 10:
        raise RuntimeError(
            f"[VOL] Only {len(snapshot)} ticks collected — "
            "insufficient for volatility estimation."
        )

    # Aggregate to 0.25-second bars (last tick price per bar)
    start_ts = snapshot[0][0]
    df = pd.DataFrame(snapshot, columns=["ts", "price"])
    df["bar"] = ((df["ts"] - start_ts) / 0.25).astype(int)
    btc_price_agg = df.groupby("bar")["price"].last()

    returns = np.log(btc_price_agg / btc_price_agg.shift(1)).dropna()
    vol_per_quarter_sec = float(np.std(returns))

    # Scale to per-√second so the formula vol * sqrt(time_left_sec) is correct
    vol_per_sqrt_sec = vol_per_quarter_sec / np.sqrt(0.25)

    print(f"[VOL] Done.  ticks={len(snapshot):,}  bars={len(btc_price_agg):,}  "
          f"vol_0.25s={vol_per_quarter_sec:.6f}  "
          f"vol/sqrt(s)={vol_per_sqrt_sec:.6f}")

    return vol_per_sqrt_sec


# ── Dry-run signal logger ─────────────────────────────────────────────────────

DRY_RUN_LOGS_DIR    = Path(__file__).parent / "dry_run_logs"
DRY_RUN_RAW_DIR     = DRY_RUN_LOGS_DIR / "raw"
MIN_DEPTH_THRESHOLD = 500   # minimum top-3 orderbook depth (dollars) to consider a trade liquid

# Signal log — one row per signal tick that clears EV_THRESHOLD + depth filter
_LOG_COLUMNS = [
    "timestamp_utc",
    "ticker",
    "best_yes_bid",     # best YES bid (dollars)  — collected
    "best_yes_ask",     # best YES ask = 1 - best_no_bid — derived
    "best_no_bid",      # best NO  bid (dollars)  — collected
    "best_no_ask",      # best NO  ask = 1 - best_yes_bid — derived
    "signal_side",      # "yes" or "no" — which side triggered, recorded at log time
    "p_up",             # GBM probability BTC closes above s0_price
    "ev",               # expected value of the triggered signal
    "kelly_contracts",  # full-Kelly position size in contracts (uncapped)
    "depth",            # top-3 orderbook depth for the triggered side (dollars)
    "depth_ok",         # True if depth >= MIN_DEPTH_THRESHOLD
    "vol_estimate",     # per-sqrt(s) GBM vol estimate derived from the prior window
    "realized_vol",     # per-sqrt(s) vol actually realized in this window (stamped at close)
    "coinbase_price",   # live BTC-USD price at the moment of this signal
    "time_elapsed_s",   # seconds elapsed in the 15-min window at signal time
    "s0_price",         # window strike / threshold
    "resolution",       # "yes"/"no" — BTC above/below s0_price at window close
]

# Raw data — one row per ready tick, no filtering, written per-window to dry_run_logs/raw/
_RAW_COLUMNS = [
    "timestamp_utc",
    "ticker",
    "time_elapsed_s",
    "best_yes_bid",
    "best_yes_ask",
    "best_no_bid",
    "best_no_ask",
    "yes_depth_3",
    "no_depth_3",
    "spread_yes",
    "coinbase_price",
    "p_up",
    "ev_yes",
    "ev_no",
    "s0_price",
]


class DryRunLogger:
    """
    Records every tick where at least one side's expected value clears
    EV_THRESHOLD, independent of whether the executor actually traded
    (position might already be open, size might be zero, etc.).

    This gives you the full opportunity set to evaluate the strategy
    before risking real money.

    Lifecycle
    ---------
    log_tick()        — call on every ready snapshot inside the tick loop
    finalize_window() — call once per window after the position is flat;
                        attaches resolution and queues rows for writing
    flush_remaining() — call once at session end to write any partial batch
    """

    def __init__(self, events_per_file: int = 4):
        self.events_per_file  = events_per_file
        self._window_rows: list[dict] = []  # signal rows for the in-progress window
        self._raw_rows:    list[dict] = []  # full-snapshot rows for every ready tick
        self._price_buffer: list      = []  # (time_elapsed_s, price) for realized-vol calc
        self._buffer:      list[dict] = []  # completed signal windows pending flush
        self._completed    = 0
        self._file_index   = 0
        DRY_RUN_LOGS_DIR.mkdir(parents=True, exist_ok=True)
        DRY_RUN_RAW_DIR.mkdir(parents=True, exist_ok=True)

    # ── Per-tick full recording (unconditional) ───────────────────────────────

    def record_tick(self, snapshot: dict, ticker: str) -> None:
        """
        Called on every ready tick before any signal filtering.
        1. Feeds _price_buffer for realized-vol computation.
        2. Records the full market snapshot + model metrics to _raw_rows,
           flushed to dry_run_logs/raw/{ticker}_*.csv at window close.
        """
        t = snapshot["time_elapsed"]
        p = snapshot["coinbase_price"]
        self._price_buffer.append((t, p))

        vol = strategy_rules.VOLATILITY
        if vol and vol > 0:
            p_up   = strategy_rules.calculate_p_up(snapshot["s0_price"], p, t, vol)
            ev_yes = strategy_rules.calculate_ev_up(
                snapshot["best_yes_ask"], p_up)
            ev_no  = strategy_rules.calculate_ev_down(
                snapshot["best_yes_bid"], p_up)
        else:
            p_up = ev_yes = ev_no = None

        self._raw_rows.append({
            "timestamp_utc":  datetime.now(timezone.utc).isoformat(),
            "ticker":         ticker,
            "time_elapsed_s": round(t, 3),
            "best_yes_bid":   round(snapshot["best_yes_bid"], 4),
            "best_yes_ask":   round(snapshot["best_yes_ask"], 4),
            "best_no_bid":    round(snapshot["best_no_bid"],  4),
            "best_no_ask":    round(snapshot["best_no_ask"],  4),
            "yes_depth_3":    round(snapshot["yes_depth_3"],  2),
            "no_depth_3":     round(snapshot["no_depth_3"],   2),
            "spread_yes":     round(snapshot["spread_yes"],   4),
            "coinbase_price": round(p, 2),
            "p_up":           round(p_up,   6) if p_up   is not None else "",
            "ev_yes":         round(ev_yes, 6) if ev_yes is not None else "",
            "ev_no":          round(ev_no,  6) if ev_no  is not None else "",
            "s0_price":       snapshot["s0_price"],
        })

    # ── Per-tick evaluation ───────────────────────────────────────────────────

    def log_tick(self, snapshot: dict, ticker: str) -> None:
        """
        Evaluate entry conditions using the current snapshot and append a row
        for each side whose EV exceeds EV_THRESHOLD.  Called on every ready
        tick regardless of position state.

        snapshot must already contain "cash_balance" (injected by the tick
        loop before this call) so Kelly contract count can be computed.
        """
        vol = strategy_rules.VOLATILITY
        if vol is None or vol <= 0:
            return

        # Crossed-book guard: YES_ask + NO_ask < 1 means the book is stale or
        # has a phantom resting order. Both sides appear cheap simultaneously,
        # which is physically impossible in a real market. Skip these ticks.
        if snapshot["best_yes_ask"] + snapshot["best_no_ask"] < 1.0:
            return

        cash    = snapshot.get("cash_balance")   # may be None before first API fetch
        cb_price = snapshot["coinbase_price"]

        p_up = strategy_rules.calculate_p_up(
            snapshot["s0_price"], cb_price, snapshot["time_elapsed"], vol,
        )
        ev_up   = strategy_rules.calculate_ev_up(
            snapshot["best_yes_ask"], p_up,
        )
        ev_down = strategy_rules.calculate_ev_down(
            snapshot["best_yes_bid"], p_up,
        )

        now_utc = datetime.now(timezone.utc).isoformat()

        def _base_row():
            return {
                "timestamp_utc":  now_utc,
                "ticker":         ticker,
                "best_yes_bid":   round(snapshot["best_yes_bid"], 4),
                "best_yes_ask":   round(snapshot["best_yes_ask"], 4),
                "best_no_bid":    round(snapshot["best_no_bid"],  4),
                "best_no_ask":    round(snapshot["best_no_ask"],  4),
                "p_up":           round(p_up, 6),
                "vol_estimate":   round(vol, 8),
                "realized_vol":   None,
                "coinbase_price": round(cb_price, 2),
                "time_elapsed_s": round(snapshot["time_elapsed"], 2),
                "s0_price":       snapshot["s0_price"],
                "resolution":     None,
            }

        if ev_up > strategy_rules.EV_THRESHOLD:
            ask   = snapshot["best_yes_ask"]
            depth = snapshot["yes_depth_3"]
            kf    = max(0.0, strategy_rules.kelly_fraction_up(ask, p_up))
            kelly = int(cash * kf / ask) if (cash and ask > 0) else 0
            row   = _base_row()
            row.update({
                "signal_side":     "yes",
                "ev":              round(ev_up, 6),
                "kelly_contracts": kelly,
                "depth":           round(depth, 2),
                "depth_ok":        depth >= MIN_DEPTH_THRESHOLD,
            })
            self._window_rows.append(row)

        if ev_down > strategy_rules.EV_THRESHOLD:
            ask_down = snapshot["best_no_ask"]
            depth    = snapshot["no_depth_3"]
            kf       = max(0.0, strategy_rules.kelly_fraction_down(
                            snapshot["best_yes_bid"], p_up))
            kelly = int(cash * kf / ask_down) if (cash and ask_down > 0) else 0
            row   = _base_row()
            row.update({
                "signal_side":     "no",
                "ev":              round(ev_down, 6),
                "kelly_contracts": kelly,
                "depth":           round(depth, 2),
                "depth_ok":        depth >= MIN_DEPTH_THRESHOLD,
            })
            self._window_rows.append(row)

    # ── Realized volatility ───────────────────────────────────────────────────

    def _compute_realized_vol(self) -> float:
        """
        Compute realized vol from the full Coinbase price stream received this
        window (_price_buffer, populated every ready tick before any filtering).
        Aggregates to 0.25-second bars and returns per-sqrt-second std of log
        returns — same units as vol_estimate so the two are directly comparable.
        """
        if len(self._price_buffer) < 10:
            return 0.0
        elapsed = np.array([t for t, p in self._price_buffer])
        prices  = np.array([p for t, p in self._price_buffer])
        bar_idx = (elapsed / 0.25).astype(int)
        bar_prices = (pd.Series(prices, index=bar_idx)
                        .groupby(level=0).last())
        if len(bar_prices) < 3:
            return 0.0
        log_returns = np.log(bar_prices.values[1:] / bar_prices.values[:-1])
        if len(log_returns) < 2:
            return 0.0
        return round(float(np.std(log_returns)) / np.sqrt(0.25), 10)

    # ── Per-window finalization ───────────────────────────────────────────────

    def finalize_window(self, final_btc_price: float, s0_price: float,
                        ticker: str) -> float:
        """
        Attach resolution and realized_vol to every signal row from the
        just-closed window, write the full raw-tick CSV, move signal rows
        to the write buffer, and flush the signal CSV every events_per_file
        windows.  Returns realized_vol so the main loop can update the EWMA.
        """
        resolution   = "yes" if final_btc_price > s0_price else "no"
        realized_vol = self._compute_realized_vol()
        n_signals    = len(self._window_rows)

        for row in self._window_rows:
            row["resolution"]   = resolution
            row["realized_vol"] = realized_vol

        # Write per-window raw CSV (all ticks, no filter)
        if self._raw_rows:
            ts         = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            safe_tick  = ticker.replace("-", "_")
            raw_path   = DRY_RUN_RAW_DIR / f"{safe_tick}_{ts}.csv"
            with open(raw_path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=_RAW_COLUMNS,
                                        extrasaction="ignore")
                writer.writeheader()
                writer.writerows(self._raw_rows)
            print(f"[DRY LOG] Raw  {len(self._raw_rows):,} ticks  -> {raw_path.name}")

        self._buffer.extend(self._window_rows)
        self._window_rows  = []
        self._raw_rows     = []
        self._price_buffer = []
        self._completed   += 1

        print(f"[DRY LOG] Window finalized — "
              f"resolution={resolution}  "
              f"btc={final_btc_price:.2f}  s0={s0_price:.2f}  "
              f"signals={n_signals}  realized_vol={realized_vol:.2e}  "
              f"windows_done={self._completed}")

        if self._completed % self.events_per_file == 0:
            self._write_csv()

        return realized_vol

    # ── Session-end flush ─────────────────────────────────────────────────────

    def flush_remaining(self) -> None:
        """
        Write whatever remains in the buffer at session end, even if fewer
        than `events_per_file` windows completed.  Rows from an in-progress
        window (no resolution yet) are included with blank resolution fields.
        """
        self._buffer.extend(self._window_rows)
        self._window_rows  = []
        self._raw_rows     = []
        self._price_buffer = []
        if self._buffer:
            self._write_csv(label="partial")

    # ── CSV writer ────────────────────────────────────────────────────────────

    def _write_csv(self, label: str = "") -> None:
        self._file_index += 1
        ts    = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        tag   = f"_{label}" if label else ""
        fname = DRY_RUN_LOGS_DIR / f"dry_run_{ts}{tag}_{self._file_index:03d}.csv"

        with open(fname, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_LOG_COLUMNS,
                                    extrasaction="ignore")
            writer.writeheader()
            writer.writerows(self._buffer)

        print(f"[DRY LOG] Wrote {len(self._buffer)} row(s) → {fname.name}")
        self._buffer = []


# ── Run parameters ────────────────────────────────────────────────────────────

RUN_MINUTES           = 20160    # 2 weeks
TICK_INTERVAL_SECONDS = 0.05     # 20 Hz polling of the live snapshot
FLATTEN_SECONDS_BEFORE_CLOSE = 5 # exit any open position 5s before expiry
DRY_RUN               = True     # << flip to False ONLY after paper-testing


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    dotenv_path = os.environ.get(
        "KALSHI_ENV_PATH",
        os.path.expanduser("~/.claude/projects/api_keys.env"),
    )
    load_dotenv(dotenv_path)

    key_id           = os.environ.get("kalshi_api_key_id")
    private_key_path = os.environ.get("kalshi_private_key_path")

    if not key_id or not private_key_path:
        raise EnvironmentError(
            f"Credentials not found in {dotenv_path}.\n"
            "File must contain:\n"
            "  kalshi_api_key_id=your-key-id\n"
            "  kalshi_private_key_path=C:\\path\\to\\key.key"
        )

    private_key = load_private_key(private_key_path)
    print(f"Credentials loaded.  Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}\n")

    windows_traded = 0   # declared before try so finally can always read it
    _crashed       = False
    _fire_toast("Kalshi BTC Runner", f"{'DRY RUN' if DRY_RUN else 'LIVE'} started — collecting vol...")

    try:
        # Collect 15 minutes of Coinbase data to estimate vol before trading starts
        strategy_rules.VOLATILITY = collect_volatility(duration_seconds=900)

        logger               = DryRunLogger() if DRY_RUN else None
        realized_vols_history: list[float] = []   # rolling buffer for EWMA vol update
        run_until = time.time() + (RUN_MINUTES * 60)
        previous_ticker = None

        while time.time() < run_until:

            # ── 1. Discover the current window's market ─────────────────────
            window_start, window_close = get_current_window_times()
            raw_market = get_active_btc15m_market(
                window_close, exclude_ticker=previous_ticker,
            )
            if not raw_market:
                print("Could not find new market — retrying in 10s...")
                time.sleep(10)
                continue

            market = normalize_market(raw_market)
            if market["threshold"] is None:
                print("Threshold not yet set — retrying in 10s...")
                time.sleep(10)
                continue

            # ── 2. Start live collector ─────────────────────────────────────
            collector = KalshiLiveCollector(
                market_info  = market,
                window_start = window_start,
                window_close = window_close,
                key_id       = key_id,
                private_key  = private_key,
            )
            collector.start()

            if not collector.wait_until_ready(timeout=10):
                print("Snapshot didn't arrive in 10s — skipping window.")
                collector.stop()
                previous_ticker = market["ticker"]
                continue

            # ── 3. Wire up the executor ─────────────────────────────────────
            executor = Executor(
                ticker      = market["ticker"],
                key_id      = key_id,
                private_key = private_key,
                entry_rule  = entry_rule,
                exit_rule   = exit_rule,
                dry_run     = DRY_RUN,
            )

            # ── 4. Tick loop until window close (minus flatten buffer) ──────
            flatten_at = min(
                window_close.timestamp() - FLATTEN_SECONDS_BEFORE_CLOSE,
                run_until,
            )
            last_ready_snap = None
            while time.time() < flatten_at:
                snap = collector.get_snapshot()
                if snap["ready"]:
                    last_ready_snap = snap
                    # Record full snapshot unconditionally — feeds the vol buffer
                    # and writes raw data for all ticks regardless of EV/depth.
                    if logger:
                        logger.record_tick(snap, market["ticker"])
                    # Inject cash_balance once; logger uses it for Kelly sizing,
                    # try_trade re-injects via its own cached call (no extra API hit).
                    snap["cash_balance"] = executor.get_cash_balance()
                    if logger:
                        logger.log_tick(snap, market["ticker"])
                    executor.try_trade(snap)
                time.sleep(TICK_INTERVAL_SECONDS)

            # ── 5. End-of-window cleanup ────────────────────────────────────
            if not executor.position.is_flat:
                print(f"[MAIN] Flattening {executor.position.count} "
                      f"{executor.position.side} contracts before close.")
                executor.force_flatten(reason="window close")

            # Finalize window: attach resolution + realized_vol, then update the
            # VOLATILITY estimate using EWMA(span=5) of recent realized vols.
            if logger:
                final_btc    = last_ready_snap["coinbase_price"] if last_ready_snap else 0.0
                realized_vol = logger.finalize_window(
                    final_btc, market["threshold"], market["ticker"])

                if realized_vol > 0:
                    realized_vols_history.append(realized_vol)
                    history  = realized_vols_history[-5:]   # at most last 5 periods
                    ewma_vol = float(
                        pd.Series(history).ewm(span=5, adjust=True).mean().iloc[-1]
                    )
                    strategy_rules.VOLATILITY = ewma_vol
                    print(f"[VOL] EWMA({len(history)}) -> {ewma_vol:.2e}  "
                          f"(realized={realized_vol:.2e}"
                          f"  history={[f'{v:.2e}' for v in history]})")

            collector.stop()
            windows_traded += 1
            previous_ticker = market["ticker"]

            _btc       = last_ready_snap["coinbase_price"] if last_ready_snap else 0.0
            _s0        = market["threshold"]
            _res       = "YES" if _btc > _s0 else "NO "
            _local_ts  = datetime.now().strftime("%a %b %d  %I:%M %p")
            _ewma      = f"{strategy_rules.VOLATILITY:.2e}" if strategy_rules.VOLATILITY else "n/a"
            print(f"[WIN {windows_traded:>4}]  {_local_ts}  |  {market['ticker']}  "
                  f"|  BTC ${_btc:,.0f} vs ${_s0:,.0f}  =>  {_res}  |  ewma_vol={_ewma}")
            print(f"--- Window complete. State: "
                  f"{executor.snapshot_state()} ---\n")

        # Write any rows that didn't fill a complete 4-window batch
        if logger:
            logger.flush_remaining()

        print(f"\nSession complete. Traded {windows_traded} window(s).")
        _fire_toast("Kalshi BTC Runner", f"Session complete — {windows_traded} windows traded")

    except Exception as exc:
        _crashed = True
        ts  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        msg = f"{type(exc).__name__}: {exc}"
        print(f"\n[STOPPED {ts}] CRASH: {msg}")
        _fire_toast("Kalshi BTC Runner CRASHED", msg[:120])
        raise

    finally:
        ts     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        status = "CRASH" if _crashed else "NORMAL EXIT"
        print(f"[STOPPED {ts}] {status} — windows_traded={windows_traded}")


if __name__ == "__main__":
    main()