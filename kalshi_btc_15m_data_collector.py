

# ── Imports ───────────────────────────────────────────────────────────────────

import os
import time
import json
import csv
import glob
import base64
import threading
import requests
import websocket
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


# ── Constants ─────────────────────────────────────────────────────────────────

KALSHI_REST_BASE   = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_WS_URL      = "wss://api.elections.kalshi.com/trade-api/ws/v2"
COINBASE_WS_URL    = "wss://advanced-trade-ws.coinbase.com"
SERIES_TICKER      = "KXBTC15M"
WINDOW_MINUTES     = 15
OB_SEGMENT_MINUTES = 5


# ── RSA Authentication ────────────────────────────────────────────────────────

def load_private_key(key_path):
    with open(key_path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)


def build_auth_headers(key_id, private_key, method, path):
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
    }


def build_ws_auth_headers(key_id, private_key):
    return build_auth_headers(key_id, private_key, "GET", "/trade-api/ws/v2")


# ── Market Discovery ──────────────────────────────────────────────────────────

def get_current_window_times():
    now          = datetime.now(timezone.utc)
    window_min   = (now.minute // WINDOW_MINUTES) * WINDOW_MINUTES
    window_start = now.replace(minute=window_min, second=0, microsecond=0)
    window_close = window_start + timedelta(minutes=WINDOW_MINUTES)
    mins_left    = (window_close - now).total_seconds() / 60
    print(f"Window : {window_start.isoformat()} -> {window_close.isoformat()}")
    print(f"         ({mins_left:.1f} min remaining)")
    return window_start, window_close


def get_active_btc15m_market(window_close, exclude_ticker=None, max_retries=20):
    """
    Discovers the active KXBTC15M market by querying the API directly.
    Never guesses the ticker format.

    exclude_ticker: the previous window's ticker. If the API returns this
    ticker, the new market is not open yet — retry after 3 seconds.
    max_retries: 20 attempts * 3 seconds = up to 60 seconds of waiting.
    """
    url          = f"{KALSHI_REST_BASE}/markets"
    params       = {"series_ticker": SERIES_TICKER, "status": "open", "limit": 20}
    target_close = window_close.strftime("%Y-%m-%dT%H:%M:%SZ")

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=10)
        except requests.RequestException as e:
            print(f"Request error: {e} — retrying in 3s...")
            time.sleep(3)
            continue

        if response.status_code != 200:
            print(f"Error fetching markets: {response.status_code} — retrying in 3s...")
            time.sleep(3)
            continue

        markets = response.json().get("markets", [])

        for m in markets:
            if m.get("close_time", "") == target_close:
                if m.get("ticker") != exclude_ticker:
                    print(f"Found market : {m['ticker']}")
                    print(f"Title        : {m.get('title')}")
                    return m

        print(f"New market not open yet "
              f"(attempt {attempt + 1}/{max_retries}) — retrying in 3s...")
        time.sleep(3)

    print("Max retries reached — could not find new market.")
    return None


def normalize_market(raw):
    threshold_raw = (
        raw.get("floor_strike") or
        raw.get("open_price")   or
        raw.get("strike_price") or
        None
    )
    return {
        "ticker":     raw.get("ticker"),
        "title":      raw.get("title"),
        "status":     raw.get("status"),
        "threshold":  float(threshold_raw) if threshold_raw is not None else None,
        "yes_bid":    raw.get("yes_bid_dollars"),
        "no_bid":     raw.get("no_bid_dollars"),
        "close_time": raw.get("close_time"),
    }


# ── Orderbook Reconstruction ──────────────────────────────────────────────────

def reconstruct_orderbook(ts, data_dir="data"):
    """
    Merges all segment CSVs for a window into a single reconstructed file.

    Adds explicit NO contract columns (best_no_bid, best_no_ask, mid_no,
    spread_no) which are the mirror of the YES columns.

    No filters are applied — every row from every segment is included.
    """
    pattern   = os.path.join(data_dir, f"orderbook_{ts}_seg*.csv")
    seg_files = sorted(glob.glob(pattern))

    if not seg_files:
        print(f"No segment files found for ts={ts}")
        return None

    out_file = os.path.join(data_dir, f"orderbook_{ts}_reconstructed.csv")

    headers = [
        "timestamp_utc", "unix_ms", "event_type",
        "best_yes_bid", "best_yes_ask", "mid_yes", "spread_yes",
        "best_no_bid",  "best_no_ask",  "mid_no",  "spread_no",
        "yes_depth_3",  "no_depth_3",
    ]

    total = 0
    with open(out_file, "w", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=headers)
        writer.writeheader()

        for seg_path in seg_files:
            with open(seg_path, "r", newline="") as in_f:
                reader = csv.DictReader(in_f)
                for row in reader:
                    best_yes_bid = float(row["best_yes_bid"])
                    best_no_bid  = float(row["best_no_bid"])
                    best_yes_ask = float(row["best_yes_ask"])
                    best_no_ask  = float(row["best_no_ask"])

                    writer.writerow({
                        "timestamp_utc": row["timestamp_utc"],
                        "unix_ms":       row["unix_ms"],
                        "event_type":    row["event_type"],
                        "best_yes_bid":  best_yes_bid,
                        "best_yes_ask":  best_yes_ask,
                        "mid_yes":       round((best_yes_bid + best_yes_ask) / 2, 4),
                        "spread_yes":    round(best_yes_ask - best_yes_bid, 4),
                        "best_no_bid":   best_no_bid,
                        "best_no_ask":   best_no_ask,
                        "mid_no":        round((best_no_bid + best_no_ask) / 2, 4),
                        "spread_no":     round(best_no_ask - best_no_bid, 4),
                        "yes_depth_3":   row["yes_depth_3"],
                        "no_depth_3":    row["no_depth_3"],
                    })
                    total += 1

    print(f"Reconstructed {total:,} rows -> {out_file}")
    return out_file


def reconstruct_all_windows(data_dir="data"):
    seg_files  = glob.glob(os.path.join(data_dir, "orderbook_*_seg*.csv"))
    timestamps = set()
    for path in seg_files:
        fname = os.path.basename(path)
        parts = fname.replace(".csv", "").split("_")
        if len(parts) >= 2:
            timestamps.add(parts[1])

    if not timestamps:
        print("No segment files found to reconstruct.")
        return

    print(f"\nReconstructing orderbook for {len(timestamps)} window(s)...")
    for ts in sorted(timestamps):
        recon_path = os.path.join(data_dir, f"orderbook_{ts}_reconstructed.csv")
        if os.path.exists(recon_path):
            print(f"  {ts} — already reconstructed, skipping.")
        else:
            reconstruct_orderbook(ts, data_dir)


# ── Data Collector ────────────────────────────────────────────────────────────

class KalshiBTC15mCollector:
    """
    Three data streams per 15-minute window:

    Stream 1 — Kalshi WebSocket (orderbook_delta)
        Receives full snapshot on subscribe, then incremental deltas.
        Every state change is written to the current segment file.
        No validity filters — all rows are saved.

    Stream 2 — Coinbase Advanced Trade ticker
        Sub-second BTC-USD trade ticks for volatility estimation.

    Threshold file
        Written once at init from the market's floor_strike field.

    Skip counters
    -------------
    _skipped_no_yes  : _save_ob_row called but yes_bids dict was empty
    _skipped_no_no   : _save_ob_row called but no_bids dict was empty
    These are printed at stop() so you can see if rows are being lost.
    In a healthy stream both should be very small (only the first few
    deltas before the snapshot arrives).
    """

    def __init__(self, market_info, window_start, window_close,
                 key_id, private_key, data_dir="data"):

        self.market_info  = market_info
        self.ticker       = market_info["ticker"]
        self.threshold    = market_info["threshold"]
        self.window_start = window_start
        self.window_close = window_close
        self.key_id       = key_id
        self.private_key  = private_key

        # Orderbook state
        # key: price string  value: float size
        self.yes_bids = {}
        self.no_bids  = {}

        # WebSocket objects
        self._kalshi_ws           = None
        self._coinbase_ws         = None
        self._kalshi_closed_event  = threading.Event()
        self._coinbase_closed_event = threading.Event()

        # Counters
        self.ob_count        = 0
        self.coinbase_count  = 0
        self._skipped_no_yes = 0   # rows skipped because yes_bids was empty
        self._skipped_no_no  = 0   # rows skipped because no_bids was empty
        self.running         = False

        self.ts       = int(window_start.timestamp())
        self.data_dir = data_dir
        self._ob_segments_created = set()
        os.makedirs(data_dir, exist_ok=True)

        # ── threshold.csv ─────────────────────────────────────────────────────
        self.threshold_file = os.path.join(data_dir, f"threshold_{self.ts}.csv")
        with open(self.threshold_file, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "recorded_at_utc", "threshold_price",
                "window_start", "window_close", "ticker",
            ])
            w.writerow([
                datetime.now(timezone.utc).isoformat(),
                self.threshold,
                window_start.isoformat(),
                window_close.isoformat(),
                self.ticker,
            ])

        # ── btc_coinbase.csv ──────────────────────────────────────────────────
        self.coinbase_file = os.path.join(data_dir, f"btc_coinbase_{self.ts}.csv")
        with open(self.coinbase_file, "w", newline="") as f:
            csv.writer(f).writerow([
                "received_at_utc", "trade_time", "price", "volume_24h",
            ])

        thresh_str = f"${self.threshold:,.2f}" if self.threshold else "NOT FOUND"
        print(f"Threshold : {thresh_str}")
        print(f"Files     : threshold_{self.ts}.csv")
        print(f"            btc_coinbase_{self.ts}.csv")
        print(f"            orderbook_{self.ts}_seg1/2/3.csv")

    # ── Segment file management ───────────────────────────────────────────────

    def _get_ob_file(self):
        """
        Returns the path to the current 5-minute segment file.
        Creates the file with a header row the first time each segment opens.
        """
        elapsed     = (datetime.now(timezone.utc) - self.window_start
                       ).total_seconds() / 60
        seg         = int(elapsed // OB_SEGMENT_MINUTES) + 1
        seg         = max(1, min(seg, WINDOW_MINUTES // OB_SEGMENT_MINUTES))
        path        = os.path.join(
            self.data_dir, f"orderbook_{self.ts}_seg{seg}.csv"
        )
        if seg not in self._ob_segments_created:
            with open(path, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp_utc", "unix_ms", "event_type",
                    "best_yes_bid", "best_no_bid",
                    "best_yes_ask", "best_no_ask",
                    "mid_yes", "spread_yes",
                    "yes_depth_3", "no_depth_3",
                ])
            self._ob_segments_created.add(seg)
            print(f"[OB] Opened segment {seg}: "
                  f"orderbook_{self.ts}_seg{seg}.csv")
        return path

    # ── Orderbook state ───────────────────────────────────────────────────────

    def _orderbook_summary(self):
        """
        Computes derived values from the current book state.

        Returns None ONLY if yes_bids or no_bids is completely empty
        (which happens only before the snapshot arrives).
        No other filtering is applied — all price levels are valid.
        """
        if not self.yes_bids:
            self._skipped_no_yes += 1
            return None
        if not self.no_bids:
            self._skipped_no_no += 1
            return None

        yes_sorted = sorted(
            self.yes_bids.items(), key=lambda x: float(x[0]), reverse=True
        )
        no_sorted = sorted(
            self.no_bids.items(), key=lambda x: float(x[0]), reverse=True
        )

        best_yes_bid = float(yes_sorted[0][0])
        best_no_bid  = float(no_sorted[0][0])

        # Ask prices derived from reciprocal relationship
        best_yes_ask = round(1.0 - best_no_bid,  4)
        best_no_ask  = round(1.0 - best_yes_bid, 4)

        mid_yes    = round((best_yes_bid + best_yes_ask) / 2, 4)
        spread_yes = round(best_yes_ask  - best_yes_bid,     4)
        yes_depth_3 = round(sum(v for _, v in yes_sorted[:3]), 2)
        no_depth_3  = round(sum(v for _, v in no_sorted[:3]),  2)

        return {
            "best_yes_bid": round(best_yes_bid, 4),
            "best_no_bid":  round(best_no_bid,  4),
            "best_yes_ask": best_yes_ask,
            "best_no_ask":  best_no_ask,
            "mid_yes":      mid_yes,
            "spread_yes":   spread_yes,
            "yes_depth_3":  yes_depth_3,
            "no_depth_3":   no_depth_3,
        }

    def _save_ob_row(self, event_type):
        s = self._orderbook_summary()
        if s is None:
            return
        now  = datetime.now(timezone.utc)
        path = self._get_ob_file()
        with open(path, "a", newline="") as f:
            csv.writer(f).writerow([
                now.isoformat(), int(now.timestamp() * 1000), event_type,
                s["best_yes_bid"], s["best_no_bid"],
                s["best_yes_ask"], s["best_no_ask"],
                s["mid_yes"],      s["spread_yes"],
                s["yes_depth_3"],  s["no_depth_3"],
            ])
        self.ob_count += 1
        if self.ob_count % 500 == 0:
            print(f"[OB] {self.ob_count:,} rows  "
                  f"mid={s['mid_yes']}  spread={s['spread_yes']}  "
                  f"yes_depth={s['yes_depth_3']:.0f}")

    # ── Stream 1: Kalshi WebSocket ────────────────────────────────────────────

    def on_kalshi_open(self, ws):
        print(f"Kalshi WebSocket connected — subscribing to {self.ticker}")
        ws.send(json.dumps({
            "id":  1,
            "cmd": "subscribe",
            "params": {
                "channels":       ["orderbook_delta"],
                "market_tickers": [self.ticker],
            },
        }))

    def on_kalshi_message(self, ws, message):
        if not self.running:
            return
        try:
            msg = json.loads(message)
        except json.JSONDecodeError:
            return

        msg_type = msg.get("type", "")

        if msg_type == "orderbook_snapshot":
            # Full book received — reset and repopulate
            snap = msg.get("msg", {})
            self.yes_bids = {}
            self.no_bids  = {}
            for price, size in snap.get("yes_dollars_fp", []):
                if float(size) > 0:
                    self.yes_bids[str(price)] = float(size)
            for price, size in snap.get("no_dollars_fp", []):
                if float(size) > 0:
                    self.no_bids[str(price)] = float(size)
            self._save_ob_row("snapshot")
            print(f"[OB] Snapshot received — "
                  f"{len(self.yes_bids)} YES levels, "
                  f"{len(self.no_bids)} NO levels")

        elif msg_type == "orderbook_delta":
            # Single level update:
            #   price_dollars : price level being updated
            #   delta_fp      : signed change in size (+ add, - remove)
            #   side          : "yes" or "no"
            delta    = msg.get("msg", {})
            price    = delta.get("price_dollars")
            delta_fp = float(delta.get("delta_fp", 0))
            side     = delta.get("side", "")

            if price is None:
                return

            target = self.yes_bids if side == "yes" else self.no_bids

            if price in target:
                new_size = target[price] + delta_fp
                if new_size <= 0:
                    del target[price]
                else:
                    target[price] = new_size
            elif delta_fp > 0:
                target[price] = delta_fp

            self._save_ob_row("delta")

    def on_kalshi_error(self, ws, error):
        print(f"[KL ERROR] {error}")

    def on_kalshi_close(self, ws, code, msg):
        print(f"[KL CLOSED] code={code}")
        self._kalshi_closed_event.set()

    # ── Stream 2: Coinbase ticker ─────────────────────────────────────────────

    def on_coinbase_open(self, ws):
        print("Coinbase ticker stream connected.")
        ws.send(json.dumps({
            "type":        "subscribe",
            "product_ids": ["BTC-USD"],
            "channel":     "ticker",
        }))

    def on_coinbase_message(self, ws, message):
        if not self.running:
            return
        try:
            msg = json.loads(message)
        except json.JSONDecodeError:
            return
        if msg.get("channel") != "ticker":
            return
        for event in msg.get("events", []):
            for ticker in event.get("tickers", []):
                if ticker.get("product_id") != "BTC-USD":
                    continue
                price      = float(ticker["price"])
                volume_24h = float(ticker.get("volume_24_h", 0))
                trade_time = ticker.get("time", "")
                self.coinbase_count += 1
                now = datetime.now(timezone.utc)
                with open(self.coinbase_file, "a", newline="") as f:
                    csv.writer(f).writerow([
                        now.isoformat(), trade_time, price, volume_24h,
                    ])

    def on_coinbase_error(self, ws, error):
        print(f"[CB ERROR] {error}")

    def on_coinbase_close(self, ws, code, msg):
        print(f"[CB CLOSED] code={code}")
        self._coinbase_closed_event.set()

    # ── Start / stop ──────────────────────────────────────────────────────────

    def start(self):
        self.running = True
        self._kalshi_closed_event.clear()
        self._coinbase_closed_event.clear()

        # Auth headers built fresh immediately before connecting
        auth_headers = build_ws_auth_headers(self.key_id, self.private_key)

        self._kalshi_ws = websocket.WebSocketApp(
            KALSHI_WS_URL,
            header=auth_headers,
            on_open=self.on_kalshi_open,
            on_message=self.on_kalshi_message,
            on_error=self.on_kalshi_error,
            on_close=self.on_kalshi_close,
        )
        threading.Thread(
            target=self._kalshi_ws.run_forever, daemon=True
        ).start()

        self._coinbase_ws = websocket.WebSocketApp(
            COINBASE_WS_URL,
            on_open=self.on_coinbase_open,
            on_message=self.on_coinbase_message,
            on_error=self.on_coinbase_error,
            on_close=self.on_coinbase_close,
        )
        threading.Thread(
            target=self._coinbase_ws.run_forever, daemon=True
        ).start()

        print(f"\nCollecting: {self.market_info['title']}\n")

    def stop(self):
        """
        Sets running=False first so in-flight message handlers stop writing,
        then closes both streams in parallel and waits for both close events.
        """
        self.running = False

        def close_kalshi():
            if self._kalshi_ws:
                try:
                    self._kalshi_ws.close()
                except Exception:
                    pass

        def close_coinbase():
            if self._coinbase_ws:
                try:
                    self._coinbase_ws.close()
                except Exception:
                    pass

        t1 = threading.Thread(target=close_kalshi,   daemon=True)
        t2 = threading.Thread(target=close_coinbase,  daemon=True)
        t1.start()
        t2.start()

        kalshi_ok   = self._kalshi_closed_event.wait(timeout=5)
        coinbase_ok = self._coinbase_closed_event.wait(timeout=5)

        if not kalshi_ok:
            print("[WARN] Kalshi stream did not confirm close within 5s")
        if not coinbase_ok:
            print("[WARN] Coinbase stream did not confirm close within 5s")

        print(
            f"\nCollector stopped."
            f"\n  OB rows written : {self.ob_count:,}"
            f"\n  Coinbase trades : {self.coinbase_count:,}"
            f"\n  Skipped (no YES): {self._skipped_no_yes}"
            f"\n  Skipped (no NO) : {self._skipped_no_no}"
            f"\n  Threshold       : "
            + (f"${self.threshold:,.2f}" if self.threshold else "NOT FOUND")
            + f"\n  OB segments     : {sorted(self._ob_segments_created)}"
        )

        return self.ts


# ── Main Loop ─────────────────────────────────────────────────────────────────

RUN_MINUTES = 15





















if __name__ == "__main__":

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
    print(f"Credentials loaded from {dotenv_path}")

    run_until         = time.time() + (RUN_MINUTES * 60)
    collected_windows = []
    previous_ticker   = None

    while time.time() < run_until:

        window_start, window_close = get_current_window_times()

        raw_market = get_active_btc15m_market(
            window_close,
            exclude_ticker=previous_ticker,
        )

        if not raw_market:
            print("Could not find new market — retrying in 10 seconds...")
            time.sleep(10)
            continue

        market = normalize_market(raw_market)

        if market["threshold"] is None:
            print("Threshold not yet set — retrying in 10 seconds...")
            time.sleep(10)
            continue

        collector = KalshiBTC15mCollector(
            market_info  = market,
            window_start = window_start,
            window_close = window_close,
            key_id       = key_id,
            private_key  = private_key,
            data_dir     = "data",
        )
        collector.start()

        sleep_until       = min(window_close.timestamp(), run_until)
        seconds_remaining = sleep_until - time.time()
        if seconds_remaining > 0:
            time.sleep(seconds_remaining)

        ts = collector.stop()
        collected_windows.append(ts)
        previous_ticker = market["ticker"]
        print("\n--- Window closed. Rolling to next window ---\n")

    print(f"\nData collection complete after {RUN_MINUTES} minutes.")
    print(f"Collected {len(collected_windows)} window(s): {collected_windows}")
    reconstruct_all_windows(data_dir="data")