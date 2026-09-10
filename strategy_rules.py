"""
Strategy rules — entry and exit logic for BTC 15m stat-arb.

This file is imported by the main script. The executor calls these two
functions on every orderbook update.

CONTRACT
--------
Both functions must:
  * return None when no action should be taken (this is the common case)
  * return an EntrySignal / ExitSignal when you want to trade

`snapshot` is the dict produced by KalshiLiveCollector.get_snapshot() and
passed to executor.try_trade(snapshot). Its exact shape is:

    {
        "best_yes_bid":   float,   # best YES bid (dollars) — collected
        "best_yes_ask":   float,   # best YES ask = 1 - best_no_bid — derived
        "best_no_bid":    float,   # best NO  bid (dollars) — collected
        "best_no_ask":    float,   # best NO  ask = 1 - best_yes_bid — derived
        "yes_depth_3":    float,   # sum of top-3 YES bid sizes
        "no_depth_3":     float,   # sum of top-3 NO  bid sizes
        "time_elapsed":   float,   # seconds since window_start
        "coinbase_price": float,   # latest Coinbase BTC-USD trade price
        "s0_price":       float,   # window threshold / strike (static)
        # ── extra fields included for free ──
        "cash_balance":   float,   # settled account balance in DOLLARS (injected by executor)
        "secs_to_close":  float,   # window expiry distance (seconds)
        "spread_yes":     float,   # best_yes_ask - best_yes_bid
        "ts_ms":          int,     # unix ms of this update
        "ready":          bool,    # True once orderbook + Coinbase are live
    }

`position` is the Position dataclass from executor.py:
    position.is_flat   -> bool
    position.side      -> "yes" / "no" / None
    position.count     -> int
    position.avg_entry -> float (dollars)
"""

import math
import numpy as np
from scipy.stats import norm
from executor import EntrySignal, ExitSignal, Position


# ── Strategy constants ─────────────────────────────────────────────────────────

FEE_COEFF    = 0.07    # Kalshi fee coefficient (see _kalshi_fee for full formula)
EV_THRESHOLD = 0.03    # minimum expected value required to enter a position
MIN_DEPTH    = 500.0   # minimum top-3 orderbook depth (dollars) required to trade

# Set by main_runner before trading begins (per-√second units).
# Derived from std of 0.25-second log-return bars, scaled by 1/sqrt(0.25).
VOLATILITY: float = None


# ══════════════════════════════════════════════════════════════════════════════
#                              MATH ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def calculate_p_up(s_zero, s_t, time_elapsed, volatility):
    """
    GBM probability that BTC closes above s_zero given current price s_t,
    time_elapsed seconds into the 900-second window, and volatility in
    per-√second units.
    """
    threshold = np.log(s_zero / s_t)
    time_left = 900 - time_elapsed
    if time_left <= 0:
        return 0.0
    return 1 - norm.cdf(threshold / (volatility * np.sqrt(time_left)))


def _kalshi_fee(price: float) -> float:
    """
    Kalshi market-order fee per contract, in dollars.

    fee = roundup(0.07 × P × (1 − P) × 100) / 100

    where P is the contract price in dollars and roundup rounds to the
    next cent. Fee is highest at P=0.50 (~1.75¢) and lowest near 0 or 1.
    """
    return math.ceil(FEE_COEFF * price * (1.0 - price) * 100) / 100


def calculate_ev_up(ask_up, p_up):
    """EV of buying YES at ask_up: net payoff if win minus cost if lose."""
    fee = _kalshi_fee(ask_up)
    return (1 - fee - ask_up) * p_up - ask_up * (1 - p_up)


def calculate_ev_down(bid_up, p_up):
    """EV of buying NO given best YES bid. ask_down = 1 - bid_up."""
    ask_down = 1 - bid_up
    fee = _kalshi_fee(ask_down)
    return (1 - fee - ask_down) * (1 - p_up) - ask_down * p_up


def kelly_fraction_up(ask_up, p_up):
    """Full Kelly fraction for a YES position."""
    fee = _kalshi_fee(ask_up)
    b = (1 - fee - ask_up) / ask_up
    return p_up - (1 - p_up) / b


def kelly_fraction_down(bid_up, p_up):
    """Full Kelly fraction for a NO position."""
    ask_down = 1 - bid_up
    fee = _kalshi_fee(ask_down)
    b = (1 - fee - ask_down) / ask_down
    return (1 - p_up) - p_up / b


# ── Shared sizing helper ───────────────────────────────────────────────────────

def _size_from_kelly(kelly_fraction, cash_balance, price_per_contract, depth_3) -> int:
    """
    Convert a Kelly fraction to a contract count.

    Kelly sizing:
        stake_dollars = cash_balance * kelly_fraction
        count_kelly   = stake_dollars / price_per_contract

    Depth cap: never more than 20% of the top-3 orderbook depth (avoids
    moving a thin book and signals that get stale quickly).

    Returns 0 if cash_balance is None or kelly_fraction <= 0.
    """
    if cash_balance is None or cash_balance <= 0 or kelly_fraction <= 0:
        return 0
    stake_dollars  = cash_balance * kelly_fraction
    count_kelly    = int(stake_dollars / price_per_contract)
    count_depth_cap = int(0.20 * depth_3)
    return min(count_kelly, count_depth_cap)


# ══════════════════════════════════════════════════════════════════════════════
#                                ENTRY RULE
# ══════════════════════════════════════════════════════════════════════════════

def entry_rule(snapshot, position: Position):
    """
    Called only when position.is_flat == True.

    Evaluates expected value for YES and NO sides.
    Enters the side whose EV exceeds EV_THRESHOLD (prefers YES on a tie).
    Sizes using full Kelly capped at 20% of top-3 orderbook depth.

    Return EntrySignal(side="yes"|"no", count=N, reason="...") or None.
    """
    if VOLATILITY is None or VOLATILITY <= 0:
        return None
    if snapshot.get("cash_balance") is None:
        return None

    # Crossed-book guard: a healthy book always has YES_ask + NO_ask >= 1.
    # If the sum is below 1, there is a phantom stale order distorting prices.
    if snapshot["best_yes_ask"] + snapshot["best_no_ask"] < 1.0:
        return None

    cash = snapshot["cash_balance"]

    p_up = calculate_p_up(
        snapshot["s0_price"],
        snapshot["coinbase_price"],
        snapshot["time_elapsed"],
        VOLATILITY,
    )

    ev_up   = calculate_ev_up(snapshot["best_yes_ask"], p_up)
    ev_down = calculate_ev_down(snapshot["best_yes_bid"], p_up)

    # Enter YES if its EV clears the threshold and beats the NO side
    if ev_up >= ev_down and ev_up > EV_THRESHOLD:
        if snapshot["yes_depth_3"] < MIN_DEPTH:
            return None
        kf   = kelly_fraction_up(snapshot["best_yes_ask"], p_up)
        size = _size_from_kelly(kf, cash, snapshot["best_yes_ask"], snapshot["yes_depth_3"])
        if size < 1:
            return None
        return EntrySignal(
            side="yes", count=size,
            reason=f"ev_up={ev_up:.3f} kf={kf:.3f} p_up={p_up:.3f}",
        )

    # Enter NO if its EV clears the threshold
    if ev_down > EV_THRESHOLD:
        if snapshot["no_depth_3"] < MIN_DEPTH:
            return None
        kf   = kelly_fraction_down(snapshot["best_yes_bid"], p_up)
        size = _size_from_kelly(kf, cash, snapshot["best_no_ask"], snapshot["no_depth_3"])
        if size < 1:
            return None
        return EntrySignal(
            side="no", count=size,
            reason=f"ev_down={ev_down:.3f} kf={kf:.3f} p_up={p_up:.3f}",
        )

    return None


# ══════════════════════════════════════════════════════════════════════════════
#                                 EXIT RULE
# ══════════════════════════════════════════════════════════════════════════════

def exit_rule(snapshot, position: Position):
    """
    Called only when position.is_flat == False.

    Exits when the EV of the held side falls to zero or below — i.e. when
    there is no longer positive edge in continuing to hold.
    Time-based forced exit (e.g. 5s before window close) is handled by
    main_runner via executor.force_flatten(); no need to replicate here.

    Return ExitSignal(count=N, reason="...") or None.
    """
    if VOLATILITY is None or VOLATILITY <= 0:
        return ExitSignal(position.count, reason="no volatility estimate — safety exit")

    p_up = calculate_p_up(
        snapshot["s0_price"],
        snapshot["coinbase_price"],
        snapshot["time_elapsed"],
        VOLATILITY,
    )

    if position.side == "yes":
        ev = calculate_ev_up(snapshot["best_yes_ask"], p_up)
    else:
        ev = calculate_ev_down(snapshot["best_yes_bid"], p_up)

    if ev <= 0:
        return ExitSignal(
            count=position.count,
            reason=f"ev_gone ev={ev:.3f} p_up={p_up:.3f}",
        )

    return None
