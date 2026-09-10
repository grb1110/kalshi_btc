A live statistical arbitrage system for Kalshi's KXBTC15M markets (15-minute BTC up/down binary contracts). It streams live orderbook and price data, estimates the probability that BTC closes above a threshold, and sizes/executes market orders accordingly.

Status: research / paper-trading project. This has been run in dry-run mode and has not accumulated enough live trading evidence to demonstrate profitability. Treat it as an exploration of the mechanics of prediction-market stat-arb, not a proven trading system. See Status & Limitations.

How it works

1. Data collection — two WebSocket streams run in parallel:

Kalshi's orderbook feed for the active KXBTC15M market (best yes/no bid-ask)
Coinbase's BTC-USD tick feed

The relevant Kalshi market for the current window is discovered by matching series_ticker and close_time, since a new market opens every 15 minutes.

2. Probability model — BTC is modeled as a driftless geometric Brownian motion (martingale) over the remaining time in the window. Given the current spot price, the strike/threshold, and realized/assumed volatility, the model uses the log-normal CDF to estimate P(price > threshold) at expiry. The implied YES ask is derived as 1 - best_no_bid (Kalshi doesn't always quote both sides directly).

3. Position sizing — sized using fractional (quarter) Kelly against the account's live cash balance, to keep bet sizes conservative relative to the model's edge estimate and account for model uncertainty.

4. Execution — executor.py places market orders (no limit-order support currently) via Kalshi's signed REST API (RSA-PSS-SHA256 request signing). It reconciles local position state against /portfolio/positions before every order to avoid drift, throttles order frequency, and logs every attempted trade (filled or not) to a CSV.

Architecture
kalshi_live_data_collector.py       # Coinbase + Kalshi WebSocket streams, orderbook reconstruction
kalshi_btc_15m_data_collector.py    # KXBTC15M-specific market discovery & data collection
strategy_rules.py                   # entry_rule() / exit_rule() — the actual trading logic
executor.py                         # Order placement, position tracking, Kelly-ready balance API
main_runner.py                      # Wires collector -> strategy -> executor together; main entry point
launch_runner.bat                   # Windows launch script
watchdog.ps1                        # PowerShell watchdog for auto-restarting the runner
math_engine.ipynb                   # Probability model derivation (log-normal CDF / GBM math)
dry_run_analysis.ipynb              # Post-hoc analysis of dry-run trade logs
data/                                # Collected market/orderbook data

strategy_rules.py plugs into executor.py via two hook functions:

python
def entry_rule(snapshot: dict, position: Position) -> EntrySignal | None
def exit_rule(snapshot: dict, position: Position) -> ExitSignal | None

main_runner.py feeds each orderbook snapshot into executor.try_trade(snapshot), which routes to entry_rule or exit_rule depending on whether the strategy currently holds a position.

Status & Limitations
No live-trading track record — all results so far are from dry-run/paper trading.
The probability model assumes a driftless GBM with a given volatility input; real BTC returns exhibit jumps and volatility clustering that this doesn't currently capture.
Market orders only — no limit-order/cancel-replace path yet.
Position sizing depends on an accurate, up-to-date cash balance read; if the balance API call fails, the strategy is designed to skip trading rather than guess.
