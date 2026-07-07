# poly-market-maker — backtesting and paper trading for prediction-market market making

A backtesting and paper-trading system for market making on Polymarket
prediction markets. It replays (or receives live) Polymarket CLOB WebSocket
data, quotes a logit-space Avellaneda–Stoikov strategy, judges fills with a
conservative queue-position simulator, and decomposes PnL into spread capture,
inventory, and adverse selection — with the decomposition reconciled against
independent cash/inventory accounting as an exact identity. A live
paper-trading loop runs the same engine against the real-time feed with
virtual orders only; there is no order-routing code anywhere in the added
modules (enforced by test).

<!-- TODO: embed a representative PnL decomposition table once a suitable tape
is committed. The only tape currently in the repo (smoke_test.jsonl, a
3.8-minute connectivity smoke run with a single trade print) produces 0 fills
under the conservative fill model, so there is nothing representative to show.
Needed: a multi-hour recorded session with regular trade prints (e.g. the
log_btc-updown-5m sample referenced in ASSUMPTIONS.md, or any tape written by
paper_trading/). Numbers quoted below reference ASSUMPTIONS.md. -->

## Attribution

This repository is a fork of
[Polymarket/poly-market-maker](https://github.com/Polymarket/poly-market-maker),
Polymarket's CLOB market-maker keeper. The upstream keeper framework —
`poly_market_maker/` (order/orderbook/lifecycle, AMM and Bands strategies,
CLOB/chain connectivity), `tests/`, `config/`, `docs/`, and the Docker/install
scripts — is their work and is left untouched: nothing below imports from or
modifies `poly_market_maker/`.

Original work in this fork:

| Component | Files |
|---|---|
| Market-data loader / book reconstruction | `data_loader.py` |
| Queue-aware fill simulator | `fill_simulator.py` |
| Logit-space Avellaneda–Stoikov strategy + backtest loop | `strategy.py` |
| PnL decomposition and diagnostics | `analytics.py` |
| Live paper trader (virtual orders) | `paper_trading/paper_trader.py` |
| Market screener | `paper_trading/select_markets.py` |
| Multi-market dashboard (work in progress) | `paper_trading/cockpit/` |
| Assumptions log | `ASSUMPTIONS.md` |
| Test suite (38 engine tests + 14 dashboard tests) | `test_*.py`, `paper_trading/test_paper_trader.py`, `paper_trading/cockpit/test_cockpit.py` |

## Why logit space

Standard Avellaneda–Stoikov derives its reservation price and optimal spread
for an unbounded diffusion mid-price, but prediction-market prices are
probabilities confined to [0, 1], where that assumption fails at the
boundaries. The strategy therefore quotes in logit space: the mid is mapped to
`x = ln(p / (1 − p))`, which is approximately diffusive and unbounded, and
volatility is estimated as the rolling sample standard deviation of
logit-mid returns (`sigma_window` events, floored). Reservation price and
half-spread are computed in that space — `r = x − q·γ·σ²·H` and
`δ = γ·σ²·H / 2 + (1/γ)·ln(1 + γ/κ)` — so the inventory skew and the spread
share one consistent volatility measure. Both quotes are mapped back through
the sigmoid, which lands in (0, 1) by construction, then rounded outward to
the tick grid (bids floored, asks ceiled) with `bid < ask` enforced. The
horizon `H` defaults to a constant `τ` (infinite-horizon approximation); a
terminal countdown to market expiry exists as a config hook but is not the
validated default, because sparse near-expiry data makes the countdown blow
up the spread. See `strategy.py` and ASSUMPTIONS.md §P2.2.

## Architecture

```
recorded tape (JSONL)  ─┐
                        ├─> data_loader ──> strategy (A-S quotes, requote gate)
live WS feed ───────────┘        │                    │
                                 v                    v
                          fill_simulator <── virtual orders
                                 │
                                 v
                          analytics (PnL decomposition, sweeps)
```

**Backtest engine** (`data_loader.py`, `strategy.py::run_backtest`).
Replays a recorded WebSocket tape event by event, ordered strictly by recorder
receive time (`local_timestamp`) — the exchange timestamp is kept for
diagnostics only, never for ordering, to avoid look-ahead. Book state is
mutated before each event is yielded. Quoting uses cancel/replace gated on the
target price moving at least `requote_tick_threshold` ticks (requoting every
event would reset queue position and suppress nearly all fills).

**Fill simulator** (`fill_simulator.py`). Deliberately pessimistic: every
ambiguous modelling choice under-fills rather than over-fills.

- Fills occur only on observed trade prints (`last_trade_price`) on the
  opposite aggressor side at the order's exact price level. "Price touched my
  level" never fills; book-size reductions never fill (they may be cancels).
- Queue position: the order joins behind all resting size at its level. Since
  the data cannot show whether a reduction happened ahead of or behind us,
  reductions are assumed to happen behind us first — expressed as the clamp
  invariant `queue_ahead := min(queue_ahead, level_size)`, enforced with
  runtime asserts. Trades consume the queue ahead before any volume reaches
  the order; partial fills fall out naturally.
- Adverse selection is measured per fill over a parameterised window
  (default 1.0 s): did the mid move against the resulting position? The label
  is tri-state — adverse / not adverse / **unmeasurable** (no valid mid after
  the window) — and unmeasurable fills are excluded from adverse rates and
  attribution rather than being counted as benign.

**PnL decomposition** (`analytics.py`). Each fill's PnL versus the final mark
is split into three components:

- `spread_capture = Σ signed_qty · (mid_at_fill − fill_price)`
- `adverse_selection_cost = −Σ signed_qty · (mid_after − mid_at_fill)` over
  measurably adverse fills
- `inventory_pnl = Σ signed_qty · (mid_final − base)` — the residual mark drift

These satisfy `spread + inventory − adverse = cash_final + inv_final · mid_final`
as an exact algebraic identity (the terms telescope per fill), and
`attribute_pnl` asserts the reconciliation against the engine's independent
cash/inventory accounting (observed error ≈ 1e-15 on the recorded sample; see
ASSUMPTIONS.md §P2.4). Diagnostics include fill ratio, queue position at fill,
adverse-window sensitivity sweeps, generic config sweeps, and per-time-bucket
decomposition that sums back to the global totals.

**Paper trading** (`paper_trading/paper_trader.py`). The same engine driven by
the live Polymarket market WebSocket: receive → rebuild book (identical parse
path as backtest) → requote → judge virtual fills → attribute PnL. The live
tape is written in the same JSONL format the backtest consumes, so every
paper session is reusable as backtest input (tested). Inventory/cash/ledger
persist across restarts; open orders and queue positions deliberately do not
(stale queue position after downtime would be optimistic). There is no
place/cancel/submit code, no signing, and no key handling in any of the added
modules — a guard test asserts the absence of an order-routing surface.

**Dashboard** (`paper_trading/cockpit/`, work in progress). An aiohttp server
orchestrating several paper traders concurrently, with a Chart.js dashboard
for live parameter adjustment (γ, κ, order size, inventory cap), per-market
best-queue/inventory/PnL telemetry, and loss-limit auto-stop.

## Testing

52 tests, standard library only, no test framework required
(`python test_data_loader.py`, etc.). The 38 engine tests cover: tape parsing
and book reconstruction, including batched multi-token frames (6); the queue
clamp invariants, exact-price fill matching, partial fills, and tri-state
adverse resolution (10); quoting logic — logit/sigmoid round-trip, reservation
= mid when flat, inventory skew sign, tick rounding and bounds, inventory-cap
side suppression, horizon switch (7); attribution — the reconciliation
identity on hand-checkable ledgers and end-to-end on the recorded sample,
adverse exclusion, time-bucket consistency (6); and the paper trader —
virtual fill cycle, heartbeat adverse resolution, state persist/restore,
tape/backtest compatibility, and the no-real-order-API guard (9). The
remaining 14 cover the dashboard layer (live parameter mutation, queue reset,
validation, multi-market isolation, loss-limit stop, and its own
no-order-routing guard).

## Assumptions and limitations

Every modelling decision is logged in [ASSUMPTIONS.md](./ASSUMPTIONS.md) with
its rationale and, where possible, verification against the recorded data.
The ones that matter most when reading results:

- **Conservative by construction**: exact-price fill matching misses sweeping
  trades and the queue clamp minimises advancement, so reported fills are a
  lower bound. Observed fill ratios are correspondingly low.
- **No market impact**: virtual orders never consume liquidity or move other
  participants; live-shadow PnL is optimistic in that dimension.
- **Single-token quoting**: the YES token only; the NO book is delta-only in
  the data (never snapshotted) and is deliberately not complement-seeded.
- κ is a config constant (too few trade prints to estimate order-flow decay);
  sweep it via `analytics.config_sweep` instead.

## Upstream keeper usage

The original keeper (AMM/Bands strategies, real order placement) is unchanged
from upstream and unrelated to the backtesting/paper-trading work above. To
run it, see the upstream repository's instructions: `./install.sh`, configure
`.env` and `config.env`, then `./run-local.sh` (or `docker compose up`).
Strategy docs are in [docs/strategies](./docs/strategies). Note that the
research code in this fork requires none of the keeper's setup — no keys, no
funds, no `.env`.
