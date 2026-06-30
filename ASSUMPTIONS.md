# ASSUMPTIONS — backtest module (files 1 & 2)

Every assumption made in `data_loader.py` (book reconstruction) and
`fill_simulator.py` (fill / queue logic) is listed here for review. The guiding
principle throughout is **conservatism**: when the data is ambiguous, we choose
the interpretation that *under*-fills rather than the one that produces an
optimistic fill which would not have occurred live.

All claims marked "(verified)" were checked directly against the sample log
`log_btc-updown-5m-1776351600.jsonl` (3,091 lines, single binary market).

---

## 0. Observed schema (verified)

Top-level record: `{ "local_timestamp": float, "slug": str, "data": ... }`.

`data` carries one of three `event_type`s:

| event_type | shape | meaning |
|---|---|---|
| `book` | dict, or length-1 list of dicts | full L2 snapshot for ONE token |
| `price_change` | dict with `price_changes[]` | incremental L2 updates (both tokens) |
| `last_trade_price` | dict | a trade print (both possible, only `...5616` seen) |

* `price_change[i].size` is the **new absolute size** at that price level, not a
  delta. `size == 0` removes the level. (verified)
* `price_change[i].side`: `BUY` = bid level, `SELL` = ask level. (verified)
* `last_trade_price.side` is the **taker/aggressor** side: a `BUY` print lifts
  asks (fills resting sells), a `SELL` print hits bids (fills resting buys).
  Verified by example: a `BUY` print at 0.67 while `best_ask = 0.67`.
* Two complementary tokens (YES/NO): prices mirror as `p` ↔ `1 − p`. The bot
  quotes `...5616`.

---

## 1. Time ordering (decision #1)

* **`local_timestamp` (recorder receive wall-clock) is the only ordering key.**
  Records are consumed in file order, which is non-decreasing in
  `local_timestamp` (verified). A backward step raises `ValueError` under
  `strict_ordering=True`.
* The exchange `timestamp` field is preserved (`exchange_timestamp`,
  `OrderBook.last_exchange_ts`) for diagnostics **only**. It is never used for
  ordering — exchange clock vs. receive clock diverge (up to ~27 s near market
  close in the sample), and the bot can only act on data when it arrives. Using
  exchange time could reorder events and introduce look-ahead.
* **No look-ahead:** each message's state mutation is applied *before* the event
  is yielded. The one foot-gun is that `MarketEvent.books` exposes live, mutating
  `OrderBook` objects; the consumer must read them within the loop iteration and
  not stash a reference for later. Documented on `MarketEvent`.

---

## 2. Two-token reconstruction & the NO token (decision #2)

* Both tokens are reconstructed into independent `OrderBook`s.
* **Snapshots exist for `...5616` only.** Scanned all 3,091 lines: 147 snapshots,
  every one for `...5616`; the NO token `...590` receives **zero** snapshots
  anywhere in the file (verified).
* Consequence: `...590` is **delta-only** with no absolute baseline → its book is
  **not reliable** for fill simulation. We keep the two-token code skeleton, but
  the real backtest should target the single token `...5616`.
* `OrderBook.synced` is `True` only after a snapshot. `...590` stays
  `synced=False` for the whole run.
* `parity_deviation = (YES_mid + NO_mid) − 1` is exposed as a diagnostic, but it
  returns `reliable=False` whenever either book is unsynced (always the case
  while `...590` is delta-only) or a mid is missing. **Callers must treat an
  unreliable value as untrustworthy.**
* We do **not** seed `...590` from the complement (`1 − p`) of the YES book. That
  would be an unverified assumption; left out deliberately.

---

## 3. Fill conditions (decision #3) — `fill_simulator.py`

* **Fills occur ONLY on observed trade prints (`last_trade_price`).** There is no
  "price touched my level ⇒ filled" rule. Book size reductions never, by
  themselves, fill us (a reduction may be a cancellation).
* A trade fills our order only when **both**:
  1. it is on the **opposite aggressor side** — a resting BID (BUY) is filled by
     a `SELL` print; a resting ASK (SELL) by a `BUY` print; and
  2. it prints at our **exact price level** (`trade.price == order.price`).
* Exact-price matching can **miss** fills from a sweeping trade that prints only
  at its final level → we then *under*-fill. This is the safe direction and is
  accepted deliberately. (A future refinement could match `trade.price` crossing
  our level; intentionally not done now to avoid optimism.)
* A matching trade of size `T` consumes the queue ahead first
  (`consumed = min(queue_ahead, T)`), and only the **through** volume
  `T − consumed` fills our order, up to `remaining`. **Partial fills** fall out
  of this naturally and are supported.

---

## 4. Queue-advancement invariant (decision #3, core) — `fill_simulator.py`

* `L_new` = level size **excluding our own order**. The historical book never
  contains our hypothetical order, so the recorded level size *is* "others".
* `queue_ahead` = the portion of `L_new` resting **in front of** us.
* We cannot observe whether a reduction hit in front of or behind us. We assume
  conservatively that **reductions happen behind us first**, minimising our
  advancement. This is exactly the clamp:

  ```
  queue_ahead := min(queue_ahead, L_new)     # invariant: queue_ahead <= L_new
  ```

  enforced with `assert` in `clamp_queue_ahead` (also asserts we never move
  backward). Test cases pinned in `test_fill_simulator.py`:

  * others `100 → 60`, `queue_ahead = 30` ⇒ `min(30,60) = 30` (no advance)
  * others `100 → 20`, `queue_ahead = 30` ⇒ `min(30,20) = 20` (forced advance)

* Level size **increases** never move us backward (new orders join behind us).
* **No double-counting across trade vs. book update.** A trade reduces our queue
  via the trade-consumption step; the matching `price_change` later clamps the
  book size. In the recorded data the **trade print arrives *before* its matching
  `price_change`** (verified on all sampled trades: trade at line *n*, the
  same-price `price_change` at *n+1…*). Because the queue is consumed on the
  trade and the clamp only ever *lowers* `queue_ahead`, processing order is
  trade-first then clamp, which cannot over-advance. The per-event operation
  order in `FillSimulator.process` (initialise → resolve adverse → clamp → trade)
  reflects this. **Risk to review:** if a future data source delivered the book
  decrease *before* the trade print, the clamp could pre-advance and the trade
  could then double-consume (optimistic). Flagged here for that case.

---

## 5. Adverse selection & misc

* **Adverse selection** is measured over a **parameterised** window
  `adverse_window_seconds` (constructor arg), default
  `DEFAULT_ADVERSE_WINDOW_SECONDS = 1.0` s — **not** hardcoded at any call site,
  so a sensitivity sweep is possible.
* Definition: after a fill, compare the mid `adverse_window_seconds` later
  (`local_timestamp`-based) against the mid at fill time.
  * resting BID filled (we bought) → `adverse` if mid **fell**.
  * resting ASK filled (we sold) → `adverse` if mid **rose**.
* **`adverse` is tri-state** — "unmeasurable" is kept distinct from "not
  adverse":
  * `True`  — measurable, mid moved against us.
  * `False` — measurable, mid unchanged or moved in our favour.
  * `None`  — **unmeasurable**: no valid mid was available after the window (or
    the window never elapsed within the data, or `mid_before` was missing).
    Such fills MUST be excluded from PnL / adverse attribution and are never
    counted as not-adverse. `adverse_summary()` enforces this — its
    `adverse_rate` is computed over measurable fills only and is `None` when
    nothing was measurable.
* **Measurement is resolved at the first event at/after the deadline that has a
  valid mid.** Until then the fill stays pending. The actual measurement time
  (`mid_after_local_ts`) and elapsed time from the fill (`measured_dt`) are
  recorded on each `Fill`, so an adverse-window sensitivity analysis can tell
  whether the measurement landed near the intended window or later.
* Fills still pending at end of stream are resolved by `finalize()` to
  `adverse=None` (unmeasurable). `last_mid` is accepted for backward
  compatibility but intentionally not used to force an out-of-window measurement.
* **Prices** use `decimal.Decimal` keys for exact tick comparison; **sizes** are
  floats. Float size/price comparisons use `_EPS = 1e-9`.
* **Unknown `event_type`s** are passed through without mutating book state — we
  do not guess at undocumented semantics.
* **Dependencies:** standard library only (`json`, `decimal`, `dataclasses`).
  numpy/pandas were not required.

---

## Open questions for review (do not affect files 1 & 2 correctness, but matter for files 3 & 4)

1. Should sweeping trades that print at a single price be allowed to fill our
   better-priced resting orders (relaxing the exact-price match in §3)?
2. For the NO token, is complement-seeding (`1 − p`) ever acceptable, or should
   `...590` remain strictly delta-only / out of scope?
3. Preferred adverse-selection window default and whether a "next mid change"
   (event-based) mode is wanted alongside the time-based window.

---

# Phase 2 — strategy.py & analytics.py

## P2.1 Scope of the strategy

* The single bid+ask A-S pair is a **minimal strategy to validate the attribution
  engine**, NOT a replacement for the repo's BAND/AMM strategies. Interfaces
  (`run_backtest`, the `FillRecord` ledger, every analytics function) accept N
  concurrent orders so BAND/AMM can later feed the same `FillSimulator` +
  `analytics` with no rework — feeding more fills is the only change.
* Single token `...5616` is quoted (PnL/inventory). The two-token skeleton is
  preserved (parity diagnostic) but `...590` is never complement-seeded.

## P2.2 logit-space A-S (strategy.py)

* `x = ln(p/(1-p))`, `p = mid` clipped to `[price_eps, 1-price_eps]`
  (`price_eps=1e-4`) to keep the logit finite; `mid is None` (one-sided book)
  ⇒ **skip quoting** that event. Quotes are mapped back with `sigmoid`, which is
  always in (0,1); bids floor / asks ceil to the tick grid and `bid<ask` is
  enforced (widen ask by a tick if rounding collapses them).
* **Reservation** `r = x - q·gamma·sigma^2·H` and **half-spread**
  `delta = gamma·sigma^2·H/2 + (1/gamma)·ln(1+gamma/kappa)`, both in logit space.
  `sigma` is the rolling sample std of **logit-mid returns** (`sigma_window`,
  floored at `sigma_floor`) so reservation and spread share one space — unit
  tested (`test_reservation_equals_mid_when_flat`, round-trip).
* **Horizon** `H=(T-t)`: default **constant `tau`** (infinite-horizon approx);
  `horizon_mode="terminal"` with `t_expiry` switches to a floored countdown
  (hook). Rationale: sparse near-expiry data makes a countdown blow quotes up;
  constant tau isolates the validation. Switchable + sweepable via config.
* **kappa**: constant from config (`kappa_mode="const"`). Online estimation is a
  documented extension point only — 77 trade prints are too few to fit
  fill-intensity decay stably. Sweep kappa via `analytics.config_sweep` instead.
* **Requote**: cancel/replace only when the target price moves
  `>= requote_tick_threshold` ticks (default 1). Requoting every event is avoided
  (queue resets would suppress nearly all fills). `requote_mode="interval"`
  provides a fixed-interval alternative for sweeps.
* **Inventory cap**: at `|inventory| >= max_inventory` the breaching side stops
  quoting (long ⇒ no bid, short ⇒ no ask).
* **Accounting** (initial cash/inventory = 0): BUY fill ⇒ `inv+=qty, cash-=qty·price`;
  SELL fill ⇒ `inv-=qty, cash+=qty·price`; equity = `cash + inv·mid`.

## P2.3 Adverse measurement across order lifecycle

* Adverse selection is measured by `FillSimulator` over `adverse_window_seconds`.
  To measure it correctly even after a quote is cancelled or fully filled, the
  engine **drains** such orders: it sets `order.active=False` (so they cannot
  fill again — `FillSimulator._apply_trade` early-returns) but keeps feeding them
  events until the window elapses, then finalises. This avoids marking
  recently-filled orders `adverse=None` just because the quote was replaced.
* `adverse=None` (unmeasurable) fills are **excluded** from `adverse_rate` and
  `adverse_cost` everywhere (`adverse_diagnostics`, attribution).

## P2.4 PnL attribution & reconciliation (analytics.py) — most important

With `signed_qty = +qty` (BUY) / `-qty` (SELL), `mid_fill = mid_before` (or
`fill_price` if `mid_before is None`), `mid_after` measured over the window:

* `spread_capture = Σ signed_qty·(mid_fill - fill_price)`
* `adverse_selection_cost = -Σ_{adverse==True, measurable} signed_qty·(mid_after - mid_fill)`
* `inventory_pnl = Σ signed_qty·(mid_final - base)`, `base = mid_after` for
  adverse&measurable fills else `mid_fill`.

**Identity** `spread_capture + inventory_pnl - adverse_selection_cost = total_pnl`
where `total_pnl = cash_final + inv_final·mid_final`. Proof: per fill the three
terms telescope to `signed_qty·(mid_final - fill_price)`, whose sum is exactly
`cash_final + inv_final·mid_final`. So this is an exact algebraic identity (not an
approximation); `attribute_pnl` asserts `|decomposed - total| <= tol`. Verified
on the real sample at machine precision (error ≈ 1e-15) with all three components
non-trivial.

* **Sign note**: `spread_capture` uses `(mid_fill - fill_price)` (positive =
  edge captured), the negation of the literal `(fill_price - mid)`, so the
  three-term identity closes with the correct sign. Confirmed with the user.
* `mid_before is None` fills contribute 0 spread and fall entirely into
  inventory (count reported as `n_mid_before_missing`); the identity still holds.
* `mid_final` is the last valid mid (book may be one-sided at expiry).

## P2.5 Diagnostics

* fill ratio = filled_qty / quoted_qty; average `queue_ahead_before`; inventory
  max/min/final; **adverse window sweep** (0.5/1.0/2.0 s — re-runs the engine
  because adverse labels depend on the window); generic `config_sweep` over any
  config field (kappa, gamma, requote interval, …); per-time-bucket decomposition
  (bucket sums equal the global totals); `parity_summary` (flagged
  `reliable=False` throughout — NO token is delta-only).
* **Observed fill ratio is low** under the default A-S spread + exact-price
  matching (0 fills at the wide default; ~0.2 with a tighter `gamma`/`kappa`).
  This is the conservative under-fill the user accepted; revisit matching only if
  it proves unrealistically low for the intended use.

---

# Paper Trading Phase 1 — paper_trader.py

## PT.0 Safety (hard rules)

* **No real-order path.** `paper_trader.py` contains no order-routing code — no
  place/cancel/submit/sign, no private keys, no funds path. Orders exist only as
  in-memory `MyOrder` objects judged against the live tape. Enforced by a test
  (`test_no_real_order_api_present`). It is structurally impossible to move money.
* **`poly_market_maker/` is never imported or modified.** Only the root engine
  modules are reused.

## PT.1 Engine reuse & the one additive change

* The live loop reuses the validated engine unchanged: `ASMarketMaker.quotes`,
  `_should_requote`, `FillSimulator` (queue + tri-state adverse), `attribute_pnl`.
  `run_backtest` is not used; its per-event orchestration is reproduced for the
  live loop, and the virtual-order manager mirrors its active/draining logic.
* **One additive refactor** to `data_loader.py`: the per-message parse/dispatch
  was extracted into a public `build_market_event(books, msg, primary_asset=...)`
  that `iter_market_events` now calls. Behaviour and signatures are unchanged and
  all 27 engine tests still pass — this gives live and backtest a single,
  identical book-reconstruction path.

## PT.2 Connectivity (from the recorder spec)

* `WSS_URI = wss://ws-subscriptions-clob.polymarket.com/ws/market`; subscribe
  `{"type":"market","assets_ids":[token_id]}`; `ping_interval=20, ping_timeout=20`;
  exceptions trigger a reconnect loop with exponential backoff.
* Each frame is logged as `{"local_timestamp": time.time(), "slug": slug,
  "data": <frame>}` — **byte-compatible with the backtest tape**, so live logs
  are reusable as future backtest input (verified by
  `test_jsonl_tape_is_backtest_compatible`). `websockets` is imported lazily so
  the module/tests import without it installed.
* **Framing assumption**: like the recorder that produced the sample, each WS
  frame is treated as one `data` payload (dict for book/price_change/trade, list
  for a snapshot). If a live frame batches multiple distinct updates as an array,
  it would need splitting — flagged as a follow-up.

## PT.3 Real-time specifics

* **Adverse resolution timer.** Backtest resolves adverse windows on the next
  event; live quiet periods could stall this. A heartbeat (`heartbeat()` /
  `_adverse_resolver_loop`, every `adverse_resolve_interval`) feeds a no-op
  `MarketEvent` (event_type None, no trade) so the engine's `_resolve_adverse`
  runs with the current mid — no new fills, no queue change (engine logic
  unchanged, only the call is wrapped). Verified by `test_heartbeat_resolves_adverse`.
* **State persistence.** `inventory`, `cash`, the fill `ledger`, and `last_mid`
  are snapshotted atomically every `state_save_interval` and on shutdown (for the
  24/7 VM). On restart (`reload_state=True`, default) these cumulative values are
  restored so PnL continues; `--no-reload` starts fresh.
* **What is NOT restored**: open virtual orders and queue positions. After
  downtime the book has changed and any queue position is meaningless, so the
  next requote lays fresh quotes (the conservative, non-optimistic choice).
  Verified by `test_state_persist_and_restore_inv_cash_only`.

## PT.4 Modelling limitations (carried over + new)

* **Market impact is not modelled.** Virtual orders never consume real liquidity
  and never influence other participants' behaviour or the tape. Reported PnL is
  an upper-ish bound on realism in that dimension.
* Conservative **queue assumption** (reductions assumed behind us) and
  **fills only on observed trade prints at the exact price** are inherited from
  `fill_simulator.py` (no optimistic conversion).
* **Single token** (`...5616`-style YES token) is quoted; manual `--token-id`/
  `--slug` (auto-discovery deferred to a later phase).
* Success criterion for Phase 1 is **continuity** (no crash; some virtual fills;
  fills reconcile in analytics), not profit — zero/negative PnL still passes.

---

# Paper Trading Phase 1.1 — multi-token / batched-frame handling (fix)

A live Messi-market smoke run confirmed Polymarket batches both tokens (and can
batch multiple messages) per frame. The earlier "each frame is one payload, list
= snapshots" assumption (PT.2) was a real bug in the shared parser and is now
fixed in `data_loader.py` (not worked around in `paper_trader.py`), keeping the
live tape and backtest on one identical parse path.

* **`build_market_events(books, msg, primary_asset=...)`** (generator) replaces
  the old single-event parser. A frame's `data` is EITHER one message dict OR a
  list of independent message dicts; each element is dispatched by its OWN
  `event_type` (book / price_change / last_trade_price). The old "a list means
  book snapshots" branch — which silently dropped batched `price_change` and
  `last_trade_price` messages and could wipe books — is removed. Each message is
  applied immediately before its event is yielded (no intra-frame look-ahead).
  `iter_market_events` flattens this, so the consumer-facing event stream is
  unchanged; `build_market_event` (singular) is retained as a back-compat wrapper.
  Verified: all 29 engine tests pass and the sample's book-only list frames
  produce identical book state (same bid/ask counts), and the sample backtest
  reconciles bit-identically (19 fills, error ≈ 1e-15).
* **Single-token isolation in `paper_trader.handle_message`**: it now iterates
  the per-message events; **fill judging runs on every event** (the engine
  isolates by `asset_id`, so only YES orders can fill), but **quoting and the
  volatility (`observe_mid`) update are HARD-GATED to events where the YES token
  was actually touched** (`token_id in event.asset_ids`). A NO-token-only update
  therefore updates the NO book but never re-quotes and never pollutes the YES
  sigma buffer. Verified by `test_no_only_frame_does_not_requote_or_observe`,
  `test_yes_no_frame_updates_both_books_quotes_yes_only`, and
  `test_list_frame_trade_reaches_fill_engine`.
