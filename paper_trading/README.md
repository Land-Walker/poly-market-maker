# paper_trading/

Live **paper trading** (virtual orders only — zero real orders, no funds path)
for the logit-space Avellaneda–Stoikov strategy, plus a market-picker. The
shared engine stays at the repo root and is imported, never duplicated.

## Layout & imports

```
poly-market-maker/            <- repo root (shared engine + backtest, unchanged)
  data_loader.py  fill_simulator.py  analytics.py  strategy.py
  test_*.py (engine/backtest tests)
  paper_trading/              <- this folder
    paper_trader.py           live WS paper trader (imports root engine)
    test_paper_trader.py      its tests (run from repo root)
    select_markets.py         Gamma candidate lister (standalone, stdlib only)
    outputs/                  default place for tape/state files
```

The engine modules live at the root (shared by backtest **and** paper trading),
so the two files here add a 2-line `sys.path` bootstrap to put the repo root on
the path before importing `data_loader` / `fill_simulator` / `analytics` /
`strategy`. Nothing at the root changed; `poly_market_maker/` is never touched.

## Run the paper trader

```bash
pip install websockets
python paper_trading/paper_trader.py \
    --token-id <YES_TOKEN_ID> --slug <market-slug> \
    --log paper_trading/outputs/tape.jsonl \
    --state paper_trading/outputs/state.json
# options: --gamma --kappa --order-size --max-inventory --adverse-window --no-reload
```

The tape is byte-compatible with backtest input, so a paper-trading session can
later be replayed through `data_loader` / `analytics`.

## Pick a market

```bash
python paper_trading/select_markets.py --min-vol24 10000 --min-days 14 --top 20
python paper_trading/select_markets.py --self-test      # offline logic check
```

Lists **pure-binary** (`negRisk=false`), order-book-enabled, suitably-active
markets with enough time to resolution, sorted by 24h volume. Prints slug,
question, volume24hr, liquidity, endDate, tick size, min size, and the YES/NO
`clobTokenIds`. Selection is manual (`--list-only`). Note: the highest-volume
markets are often negRisk grouped multi-outcome markets (World-Cup-winner style)
and are correctly excluded — pure-binary candidates appear further down the list.

## Tests

```bash
python paper_trading/test_paper_trader.py     # 9 paper-trader tests
# engine/backtest tests run from the root: python test_*.py
```

## Limitations

See the repo-root `ASSUMPTIONS.md` (sections "Paper Trading Phase 1" and "1.1"):
market impact not modelled, conservative queue assumption, single-token
isolation, fills only on observed trade prints at the exact price.
