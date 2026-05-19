# gmx-strategies

GMX V2 strategies.

## v0.3 — funding-arb runtime scaffold (paper mode only)

Liquidation-triggering was removed in v0.2. The architecture audit (2026-05-18,
see `memory/arch_gmx_v2_audit.md`) revealed GMX V2 `LiquidationHandler.executeLiquidation`
is permissioned via the `onlyLiquidationKeeper` Timelock role — non-keeper callers
revert and liquidation fees flow to GM-pool LPs, not callers. The "Data Streams
edge" claim was a misread of the V2 role model.

v0.3 wires a paper-mode runtime loop around the surviving funding-rate arb
pure helpers. Both the GMX V2 funding-rate read and the CEX hedge-leg
funding read are placeholder fetchers; live web3 / Binance integration lands
in G2/G3.

## Sub-strategies

| Sub-strategy                     | Status                                          |
| -------------------------------- | ----------------------------------------------- |
| Funding rate arb (delta-neutral) | pure helpers + paper-mode runtime + 14 tests    |
| Liquidation triggering           | REMOVED in v0.2 (architecture audit)            |

## Architecture

```
chainlink-streams (Go) -> Redis chainlink:{btc,eth,sol,...}:latest
                          |
                          +-> gmx-strategies (Python)
                                |
                                +- funding_arb.py            (pure helpers)
                                +- funding_arb_runtime.py    (paper-mode loop)
                                +- markets.py                (V2 market metadata)
                                +- main.py                   (entrypoint)
                                |
                                +-> Redis pub/sub  funding_arb:signals
                                +-> Redis stream   funding_arb:eval_log
```

## What's wired (v0.3)

- `funding_arb.py` — pure: `imbalance_ratio`, `annualized_yield_pct`, `detect_signal`.
  Returns the side (`long_gmx_short_cex` or `short_gmx_long_cex`) that earns funding.
- `funding_arb_runtime.py` — async loop. Iterates resolved Arbitrum markets,
  calls placeholder `fetch_gmx_funding` + `fetch_cex_funding`, runs
  `detect_signal`, emits to `funding_arb:signals` (pub/sub) + `funding_arb:eval_log`
  (XADD). Errors per market are caught + logged; sweep continues.
- `markets.py` — GMX V2 market metadata (Arbitrum + Avalanche).
- `settings.py` — pydantic config; new keys: `funding_arb_poll_interval_s`,
  `funding_arb_signals_channel`, `funding_arb_eval_log_stream`,
  `funding_arb_eval_log_maxlen`.
- `main.py` — invokes `run_funding_arb_runtime()`; paper mode.

## What's MOCKED in v0.3 (replace in G2/G3)

- `fetch_gmx_funding(market)` returns hard-coded `FundingState` per alias.
  G2 will swap in a web3 read against the GMX V2 Reader (or subgraph fallback).
- `fetch_cex_funding(symbol)` returns `0.0`. G3 will swap in a Binance
  `premiumIndex` call.

Both are injected through `run_funding_arb_runtime(gmx_fetcher=..., cex_fetcher=...)`
so the loop body stays untouched when the live readers land.

## v0.4 — G2: live GMX V2 Reader integration

`src/gmx_strategies/gmx_reader.py` implements `fetch_gmx_funding_live(market, chain)`
against Arbitrum mainnet. Three RPC calls per market:

1. `Reader.getMarket(DataStore, marketAddress)` — pulls Market.Props for the
   indexToken / longToken / shortToken triple.
2. `Reader.getMarketInfo(DataStore, MarketPrices, marketAddress)` — decodes
   `nextFunding.fundingFactorPerSecond` (30-decimal fixed-point) +
   `nextFunding.longsPayShorts` (sign) + `isDisabled`.
3. Four `DataStore.getUint(openInterestKey(market, collateralToken, isLong))`
   calls (long-collateral and short-collateral, twice per side) summed
   per `MarketUtils.getOpenInterest` semantics.

**Price source.** The MarketPrices struct uses prices from the operator's
`chainlink-streams` Redis topology (`chainlink:{alias}:latest`,
`benchmark_price_float64`). No external API — the on-chain read uses the
same oracle stack the rest of the trading stack already depends on.

**Switching modes.** Default is `settings.gmx_funding_source = "mock"`
(opt-in to live). Override via env:
```
GMX_FUNDING_SOURCE=live python -m gmx_strategies.main
```
LIVE_ENABLED gate untouched — the runtime still emits to `funding_arb:signals`
in paper mode; G2 just makes the signals reflect real on-chain conditions.

**Failure handling.** `fetch_gmx_funding_live` returns `None` on any failure
(disabled market, missing Streams price, RPC revert, decode failure). The
runtime wrapper raises on None so the existing `_process_market` per-market
try/except handles it uniformly — one bad market never kills the sweep.

## What's TODO (v0.3+)

- **G2** — GMX V2 Reader integration (web3 read of OI + funding rates per market). See "G2 integration shape" below for the verified Reader/DataStore addresses + function signature so the wiring doesn't burn a session on stale architecture (Polymarket lesson).
- **G3** — Binance perp funding-rate read (CEX hedge leg).
- **G4** — paper-trade harness scoring (eval log -> Sharpe / fill-quality).

All 5 intended markets (`btc,eth,sol,doge,xrp`) are now in `markets.py` with verified addresses (see commit `feat(markets)`). DOGE + XRP confirmed live on the current Reader 2026-05-20. The stale `wsteth` entry was removed in the same pass — GMX delisted that market and `Reader.getMarket()` returns a zero-struct for the old address.

## G2 integration shape (verified 2026-05-20)

When G2 wires `fetch_gmx_funding` against web3, use these:

| Item | Value | Notes |
|---|---|---|
| **Reader** | `0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789` | Current Arbitrum Reader (per `gmx-io/gmx-synthetics/deployments/arbitrum/Reader.json`). Note: this differs from the address cited in the 2026-05-18 GMX audit memo (`0xf60b…d139`) — Reader was redeployed. Always re-verify against `gmx-io/gmx-synthetics/deployments/arbitrum/Reader.json` at integration time, not from this README. |
| **DataStore** | `0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8` | Stable. Used as first param of every Reader call. |
| **Funding/OI read** | `Reader.getMarketInfo(DataStore, MarketUtils.MarketPrices[], address marketKey)` returns `ReaderUtils.MarketInfo` | One call per market. Construct `MarketPrices` from current Streams prices (index/long/short token min/max). |
| **MarketInfo fields used** | `.nextFunding.fundingFactorPerSecond` (the rate), `.nextFunding.longsPayShorts` (sign), `.isDisabled` (skip if true), `.borrowingFactorPerSecondForLongs/Shorts` (gross-vs-net math) | Convert `fundingFactorPerSecond` to `funding_rate_per_8h` for the existing `FundingState` shape: `rate_per_8h = factor_per_second * 8 * 3600 / 1e30` (GMX uses 30-decimal fixed-point for factors). |
| **OI per side** | `Reader.getOpenInterestWithPnl(DataStore, Market.Props, indexTokenPrice, isLong, maximize)` returns `int256` (PnL-adjusted) | Call twice (isLong=true/false) for `longs_oi_usd` / `shorts_oi_usd`. Or read raw via `DataStore.getUint(MarketUtils.openInterestKey(market, collateralToken, isLong))` — cheaper if PnL adjustment isn't needed. |

**Verification checklist before flipping G2 live**:
1. Re-read `gmx-io/gmx-synthetics/deployments/arbitrum/Reader.json` for the current address (don't trust this README — Reader could be redeployed again).
2. Confirm each entry in `ARBITRUM_MARKETS` returns a non-zero-struct from `Reader.getMarket(DataStore, marketAddress)` (script at `/tmp/gmx_market_verify.py` documents the call pattern).
3. Confirm `MarketInfo.isDisabled == false` for each market before emitting signals.
4. Match the funding-rate scaling against a known mark via the GMX UI to confirm the 30-decimal conversion is correct.

## Tests

```bash
pip install -e '.[dev]'
pytest -q
```

## Watchdog deploy (ai-primary)

The package ships a `python -m gmx_strategies.cli watchdog` subcommand that
runs trap-surface drift checks on the live external dependencies — read-only,
no order placement, no state mutation. See `src/gmx_strategies/watchdog.py`
for the full check list and severity definitions.

Checks today:
- **GMX V2 Reader address** vs `gmx-io/gmx-synthetics/deployments/arbitrum/Reader.json` on GitHub. Drift → CRITICAL (operator action: update `gmx_reader_address_arbitrum`).
- **GMX markets alive** — each entry in `ARBITRUM_MARKETS` against `Reader.getMarket`. Zero-struct (delist) → WARN.
- **HyperLend WHYPE oracle source** — `IAaveOracle.getSourceOfAsset(WHYPE)` vs the expected RedStone feed. Drift → CRITICAL.

Cron entry (every 30 minutes, alerts to `trap_alerts:gmx` Redis stream):

```
# Every 30 minutes — checks Reader/markets/HyperLend source, alerts to Redis on drift
*/30 * * * * /usr/bin/docker exec gmx-strategies python -m gmx_strategies.cli watchdog --emit-alerts >> /var/log/gmx-watchdog.log 2>&1
```

Exit codes: 0 = clean / WARN-only, 2 = CRITICAL drift, 3 = the watchdog itself
could not reach a source (treat as "no signal" — don't conclude no-drift).

Consume alerts:

```bash
# Tail new drift alerts since the last read (operator session)
redis-cli XREAD COUNT 100 STREAMS trap_alerts:gmx '$'
# Or replay the last 50
redis-cli XREVRANGE trap_alerts:gmx + - COUNT 50
```

Manual one-shot (no Redis publish, human-readable):

```bash
docker exec gmx-strategies python -m gmx_strategies.cli watchdog
```

## Hard gates

- LIVE_ENABLED defaults False
- LIVE_STRATEGIES_CONFIRMED required (CSV: funding_arb)
- Per-position cap starts at $5k
- Max 3 concurrent positions until proven
- Trading wallet keys NEVER in Claude sessions
- v0.3 runtime is paper-only; no live web3 calls, no live CEX calls
