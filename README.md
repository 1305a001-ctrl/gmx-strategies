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

## Hard gates

- LIVE_ENABLED defaults False
- LIVE_STRATEGIES_CONFIRMED required (CSV: funding_arb)
- Per-position cap starts at $5k
- Max 3 concurrent positions until proven
- Trading wallet keys NEVER in Claude sessions
- v0.3 runtime is paper-only; no live web3 calls, no live CEX calls
