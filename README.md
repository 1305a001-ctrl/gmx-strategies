# gmx-strategies

GMX V2 strategies.

## v0.2 — funding-arb only

Liquidation-triggering removed in v0.2. Architecture audit (2026-05-18) revealed
GMX V2 `LiquidationHandler.executeLiquidation` is permissioned via the
`onlyLiquidationKeeper` Timelock role — non-keeper callers revert.
Liquidation fees flow to GM-pool LPs, not to the calling address.
The "Data Streams edge" claim was a misread of the V2 role model.

This release retains funding-rate arbitrage (delta-neutral yield on GMX V2 perps
vs centralized-venue perps), which is structurally sound.

Full audit available at `memory/arch_gmx_v2_audit.md` in operator's personal memory.

## Sub-strategies

| Sub-strategy                     | Status                               |
| -------------------------------- | ------------------------------------ |
| Funding rate arb (delta-neutral) | pure helpers + 8 tests (v0.2)        |
| Liquidation triggering           | REMOVED in v0.2 (architecture audit) |

## Architecture

```
chainlink-streams (Go) -> Redis chainlink:{btc,eth,sol,wsteth}:latest
                          |
                          +-> gmx-strategies (Python)
                                |
                                +- funding_arb.py (delta-neutral yield)
                                |
                                +- markets.py (GMX V2 market metadata)
```

## What's wired (v0.2)

- `funding_arb.py` — pure: `imbalance_ratio`, `annualized_yield_pct`, `detect_signal`. Returns the side (long_gmx_short_cex or short_gmx_long_cex) that earns the funding payment.
- `markets.py` — GMX V2 market metadata (Arbitrum + Avalanche) with alias -> market address + collateral token lookup.
- `settings.py` — pydantic config, paper-mode default, live-mode gates.
- `main.py` — no-op stub. The funding-arb runtime (subgraph polling + CEX hedge leg) is not yet wired; the stub logs `funding_arb.v02_runtime_not_wired_yet` every 60s.

## What's TODO (v0.3+)

- **GMX V2 reader contract integration** — fetch OI + funding rates per market via web3.
- **Position discovery** via Goldsky GMX subgraph.
- **CEX hedge integration** — Binance / OKX spot for the funding-arb hedge leg.
- **Paper-trade harness** — eval log + scoring (same shape as liquidation-bot).

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
