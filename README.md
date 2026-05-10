# gmx-strategies

GMX V2 strategies. Per the May 2026 strategy doc honest assessment, the genuine Data Streams edge here is **liquidation triggering**. Funding rate arb is good steady income but doesn't require Data Streams.

## Sub-strategies

| Sub-strategy | Data Streams edge? | Status |
|---|---|---|
| Liquidation triggering | ⭐ YES (real edge) | ✅ pure helpers + 11 tests |
| Funding rate arb (delta-neutral) | ❌ no (yield strategy) | ✅ pure helpers + 8 tests |
| Pool imbalance arb | minimal | ⏳ scaffold only |
| Keeper bot (execution fees) | minimal direct edge | ⏳ scaffold only |
| Commodity / equity perps | thinner competition | ⏳ scaffold only |

## Architecture

```
chainlink-streams (Go) ──→ Redis chainlink:{btc,eth,sol,wsteth}:latest
                          │
                          ├──→ gmx-strategies (Python)
                          │     │
                          │     ├─ liquidation_trigger.py (THE Data Streams edge)
                          │     ├─ funding_arb.py (delta-neutral yield)
                          │     │
                          │     ▼
                          │   Redis gmx:eval_log
```

## What's wired (v0.1)

- `liquidation_trigger.py` — pure: `liquidation_price(pos)`, `distance_to_liq_pct`, `detect_trigger`. Computes the price at which a GMX V2 position becomes liquidatable; surfaces a trigger when current oracle price crosses it.
- `funding_arb.py` — pure: `imbalance_ratio`, `annualized_yield_pct`, `detect_signal`. Returns the side (long_gmx_short_cex or short_gmx_long_cex) that earns the funding payment.
- `settings.py` — pydantic config, paper-mode default, three-gate live system.
- `main.py` — async entrypoint scaffold (logs "GMX SDK not wired" until inputs land).

## What's TODO (week 2+)

- **GMX V2 reader contract integration** — fetch positions, OI, funding rates per market.
- **Position discovery** via Goldsky GMX subgraph.
- **CEX hedge integration** — Binance / OKX spot for the funding-arb hedge leg.
- **Keeper bot** — earn execution fees on order processing (separate `executor` module).
- **Paper-trade harness** — eval log + scoring (same shape as liquidation-bot).

## Tests

```bash
pip install -e '.[dev]'
pytest -q   # 19 passing
```

## Hard gates

- LIVE_ENABLED defaults False
- LIVE_STRATEGIES_CONFIRMED required (CSV: liquidation,funding_arb,keeper)
- Per-position cap starts at $5k
- Max 3 concurrent positions until proven
- Trading wallet keys NEVER in Claude sessions
