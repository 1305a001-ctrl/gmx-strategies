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
The per-venue live gates (`live_gmx_enabled` / `live_binance_enabled`)
are not consulted by the funding-arb runtime — the runtime still emits
to `funding_arb:signals` in paper mode; G2 just makes the signals
reflect real on-chain conditions.

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

## G6 — Binance auth setup

G6 is the Binance USDT-M Futures executor for the funding-arb hedge leg. G6.1 (PR #15) reads the public `exchangeInfo` endpoint for filter caching. G6.2 (this section) adds HMAC-SHA256 signed-request auth + read-only account state + a position-mode startup gate. G6.3 will add margin-type + leverage POSTs; G6.4 will add order placement.

The audit at `memory/arch_binance_executor_audit.md` is **CONDITIONAL GO** — start against TESTNET only; mainnet capital gated on the Malaysia jurisdiction decision the operator must record separately.

### 1. Create the API key

**On Binance UI (NOT the API — see step 4 below for why)**:

1. Open the Futures account FIRST. The Futures API can't be used by a key that was created before the Futures account was opened.
2. Account → API Management → Create API.
3. Name it e.g. `g6-testnet` or `g6-mainnet` so the two never get confused.
4. Required scopes:
   - `enableReading` (always granted)
   - `enableFutures` — **YES**
   - `enableSpotAndMarginTrading` — **NO** (G6 only touches futures)
   - `enableWithdrawals` — **NO. EVER.** This is a hard rule, not a default. A G6 key with withdrawals enabled is one bug away from drained custody.
   - `enableInternalTransfer` — **NO**
5. IP restriction: **enable** with ai-primary's egress IP added to the allowlist. If your ISP is residential and the IP rotates, defer mainnet until you can pin a static-IP egress (fixed-IP plan, static-IP VPN endpoint, or DigitalOcean tunnel).

**For testnet**: the same flow at `https://demo.binance.com` produces a separate testnet key + secret. Mainnet keys do NOT authenticate against testnet and vice versa.

### 2. Store the creds in /srv/secrets

Two file conventions are supported — pick one per deploy:

**Option A: pydantic-settings secrets-dir (recommended for deploys)**:

```bash
# On ai-primary, as root:
sudo install -d -m 0700 -o benadmin -g benadmin /srv/secrets
echo -n 'your-api-key-here' | sudo tee /srv/secrets/binance_api_key
echo -n 'your-api-secret-here' | sudo tee /srv/secrets/binance_api_secret
sudo chown benadmin:benadmin /srv/secrets/binance_api_*
sudo chmod 0400 /srv/secrets/binance_api_*
```

`pydantic-settings` reads each file's content as the value for the matching field. `secrets_dir` is conditionally enabled only when `/srv/secrets/` exists — dev machines without the dir won't see warnings.

**Option B: env vars (recommended for local dev / docker)**:

```bash
export BINANCE_API_KEY='your-api-key-here'
export BINANCE_API_SECRET='your-api-secret-here'
# Optional overrides:
export BINANCE_RECV_WINDOW_MS=5000
export BINANCE_FAPI_BASE_URL=https://demo-fapi.binance.com  # testnet
```

NEVER commit. The `binance.env` style file goes under `/srv/secrets/` or stays out of git via `.gitignore`.

### 3. Position mode — operator action required

G6.2 ships `assert_one_way_position_mode()` (`src/gmx_strategies/binance_startup_check.py`). When G6.4 wires it into the executor's startup path, the executor REFUSES to run if your account is in HEDGE mode.

**Why operator-action and not an API-flip in code**: position-mode is an account-wide setting, not per-strategy. If G6 silently flipped it back to one-way every boot, an operator using the same account for a separate hedge-mode experiment would have it stomped on. The intentional UI step is the moat.

**To verify / flip**:

1. Log into Binance Futures UI (mainnet or testnet).
2. Top-right user icon → Preferences → Position Mode.
3. Confirm "One-Way Mode" is selected (NOT "Hedge Mode").
4. If you change it: the flip is only accepted when no positions are open.

If the gate ever raises `BINANCE: account is in HEDGE mode...`, that's the recovery procedure. If it raises `BINANCE: cannot verify position mode — auth issue or API down`, check creds + IP allowlist + clock drift before proceeding.

### 4. Smoke each function manually after env is set

This PR does **NOT** run any smoke against real Binance servers — exposing real credentials to the agent's working environment is unsafe. The operator runs smoke once env is set:

```python
# python -m asyncio
import asyncio
from gmx_strategies import binance_account, binance_startup_check

async def smoke() -> None:
    # 1. Verify creds reach the API at all.
    bal = await binance_account.fetch_account_balance()
    print("balance:", bal[0] if bal else "FAIL — check creds + IP allowlist")

    # 2. Verify USDT free margin pulls.
    free = await binance_account.fetch_usdt_free_margin()
    print("usdt_free:", free)

    # 3. Verify position-mode gate.
    mode = await binance_account.fetch_position_mode()
    print("hedge?", mode)  # expect False
    await binance_startup_check.assert_one_way_position_mode()  # should not raise
    print("startup check: OK")

    # 4. Verify position info reads (likely empty list on a fresh account).
    pos = await binance_account.fetch_position_information()
    print("positions:", pos)

asyncio.run(smoke())
```

Expected outputs at the testnet starting state:
- `balance`: list of asset dicts with `USDT` entry; `availableBalance` ~ 10000 USDT-T at first registration (Binance grants ~$10k testnet balance; verify by reading the field, don't hardcode).
- `usdt_free`: float matching `balance[USDT].availableBalance`.
- `hedge?`: `False` (one-way).
- `startup check: OK` with no exception.
- `positions`: `[]` (empty on a fresh account).

Ready to smoke once env is set — see steps 1–3 to provision before running.

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

- `live_gmx_enabled` defaults False (gates GMX broadcasts in `gmx_signer.submit_signed`)
- `live_binance_enabled` defaults False (gates Binance broadcasts for the future G6.4 hedge-leg path)
- Both gates are per-venue; flipping one does not enable the other
- Per-position cap starts at $5k
- Max 3 concurrent positions until proven
- Trading wallet keys NEVER in Claude sessions (loaded from file at `/srv/secrets/gmx_executor_key`, mode 0400, root-owned — see "G5.2 — Signing + submission")
- v0.3 runtime is paper-only; no live web3 calls, no live CEX calls
- Binance API key: `enableFutures` + `enableReading` ONLY. NO withdrawals. EVER. IP-allowlist required.
- Position mode MUST be one-way (G6.2 `assert_one_way_position_mode` enforces at startup once wired in G6.4).

## G6 (CEX hedge leg)

`src/gmx_strategies/binance_exchange_info.py` — the first G6 module. Reads the
public `/fapi/v1/exchangeInfo` endpoint, parses per-symbol LOT_SIZE /
MARKET_LOT_SIZE / PRICE_FILTER / MIN_NOTIONAL filters into a frozen
`SymbolInfo`, caches the result with a TTL, and exposes Decimal-backed pure
helpers (`round_qty_down`, `round_price`, `passes_min_notional`,
`quantity_from_notional`) that the downstream executor MUST go through before
submitting any order. Without these checks the first BTC hedge would reject
silently with Binance `-4164 MIN_NOTIONAL` or `-1111 PRECISION`.

**Why this is load-bearing**: per the audit
(`memory/arch_binance_executor_audit.md` §7 / H1), BTCUSDT's min_notional is
several × above the operator's $10/trade cap. Hardcoding filter values is a
trap because Binance bumps them silently during volatile episodes.

**Smoke (2026-05-20, fapi.binance.com mainnet)** — 740 symbols parsed; the
5 G6 targets:

| Symbol  | lot_step | lot_min | price_tick | min_notional |
|---------|---------:|--------:|-----------:|-------------:|
| BTCUSDT |    0.001 |   0.001 |        0.1 |          $50 |
| ETHUSDT |    0.001 |   0.001 |       0.01 |          $20 |
| SOLUSDT |     0.01 |    0.01 |       0.01 |           $5 |
| DOGEUSDT|        1 |       1 |    0.00001 |           $5 |
| XRPUSDT |      0.1 |     0.1 |     0.0001 |           $5 |

Audit had estimated BTCUSDT min_notional ≈ $100; actual on 2026-05-20 is
$50 (still 5x the $10 cap — BTC remains unusable at the current cap; G6
must either raise the per-symbol cap or drop BTC from the hedge basket).
SOL/DOGE/XRP are tradeable at $5 + cap.

## G5.2 — Signing + submission

`src/gmx_strategies/gmx_signer.py` layers on top of the G5.1 encoder. It
loads the operator's private key, signs an EIP-1559 transaction for the
`PayableMulticall(sendWnt + sendTokens + createOrder)` payload that the
encoder produces, and either dry-run-simulates it via `eth_call` or
submits it via `eth_sendRawTransaction` — gated.

**This module ships the capability but does not wire it into any runtime.**
That belongs to G7.1 (or whichever strategy first proves an EV-positive
edge worth broadcasting). The funding-arb backtest at
`memory/research_funding_arb_backtest_30d.md` killed the original thesis
on all 5 markets, so G5.2 sits as reusable executor infrastructure for a
future strategy.

### Gate matrix (`submit_signed`)

| `live_gmx_enabled` | `dry_run` | key present | account match | Broadcast? |
|--------------------|-----------|-------------|---------------|------------|
| False              | any       | any         | any           | NO (sim)   |
| True               | True      | any         | any           | NO (sim)   |
| True               | False     | False       | n/a           | RuntimeErr |
| True               | False     | True        | False         | NO (sim)   |
| True               | False     | True        | True          | YES        |

"Account match" = both `signed_tx["from"]` and `intent.account` resolve to
the EOA derived from the loaded key. Any gate failing → falls back to
`eth_call` simulation; nothing touches mempool.

### Provisioning the executor key

```bash
# On ai-primary (or whichever host runs the GMX executor):
sudo mkdir -p /srv/secrets
sudo install -m 0400 -o root -g root /dev/null /srv/secrets/gmx_executor_key
# Then paste the 64-char hex (with or without 0x prefix), one line, no trailing whitespace:
sudo nano /srv/secrets/gmx_executor_key
sudo chmod 0400 /srv/secrets/gmx_executor_key
sudo chown root:root /srv/secrets/gmx_executor_key
```

**Never commit, never log, never paste into Claude sessions.** The signer
module reads the file lazily on first use, derives the EOA via
`eth_account.Account.from_key()`, and exposes only the derived address
via `get_executor_address()`. The raw key never appears in returned dicts
or log lines.

For development (e.g. running `g5_sign_smoke` from a dev shell without
writing to `/srv/secrets`), set `GMX_EXECUTOR_KEY=<hex>` in your env. The
file always wins precedence over the env var — production deploys MUST
use the file path.

### g5_sign_smoke CLI

```bash
# Loads the key, signs a synthetic $10 SOL MarketIncrease, dry-runs via
# eth_call. NEVER broadcasts (dry_run=True is hard-coded).
python -m gmx_strategies.cli g5_sign_smoke
```

Exit codes:
- `0` — simulation OK (or known-acceptable revert; encoding + multicall
  shape + market existence are sound)
- `2` — critical-fail revert (encoding bug indicator; investigate before
  re-running)
- `3` — no key configured (see provisioning above)

### Scope (what this PR DOES NOT wire)

- No funding-arb runtime call site flips to live. The runtime stays
  paper-only until a strategy with proven positive EV asks for it.
- No risk-watcher integration on the submit path. When G7.1 lands, it
  will route via the same `submit_signed(dry_run=False)` after
  risk-watcher's gates clear; the signer does NOT know about strategy-
  level halts.
- No retry / nonce-recycle logic. Each call to `sign_order` fetches a
  fresh nonce from the RPC. Re-submitting a stale signed tx is the
  caller's job.
- The single-shot receipt poll in `submit_signed` returns whatever the
  RPC has at the time of the post-broadcast probe — typically `null`
  for a few seconds. The operator can re-poll out-of-band via the
  returned `tx_hash`.
