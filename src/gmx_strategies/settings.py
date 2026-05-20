"""Env-driven settings (v0.2 — funding-arb only)."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# `secrets_dir` lets pydantic-settings read each field from a file under
# /srv/secrets/<field_name> (operator's convention) in addition to env +
# .env. Used for `binance_api_key` / `binance_api_secret` (G6.2). The
# files are owned by the deploy user, mode 0400; never committed.
# See README "G6 — Binance auth setup" for provisioning.
#
# We only enable the secrets-dir source when the dir actually exists —
# pydantic-settings emits a UserWarning otherwise (harmless but noisy on
# dev machines without /srv/secrets, which is most of them).
_SECRETS_DIR = "/srv/secrets"
_SECRETS_DIR_OR_NONE: str | None = _SECRETS_DIR if Path(_SECRETS_DIR).is_dir() else None


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", extra="ignore", secrets_dir=_SECRETS_DIR_OR_NONE,
    )

    redis_url: str = "redis://localhost:6379/0"

    # GMX V2 deployments — Arbitrum is the primary; Avalanche has growing
    # liquidity but lower; Solana deployment is newer.
    chains_enabled: str = "arbitrum"

    arbitrum_rpc_url: str = "https://arb1.arbitrum.io/rpc"
    avalanche_rpc_url: str = "https://api.avax.network/ext/bc/C/rpc"

    # --- GMX V2 Reader (G2 live integration) ---
    # Verified 2026-05-20 against gmx-io/gmx-synthetics/deployments/arbitrum/.
    # GMX has redeployed Reader at least once during this project's life
    # (see memory/arch_gmx_v2_audit.md addendum 2026-05-20). Re-pull
    # `deployments/arbitrum/Reader.json` at each integration milestone to
    # confirm. DataStore has been stable since GMX V2 launch.
    gmx_reader_address_arbitrum: str = "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789"
    gmx_datastore_address_arbitrum: str = "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8"
    # ExchangeRouter (Arbitrum) — verified 2026-05-20 against
    # gmx-io/gmx-synthetics/deployments/arbitrum/ExchangeRouter.json.
    # Entry point for all order ops. Send `multicall(sendWnt + sendTokens
    # + createOrder)` calldata HERE. NOT the ERC20 approval target — see
    # `gmx_router_proxy_address_arbitrum` below (audit C1).
    gmx_exchange_router_address_arbitrum: str = (
        "0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41"
    )
    # Router proxy (Arbitrum) — verified 2026-05-20. CRITICAL: this is the
    # ERC20 `approve()` target, NOT ExchangeRouter. Router holds the
    # `ROUTER_PLUGIN` role; ExchangeRouter calls `Router.pluginTransfer`
    # to pull collateral. Approving ExchangeRouter directly leaves
    # `pluginTransfer` reverting with onlyRouterPlugin. Audit C1.
    gmx_router_proxy_address_arbitrum: str = (
        "0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6"
    )
    # OrderVault (Arbitrum) — verified 2026-05-20. Destination of both
    # `sendWnt` (executionFee in WETH) and `sendTokens` (collateral). The
    # `createOrder` call drains it in the same multicall.
    gmx_order_vault_address_arbitrum: str = (
        "0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5"
    )
    # GMX V2 uses 30-decimal fixed-point for fundingFactorPerSecond /
    # borrowingFactorPerSecond / OI USD values. Convert with
    # `rate_per_8h = factor_per_second * 8 * 3600 / 10**30`.
    gmx_funding_factor_scale: int = 30
    # Reader/DataStore RPC timeout (seconds). Public Arbitrum RPC is ~200-500ms
    # for view calls; 5s leaves wide head-room. Override for slow back-ends.
    gmx_reader_timeout_s: float = 5.0
    # `acceptablePrice` slippage band in basis points. v2.2's pending-impact
    # mechanism tightens the band relative to v2.0/v2.1 — set wider than
    # historical defaults to avoid orders cancelling on every fill.
    # Per audit Q5+H2: 1-2% on majors (btc/eth), 3-5% on alts (sol/doge/xrp).
    gmx_default_acceptable_price_band_majors_bps: int = 150  # 1.5%
    gmx_default_acceptable_price_band_alts_bps: int = 350  # 3.5%
    # Gas limits for order execution — verified 2026-05-20 @ Arbitrum block
    # 464561127. DataStore keys `INCREASE_ORDER_GAS_LIMIT` and
    # `DECREASE_ORDER_GAS_LIMIT` both = 3,000,000. Used to right-size
    # executionFee = gas_limit * gas_price * headroom.
    gmx_increase_order_gas_limit: int = 3_000_000
    gmx_decrease_order_gas_limit: int = 3_000_000
    # Which funding fetcher the runtime uses: "mock" (paper stub from
    # funding_arb_runtime.py) or "live" (gmx_reader.fetch_gmx_funding_live).
    # Defaults to "mock" — flipping to "live" only makes signals real;
    # broadcasting to the network requires the separate `live_gmx_enabled`
    # gate below (see G5.2 section).
    gmx_funding_source: str = "mock"

    # --- Binance perp funding (G3 — CEX hedge leg) ---
    # Source for the CEX-side funding rate. "mock" (default) returns 0.0
    # via the legacy stub in funding_arb_runtime.py — keeps net_rate == gmx_rate.
    # "live" hits Binance's public /fapi/v1/premiumIndex endpoint.
    binance_funding_source: str = "mock"  # "mock" | "live"
    # Base URL — override to binance.us or a region-blocked alternative if needed.
    binance_fapi_base_url: str = "https://fapi.binance.com"
    # HTTP timeout (seconds). Binance fapi responds in ~50-200ms typically;
    # 5s leaves wide head-room and matches gmx_reader_timeout_s.
    binance_funding_timeout_s: float = 5.0
    # When True (default), the runtime uses one batched /premiumIndex call
    # per sweep (returns ~745 perp markets, we filter to our 5). Much cheaper
    # at 60s cadence than 5 separate calls. Set False to use per-market calls
    # (useful for debugging or if Binance starts rate-limiting the no-symbol path).
    binance_funding_batch: bool = True
    # Stale-near-settlement guard window (seconds). When `nextFundingTime` is
    # within this window of `now()`, the funding reader logs a WARN. The signal
    # is NOT suppressed — the rate is about to flip, which is a real edge
    # artifact the operator should know about (5min default = enough to spot
    # in logs before the new rate prints).
    binance_settlement_guard_s: int = 300
    # TTL for the binance_exchange_info module-level cache (seconds). Filter
    # values (LOT_SIZE, MIN_NOTIONAL, PRICE_FILTER) are dynamic but rarely
    # change in practice — Binance has been known to bump minQty / notional
    # during volatile episodes. 1h refresh is paranoid-cheap (one weight-1
    # request) and ensures a stale cache never silently causes -1111 PRECISION
    # or -4164 MIN_NOTIONAL rejects. See `binance_exchange_info.py` (G6.1).
    binance_exchange_info_ttl_s: int = 3600

    # --- Binance Futures HMAC auth (G6.2 — signed-endpoint creds) ---
    # API key + secret for the operator's Binance Futures account. NEVER
    # committed. Provisioned via env (BINANCE_API_KEY / BINANCE_API_SECRET)
    # or via /srv/secrets/binance_api_{key,secret} files (see secrets_dir
    # above). Both default to empty — the auth module logs a warning and
    # returns None from every signed call if either is unset. Required
    # scopes on the key: `enableFutures` + `enableReading` ONLY. NO
    # `enableWithdrawals`, NO `enableSpotAndMarginTrading`. IP-allowlist
    # to ai-primary's egress.
    binance_api_key: str = ""
    binance_api_secret: str = ""
    # `recvWindow` (ms) passed on every signed request. Default 5000ms per
    # Binance docs. Bounds the validation window for request freshness;
    # lower = stricter clock-drift requirement, higher = wider replay
    # window. 5000ms is the Binance default and is fine for ai-primary's
    # <2s clock drift.
    binance_recv_window_ms: int = 5000
    # Prefix for `newClientOrderId` generation in `binance_order.py` (G6.4).
    # Binance accepts a 1-36 char id matching `^[\.A-Z\:/a-z0-9_-]{1,36}$`.
    # The prefix tags every G6 order so the operator can grep them out of
    # the Binance UI / trade history. We append `uuid4().hex[:16]` for the
    # unique tail — total length 31 chars, well under the 36 cap.
    binance_order_idempotency_prefix: str = "gmx-strategies-"

    # Markets to monitor (must match chainlink-streams aliases for the
    # underlying asset — GMX uses Chainlink Data Streams as oracle).
    # 5 Arbitrum perp markets that overlap our 7 live Streams feeds.
    # BNB + HYPE feeds excluded — no GMX V2 market for either.
    monitored_markets: str = "btc,eth,sol,doge,xrp"

    # --- Funding rate arb (delta-neutral) ---
    # Min absolute funding rate (per 8 hours, fraction) to consider opening.
    funding_arb_min_rate: float = 0.0005  # 0.05%/8hr ~ 5.5%/yr
    funding_arb_max_position_usd: float = 50_000.0
    funding_arb_hedge_venue: str = "binance"  # spot hedge
    # Runtime loop cadence (seconds between full sweeps of monitored markets).
    funding_arb_poll_interval_s: int = 60
    # Pub/sub channel + stream for paper-mode signal emit.
    funding_arb_signals_channel: str = "funding_arb:signals"
    funding_arb_eval_log_stream: str = "funding_arb:eval_log"
    funding_arb_eval_log_maxlen: int = 1_000_000

    # --- Eval log streams ---
    paper_log_stream: str = "gmx:eval_log"
    paper_log_maxlen: int = 5_000_000

    # --- Live execution gates (per-venue, post-G5.2) ---
    # Each gate defaults False; flipping one does not enable the other.
    # `live_gmx_enabled` is consumed by gmx_signer.submit_signed.
    # `live_binance_enabled` is reserved for the future Binance hedge-leg
    # broadcaster (G6.4). Both remain False until the operator opts in.
    #
    # The pre-G5.2 `live_enabled` boolean + `live_strategies_confirmed`
    # CSV were removed in this PR — they were vestigial (no consumers) and
    # gave a false impression of a working gate. Per-venue gates are the
    # canonical mechanism going forward.
    live_gmx_enabled: bool = False
    live_binance_enabled: bool = False

    # --- GMX V2 signer (G5.2) ---
    # Path to the file containing the operator's executor private key.
    # Convention: `chown root:root && chmod 0400` so only the deploy user
    # can read. The signer module (`gmx_signer.py`) strips whitespace,
    # rejects empty files, and NEVER logs the contents. Falls back to the
    # `GMX_EXECUTOR_KEY` env var ONLY for dev convenience — production
    # deploys MUST use the file.
    gmx_executor_key_path: str = "/srv/secrets/gmx_executor_key"
    # Chain id passed into EIP-1559 transactions. 42161 = Arbitrum One.
    gmx_chain_id_arbitrum: int = 42161
    # Priority fee (gwei) for EIP-1559 tip. Arbitrum priority fees are
    # microscopic — 0.01 gwei = 10_000_000 wei is the established norm
    # for the sequencer-prioritised tier (verified vs `eth_maxPriorityFee`
    # responses on Arbitrum 2026-05-20).
    gmx_max_priority_fee_gwei: float = 0.01
    # Safety margin (percent) added on top of the audit-verified gas
    # limits before submitting. 20% gives the keeper room when blocks are
    # full or the order has a slightly heavier impact path than the
    # baseline gas-estimate assumed.
    gmx_gas_limit_safety_margin_pct: int = 20
    # Max seconds to poll for an `eth_getTransactionReceipt` after a
    # broadcast before bailing with a receiptless SendResult. The hash
    # is still returned — the operator can re-poll out-of-band.
    gmx_submit_receipt_timeout_s: int = 60

    max_position_usd: float = 5_000.0  # initial cap
    max_concurrent_positions: int = 3

    # --- G7.3 Pilot guard (refuse-to-broadcast layer) ---
    # The pilot guard is the LAST safety belt — every order placement must
    # pass `PilotGuard.check()` before broadcast. See `pilot_guard.py`.
    # The module is independent of the per-venue `live_*_enabled` gates;
    # both must be True AND the guard must allow. The guard's posture is
    # DEFAULT-DENY — empty `funding_arb_armed_markets_csv` means NOTHING
    # trades, regardless of `live_gmx_enabled`/`live_binance_enabled`.
    #
    # Memory pointers:
    #   memory/postmortem_2026_05_19_chainlink_lag_unpause.md — operator
    #     was burned by chainlink_lag bleeding $22 in 1h. Same shape we're
    #     preventing here.
    #   memory/plan_scaling_ladder_chainlink.md — discipline mirrored.
    funding_arb_armed_markets_csv: str = ""
    funding_arb_pilot_position_cap_usd: float = 10.0
    funding_arb_pilot_max_concurrent: int = 1
    funding_arb_pilot_daily_pnl_floor_usd: float = -50.0
    funding_arb_pilot_loss_cooldown_s: int = 1800
    # ruff S105 false-positive: these are Redis key names, not passwords.
    pilot_killswitch_key: str = "funding_arb:killswitch"  # noqa: S105
    pilot_last_loss_ts_key: str = "funding_arb:last_loss_ts_ms"  # noqa: S105
    funding_arb_executions_stream_key: str = "funding_arb:executions"  # noqa: S105
    guard_blocks_stream_key: str = "funding_arb:guard_blocks"  # noqa: S105
    guard_blocks_maxlen: int = 100_000
    # Cap on the executions stream length (XADD MAXLEN ~). Each entry is
    # one signal's outcome; at the 60s sweep cadence + 5 markets we expect
    # ~7k entries/day. 1M leaves >100 days of audit trail.
    funding_arb_executions_maxlen: int = 1_000_000

    # --- G7.1 Funding-arb consumer (signal → executor wiring) ---
    # HARD GATE — above the per-venue live_*_enabled flags. Default False
    # means the consumer is never instantiated; main.py keeps doing what
    # it does today (paper signal emit only). To enable the consumer the
    # operator MUST flip this AND restart the process. There is no
    # in-flight enable: this is read ONCE at startup.
    funding_arb_consumer_enabled: bool = False
    # Per-leg dry_run override. When True (default), BOTH legs use the
    # dry_run path even if live_gmx_enabled / live_binance_enabled are
    # True. Set False to allow real broadcasts (still gated by the
    # per-venue live flags + PilotGuard).
    funding_arb_executor_dry_run: bool = True
    # Default per-signal sizing. Overridden per signal if the payload
    # carries `target_position_usd` (which the G2/G3 emitter does set).
    funding_arb_target_position_usd: float = 10.0

    # HTTP
    http_host: str = "0.0.0.0"  # noqa: S104
    http_port: int = 8013

    log_level: str = "INFO"

    # --- Watchdog (trap monitors, see watchdog.py / cli.py) ---
    # Canonical source for the current GMX V2 Arbitrum Reader address.
    # The watchdog re-pulls this at run-time and compares against
    # `gmx_reader_address_arbitrum` to detect a GMX redeploy (the trap that
    # already burned us once — see memory/arch_gmx_v2_audit.md addendum
    # 2026-05-20).
    gmx_reader_github_url: str = (
        "https://raw.githubusercontent.com/gmx-io/gmx-synthetics/main/"
        "deployments/arbitrum/Reader.json"
    )
    # HyperLend Aave-V3-style Oracle on HyperEVM. setSourceOfAsset is a
    # governance-callable mutator; a rotation away from
    # `expected_hyperlend_whype_source` would silently flip the source OCDE
    # is meant to track. Watchdog checks the live mapping every run.
    hyperlend_oracle_address: str = "0xC9Fb4fbE842d57EAc1dF3e641a281827493A630e"
    hyperlend_whype_token: str = "0x5555555555555555555555555555555555555555"  # noqa: S105 — token addr, not secret
    expected_hyperlend_whype_source: str = "0x40EA33eA76Fbe35e9FB422eDd175b8c8D84A63Cc"
    hyperevm_rpc_url: str = "https://rpc.hyperliquid.xyz/evm"

    # Where the watchdog publishes drift alerts when invoked with --emit-alerts.
    # XADD with maxlen=10_000 (approx). Consumed via XREAD by an operator
    # tail / alert relay.
    trap_alerts_stream: str = "trap_alerts:gmx"
    trap_alerts_maxlen: int = 10_000
    # HTTP timeout (seconds) for watchdog HTTPS GETs to GitHub. GitHub raw
    # is usually <500ms; 10s leaves wide headroom for slow CI runners.
    watchdog_http_timeout_s: float = 10.0


settings = Settings()
