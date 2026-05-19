"""Env-driven settings (v0.2 — funding-arb only)."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

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
    # GMX V2 uses 30-decimal fixed-point for fundingFactorPerSecond /
    # borrowingFactorPerSecond / OI USD values. Convert with
    # `rate_per_8h = factor_per_second * 8 * 3600 / 10**30`.
    gmx_funding_factor_scale: int = 30
    # Reader/DataStore RPC timeout (seconds). Public Arbitrum RPC is ~200-500ms
    # for view calls; 5s leaves wide head-room. Override for slow back-ends.
    gmx_reader_timeout_s: float = 5.0
    # Which funding fetcher the runtime uses: "mock" (paper stub from
    # funding_arb_runtime.py) or "live" (gmx_reader.fetch_gmx_funding_live).
    # Defaults to "mock" — LIVE_ENABLED gate untouched, but signals stay
    # synthetic until the operator explicitly opts in.
    gmx_funding_source: str = "mock"

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

    # --- Live execution gates ---
    live_enabled: bool = False
    live_strategies_confirmed: str = ""  # CSV: funding_arb

    # Wallet — never commit
    executor_private_key: str = ""

    max_position_usd: float = 5_000.0  # initial cap
    max_concurrent_positions: int = 3

    # HTTP
    http_host: str = "0.0.0.0"  # noqa: S104
    http_port: int = 8013

    log_level: str = "INFO"


settings = Settings()
