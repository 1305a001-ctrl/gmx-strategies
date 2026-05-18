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

    # Markets to monitor (must match chainlink-streams aliases for the
    # underlying asset — GMX uses Chainlink Data Streams as oracle).
    monitored_markets: str = "btc,eth,sol,wsteth"

    # --- Funding rate arb (delta-neutral) ---
    # Min absolute funding rate (per 8 hours, fraction) to consider opening.
    funding_arb_min_rate: float = 0.0005  # 0.05%/8hr ~ 5.5%/yr
    funding_arb_max_position_usd: float = 50_000.0
    funding_arb_hedge_venue: str = "binance"  # spot hedge

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
