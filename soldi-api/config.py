from typing import List

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # SoldiAPI's own API key (clients must provide this to authenticate)
    soldi_api_key: str = "dev-key-change-me"

    # Cache TTL in seconds — must exceed the full background refresh cycle
    # (~15 min across all sports) so data stays cached between cycles.
    cache_ttl_seconds: int = 1200

    # Server settings
    host: str = "0.0.0.0"
    port: int = 3001

    # CORS
    cors_origins: List[str] = ["http://localhost:3000", "https://soldi-website.onrender.com"]

    # Debug mode (enables auto-reload)
    debug: bool = False

    # Kalshi RSA private key (PEM format, for authenticated requests / higher rate limits)
    kalshi_api_key: str = ""
    kalshi_rsa_private_key: str = ""

    # ProphetX affiliate API key
    prophetx_api_key: str = ""

    # Bet105 login credentials
    bet105_email: str = ""
    bet105_password: str = ""

    # Bookmaker.eu login credentials
    bookmaker_username: str = ""
    bookmaker_password: str = ""

    # Buckeye login credentials
    buckeye_username: str = "xl37"
    buckeye_password: str = "test"
    buckeye_url: str = "https://demotest.me/"

    # Comma-separated list of scraper names to disable (e.g. "DraftKings,BetOnline")
    # Useful for resource-constrained deployments where Playwright scrapers are too heavy.
    disabled_scrapers: str = ""

    # Line history SQLite database path
    line_history_db: str = "line_history.db"

    # Line history retention in days (auto-purge older)
    line_history_retention_days: int = 7

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}
