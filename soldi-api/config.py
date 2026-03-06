from typing import List

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # SoldiAPI's own API key (clients must provide this to authenticate)
    soldi_api_key: str = "dev-key-change-me"

    # The Odds API upstream key
    odds_api_key: str = ""

    # The Odds API base URL
    odds_api_base_url: str = "https://api.the-odds-api.com/v4"

    # Cache TTL in seconds — must exceed the full background refresh cycle
    # (~15 min across all 17 sports) so data stays cached between cycles.
    cache_ttl_seconds: int = 1200

    # Server settings
    host: str = "0.0.0.0"
    port: int = 3001

    # CORS
    cors_origins: List[str] = ["http://localhost:3000", "https://soldi-website.onrender.com"]

    # Debug mode (enables auto-reload)
    debug: bool = False

    # Polymarket wallet private key (for CLOB API auth if needed)
    polymarket_private_key: str = ""

    # Kalshi API key (for authenticated requests / higher rate limits)
    kalshi_api_key: str = ""

    # ProphetX affiliate API key
    prophetx_api_key: str = ""

    # Bet105 login credentials
    bet105_email: str = ""
    bet105_password: str = ""

    # Bookmaker.eu login credentials
    bookmaker_username: str = ""
    bookmaker_password: str = ""

    # Comma-separated list of scraper names to disable (e.g. "DraftKings,BetMGM,BetOnline")
    # Useful for resource-constrained deployments where Playwright scrapers are too heavy.
    disabled_scrapers: str = ""

    # Line history SQLite database path
    line_history_db: str = "line_history.db"

    # Line history retention in days (auto-purge older)
    line_history_retention_days: int = 7

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}
