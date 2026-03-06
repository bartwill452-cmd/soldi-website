import logging
from typing import Dict, List, Optional, Tuple

import httpx

from models import OddsEvent
from sources.base import DataSource

logger = logging.getLogger(__name__)


class TheOddsAPISource(DataSource):
    """Adapter for The Odds API v4 (https://the-odds-api.com)."""

    def __init__(self, api_key: str, base_url: str = "https://api.the-odds-api.com/v4"):
        self._api_key = api_key
        self._base_url = base_url
        self._client = httpx.AsyncClient(timeout=30.0)

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        params: Dict[str, str] = {
            "apiKey": self._api_key,
            "regions": ",".join(regions) if regions else "us",
            "markets": ",".join(markets) if markets else "h2h",
            "oddsFormat": odds_format,
        }
        if bookmakers:
            params["bookmakers"] = ",".join(bookmakers)

        url = f"{self._base_url}/sports/{sport_key}/odds"
        logger.info(f"Fetching odds from {url}")

        response = await self._client.get(url, params=params)
        response.raise_for_status()

        raw_events = response.json()
        events = [OddsEvent.model_validate(e) for e in raw_events]

        headers = {
            "x-requests-remaining": response.headers.get("x-requests-remaining", "unknown"),
            "x-requests-used": response.headers.get("x-requests-used", "unknown"),
        }

        logger.info(
            f"Got {len(events)} events for {sport_key}, "
            f"requests remaining: {headers['x-requests-remaining']}"
        )

        return events, headers

    async def close(self) -> None:
        await self._client.aclose()
