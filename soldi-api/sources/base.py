from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from models import OddsEvent, PlayerProp


class DataSource(ABC):
    """Abstract interface for odds data providers."""

    @abstractmethod
    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        """
        Fetch odds for a given sport.

        Returns:
            tuple of (events_list, response_headers_dict)
            Headers dict should include "x-requests-remaining".
        """
        ...

    async def get_player_props(
        self, sport_key: str, event_id: str
    ) -> List[PlayerProp]:
        """Fetch player props for a specific event. Override in subclasses."""
        return []

    async def get_team_futures(
        self, sport_key: str
    ) -> Optional[Dict[str, Any]]:
        """Return team-level championship/futures odds if available.

        Prediction markets (Polymarket, Kalshi) that have championship
        futures but lack game-level markets can override this to provide
        per-team championship odds that get attached to game events.

        Returns None by default. When overridden, return:
            {
                "bookmaker_key": str,
                "bookmaker_title": str,
                "teams": {
                    normalized_team_name: {
                        "raw_name": str,
                        "price": int,              # American odds
                        "liquidity": float or None,
                    }
                }
            }
        """
        return None

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources (e.g., HTTP client sessions)."""
        ...
