"""
ESPN scoreboard scraper.
Uses ESPN's public scoreboard API to fetch live scores and game status.
Does NOT scrape odds from ESPN.
"""

import logging
from typing import Dict, List, Optional, Tuple

import httpx

from models import OddsEvent, ScoreData
from sources.base import DataSource
from sources.sport_mapping import (
    ESPN_SPORT_LEAGUES,
    canonical_event_id,
    get_sport_title,
)

logger = logging.getLogger(__name__)

SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports"


class ESPNSource(DataSource):
    """Fetches live scores from ESPN's public scoreboard API (no odds)."""

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
            },
        )

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        mapping = ESPN_SPORT_LEAGUES.get(sport_key)
        if mapping is None:
            return [], {"x-requests-remaining": "unlimited"}

        espn_sport, espn_league = mapping

        try:
            scoreboard_events = await self._fetch_scoreboard(espn_sport, espn_league)
            if not scoreboard_events:
                return [], {"x-requests-remaining": "unlimited"}

            events = []
            sport_title = get_sport_title(sport_key)

            for sb_event in scoreboard_events:
                parsed = self._parse_event(sb_event, sport_key, sport_title)
                if parsed:
                    events.append(parsed)

            logger.info(f"ESPN scores: {len(events)} events for {sport_key}")
            return events, {"x-requests-remaining": "unlimited"}

        except Exception as e:
            logger.warning(f"ESPN scores failed for {sport_key}: {e}")
            return [], {"x-requests-remaining": "unlimited"}

    async def _fetch_scoreboard(self, sport: str, league: str) -> list:
        """Fetch current events from ESPN scoreboard."""
        url = f"{SCOREBOARD_URL}/{sport}/{league}/scoreboard"
        response = await self._client.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("events", [])

    def _parse_event(
        self, scoreboard_event: dict, sport_key: str, sport_title: str
    ) -> Optional[OddsEvent]:
        """Parse an ESPN scoreboard event into an OddsEvent with score data only."""
        competitions = scoreboard_event.get("competitions", [])
        if not competitions:
            return None

        competition = competitions[0]
        competitors = competition.get("competitors", [])
        if len(competitors) < 2:
            return None

        home_team = ""
        away_team = ""
        for comp in competitors:
            team_name = comp.get("team", {}).get("displayName", "")
            if comp.get("homeAway") == "home":
                home_team = team_name
            else:
                away_team = team_name

        if not home_team or not away_team:
            return None

        commence_time = scoreboard_event.get("date", "")

        # Extract score data from scoreboard
        status_data = competition.get("status", {})
        status_type = status_data.get("type", {})
        game_state = status_type.get("state", "pre")

        score_data = None
        if game_state in ("in", "post"):
            home_score = None
            away_score = None
            for comp in competitors:
                score_val = comp.get("score")
                if comp.get("homeAway") == "home":
                    home_score = str(score_val) if score_val is not None else None
                else:
                    away_score = str(score_val) if score_val is not None else None

            score_data = ScoreData(
                home_score=home_score,
                away_score=away_score,
                status=game_state,
                detail=status_type.get("shortDetail") or status_type.get("detail"),
                period=status_data.get("period"),
                clock=status_data.get("displayClock"),
            )

        # Only return events that have score data (live or completed games)
        # Pre-game events without odds are not useful
        if not score_data:
            return None

        cid = canonical_event_id(sport_key, home_team, away_team, commence_time)
        return OddsEvent(
            id=cid,
            sport_key=sport_key,
            sport_title=sport_title,
            commence_time=commence_time,
            home_team=home_team,
            away_team=away_team,
            bookmakers=[],
            score_data=score_data,
        )

    async def close(self) -> None:
        await self._client.aclose()
