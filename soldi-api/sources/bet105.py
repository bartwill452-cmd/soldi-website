"""
Bet105 sportsbook scraper.

Uses httpx to fetch odds from Bet105's GeniusSports/BetConstruct-powered
prematch platform at ppm.bet105.ag.

The PPM platform exposes a REST API at /global/api/ endpoints. We call
GetGameList for each sport to retrieve events and odds data.

GS Betting sport IDs (ppm.bet105.ag):
  1 = Baseball, 2 = Basketball (NBA + NCAAB), 3 = Football,
  4 = Hockey, 5 = Soccer, 7 = Golf, 8 = Tennis, 13 = Boxing,
  27 = MMA, 214 = World Cup, 88888477 = Futures
"""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

PPM_URL = "https://ppm.bet105.ag"

# ── OddsScreen sport_key -> GS sport_id ─────────────────────────────────────
_SPORT_MAP = {
    "basketball_nba": 2,
    "basketball_ncaab": 2,
    "americanfootball_nfl": 3,
    "icehockey_nhl": 4,
    "baseball_mlb": 1,
    "mma_mixed_martial_arts": 27,
    "boxing_boxing": 13,
    "tennis_atp": 8,
    "tennis_wta": 8,
    "soccer_epl": 5,
    "soccer_spain_la_liga": 5,
    "soccer_germany_bundesliga": 5,
    "soccer_italy_serie_a": 5,
    "soccer_france_ligue_one": 5,
    "soccer_uefa_champs_league": 5,
}  # type: Dict[str, int]

# ── Reverse mapping: GS sport_id -> list of sport_keys ──────────────────────
_SPORT_ID_TO_KEYS = {}  # type: Dict[int, List[str]]
for _key, _sid in _SPORT_MAP.items():
    _SPORT_ID_TO_KEYS.setdefault(_sid, []).append(_key)

# ── League name -> sport_key ─────────────────────────────────────────────────
_LEAGUE_TO_KEY = {
    # Soccer
    "english premier league": "soccer_epl",
    "english-premier-league": "soccer_epl",
    "premier league": "soccer_epl",
    "epl": "soccer_epl",
    "england - premier league": "soccer_epl",
    "england premier league": "soccer_epl",
    "la liga": "soccer_spain_la_liga",
    "spain - la liga": "soccer_spain_la_liga",
    "spanish la liga": "soccer_spain_la_liga",
    "spain la liga": "soccer_spain_la_liga",
    "laliga": "soccer_spain_la_liga",
    "bundesliga": "soccer_germany_bundesliga",
    "german bundesliga": "soccer_germany_bundesliga",
    "germany - bundesliga": "soccer_germany_bundesliga",
    "germany bundesliga": "soccer_germany_bundesliga",
    "serie a": "soccer_italy_serie_a",
    "italian serie a": "soccer_italy_serie_a",
    "italy - serie a": "soccer_italy_serie_a",
    "italy serie a": "soccer_italy_serie_a",
    "ligue 1": "soccer_france_ligue_one",
    "french ligue 1": "soccer_france_ligue_one",
    "france - ligue 1": "soccer_france_ligue_one",
    "france ligue 1": "soccer_france_ligue_one",
    "champions league": "soccer_uefa_champs_league",
    "uefa champions league": "soccer_uefa_champs_league",
    "uefa-champions-league": "soccer_uefa_champs_league",
    "uefa champions": "soccer_uefa_champs_league",
    "ucl": "soccer_uefa_champs_league",
    # Tennis
    "atp": "tennis_atp",
    "wta": "tennis_wta",
    # Basketball
    "nba": "basketball_nba",
    "ncaa": "basketball_ncaab",
    "ncaab": "basketball_ncaab",
    "college basketball": "basketball_ncaab",
    "ncaa basketball": "basketball_ncaab",
    # Hockey
    "nhl": "icehockey_nhl",
    # Football
    "nfl": "americanfootball_nfl",
    # Baseball
    "mlb": "baseball_mlb",
    "exhibition": "baseball_mlb",
    "spring training": "baseball_mlb",
    "preseason": "baseball_mlb",
    # MMA
    "ufc": "mma_mixed_martial_arts",
    "mma": "mma_mixed_martial_arts",
    "pfl": "mma_mixed_martial_arts",
    "bellator": "mma_mixed_martial_arts",
    # Boxing
    "boxing": "boxing_boxing",
}  # type: Dict[str, str]

# ── Known NBA teams (for basketball NBA vs NCAAB filtering) ─────────────────
_NBA_TEAMS = frozenset([
    "Atlanta Hawks", "Boston Celtics", "Brooklyn Nets", "Charlotte Hornets",
    "Chicago Bulls", "Cleveland Cavaliers", "Dallas Mavericks",
    "Denver Nuggets", "Detroit Pistons", "Golden State Warriors",
    "Houston Rockets", "Indiana Pacers", "LA Clippers",
    "Los Angeles Clippers", "Los Angeles Lakers", "Memphis Grizzlies",
    "Miami Heat", "Milwaukee Bucks", "Minnesota Timberwolves",
    "New Orleans Pelicans", "New York Knicks", "Oklahoma City Thunder",
    "Orlando Magic", "Philadelphia 76ers", "Phoenix Suns",
    "Portland Trail Blazers", "Sacramento Kings", "San Antonio Spurs",
    "Toronto Raptors", "Utah Jazz", "Washington Wizards",
])

# Keywords that indicate a futures market (skip these)
_FUTURES_KEYWORDS = frozenset([
    "wins", "champion", "coin toss", "futures", "conference",
    "division", "mvp", "award", "super bowl", "world series",
    "stanley cup", "nba finals", "pennant", "cy young", "heisman",
])

# BetConstruct/SpringBuilder API endpoints to try
_API_ENDPOINTS = [
    "{base}/global/api/GetGameList",
    "{base}/api/games/prematch",
    "{base}/api/v1/prematch/games",
]

# GS competition IDs for soccer leagues
_SOCCER_COMPETITION_IDS = {
    "soccer_epl": [122],
    "soccer_spain_la_liga": [2332],
    "soccer_germany_bundesliga": [30],
    "soccer_italy_serie_a": [178],
    "soccer_france_ligue_one": [130],
    "soccer_uefa_champs_league": [39],
}

# Cache TTL
_CACHE_TTL = 45  # seconds


class Bet105Source(DataSource):
    """Fetches odds from Bet105 via HTTP API calls.

    Attempts to access BetConstruct/GeniusSports REST API endpoints
    at ppm.bet105.ag for prematch odds data.
    """

    def __init__(self, email: str = "", password: str = ""):
        self._client = httpx.AsyncClient(
            timeout=20.0,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": PPM_URL,
                "Referer": f"{PPM_URL}/live/",
            },
            follow_redirects=True,
        )
        self._cache = {}  # type: Dict[str, Tuple[List[OddsEvent], float]]
        self._prefetch_task = None  # type: object
        self._api_endpoint = None  # type: Optional[str]

    def start_prefetch(self) -> None:
        """Start background prefetch of all supported sports."""
        self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    async def _prefetch_all(self) -> None:
        await asyncio.sleep(8)
        logger.info("Bet105: Starting continuous background prefetch (HTTP)")
        cycle = 0
        while True:
            cycle += 1
            try:
                fetched_ids = set()  # type: set
                for sport_key in _SPORT_MAP:
                    sport_id = _SPORT_MAP[sport_key]
                    if sport_id in fetched_ids:
                        continue
                    fetched_ids.add(sport_id)
                    try:
                        await self._fetch_sport(sport_id)
                    except Exception as e:
                        logger.warning(
                            "Bet105 prefetch failed for sport ID %d: %s",
                            sport_id, e,
                        )
            except Exception as e:
                logger.warning("Bet105 prefetch error: %s", e)
            logger.info("Bet105: Prefetch cycle #%d complete", cycle)
            await asyncio.sleep(30)

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        headers = {"x-requests-remaining": "unlimited"}

        if bookmakers and "bet105" not in bookmakers:
            return [], headers

        if sport_key not in _SPORT_MAP:
            return [], headers

        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _CACHE_TTL:
            return cached[0], headers
        if cached:
            return cached[0], headers
        return [], headers

    # ------------------------------------------------------------------
    # HTTP Fetching
    # ------------------------------------------------------------------

    async def _fetch_sport(self, sport_id: int) -> None:
        """Fetch events for a sport via HTTP API."""
        sibling_keys = _SPORT_ID_TO_KEYS.get(sport_id, [])

        data = await self._call_api(sport_id)
        if not data:
            now = time.time()
            for k in sibling_keys:
                if k not in self._cache:
                    self._cache[k] = ([], now)
            return

        classified = self._parse_api_response(data, sport_id)
        now = time.time()
        for key, events in classified.items():
            self._cache[key] = (events, now)
            if events:
                logger.info("Bet105: %d events for %s", len(events), key)

        for k in sibling_keys:
            if k not in classified:
                self._cache[k] = ([], now)

    async def _call_api(self, sport_id: int) -> Optional[dict]:
        """Try various API endpoints to fetch game data."""
        # Try known BetConstruct/SpringBuilder API patterns
        endpoints = [
            f"{PPM_URL}/global/api/GetGameList",
            f"{PPM_URL}/api/games/prematch",
        ]

        # If we've found a working endpoint before, try it first
        if self._api_endpoint:
            endpoints.insert(0, self._api_endpoint)

        for endpoint in endpoints:
            try:
                # Try POST with sport ID
                payload = {
                    "sportId": sport_id,
                    "language": "en",
                    "oddsFormat": "american",
                }
                response = await self._client.post(endpoint, json=payload)
                if response.status_code == 200:
                    data = response.json()
                    if data and (isinstance(data, dict) or isinstance(data, list)):
                        self._api_endpoint = endpoint
                        return data if isinstance(data, dict) else {"games": data}

                # Try GET with query params
                response = await self._client.get(
                    endpoint,
                    params={"sportId": sport_id, "language": "en"},
                )
                if response.status_code == 200:
                    data = response.json()
                    if data and (isinstance(data, dict) or isinstance(data, list)):
                        self._api_endpoint = endpoint
                        return data if isinstance(data, dict) else {"games": data}

            except Exception as e:
                logger.debug("Bet105 API endpoint %s failed: %s", endpoint, e)
                continue

        # Try fetching the HTML page and extracting any embedded JSON data
        try:
            response = await self._client.get(
                f"{PPM_URL}/en/prematch/sport/{sport_id}",
                headers={"Accept": "text/html,application/xhtml+xml"},
            )
            if response.status_code == 200:
                html = response.text
                # Look for embedded JSON data in script tags
                json_match = re.search(
                    r'(?:window\.__INITIAL_STATE__|var\s+data\s*=|gameList\s*=)\s*(\{.+?\});?\s*(?:</script>|$)',
                    html,
                    re.DOTALL,
                )
                if json_match:
                    import json
                    try:
                        data = json.loads(json_match.group(1))
                        return data
                    except Exception:
                        pass
        except Exception as e:
            logger.debug("Bet105 HTML fetch failed: %s", e)

        return None

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_api_response(
        self, data: dict, sport_id: int
    ) -> Dict[str, List[OddsEvent]]:
        """Parse API response into events grouped by sport_key."""
        result = {}  # type: Dict[str, List[OddsEvent]]

        # Try various response formats
        games = []  # type: list
        if "games" in data:
            games = data["games"]
        elif "data" in data:
            inner = data["data"]
            if isinstance(inner, list):
                games = inner
            elif isinstance(inner, dict) and "games" in inner:
                games = inner["games"]
        elif "events" in data:
            games = data["events"]
        elif isinstance(data, list):
            games = data

        for game in games:
            if not isinstance(game, dict):
                continue

            event = self._parse_game(game, sport_id)
            if event:
                result.setdefault(event.sport_key, []).append(event)

        return result

    def _parse_game(self, game: dict, sport_id: int) -> Optional[OddsEvent]:
        """Parse a single game from the API response."""
        # Extract team names (various field name conventions)
        home_team = (
            game.get("homeTeam") or game.get("team2") or
            game.get("home") or game.get("Team2") or ""
        )
        away_team = (
            game.get("awayTeam") or game.get("team1") or
            game.get("away") or game.get("Team1") or ""
        )

        if isinstance(home_team, dict):
            home_team = home_team.get("name", "")
        if isinstance(away_team, dict):
            away_team = away_team.get("name", "")

        if not home_team or not away_team:
            return None

        home_team = resolve_team_name(str(home_team).strip())
        away_team = resolve_team_name(str(away_team).strip())

        # Skip futures
        combined = (away_team + " " + home_team).lower()
        if any(kw in combined for kw in _FUTURES_KEYWORDS):
            return None

        # Determine sport_key from league name or team names
        league = str(game.get("league") or game.get("competition") or
                     game.get("leagueName") or "").strip()
        sport_key = self._resolve_sport_key(league, home_team, away_team, sport_id)
        if not sport_key:
            return None

        # Parse start time
        start_time = game.get("startTime") or game.get("dateTime") or game.get("date") or ""
        commence_time = self._parse_time(str(start_time))

        # Parse markets
        markets_list = self._parse_markets(game)
        if not markets_list:
            return None

        sport_title = get_sport_title(sport_key)
        cid = canonical_event_id(sport_key, home_team, away_team, commence_time)

        return OddsEvent(
            id=cid,
            sport_key=sport_key,
            sport_title=sport_title,
            commence_time=commence_time,
            home_team=home_team,
            away_team=away_team,
            bookmakers=[
                Bookmaker(
                    key="bet105",
                    title="Bet105",
                    markets=markets_list,
                )
            ],
        )

    def _parse_markets(self, game: dict) -> List[Market]:
        """Parse markets from a game object."""
        markets = []  # type: List[Market]
        seen_keys = set()  # type: set

        # Try various market data structures
        market_data = (
            game.get("markets") or game.get("odds") or
            game.get("offerings") or []
        )

        if isinstance(market_data, dict):
            market_data = list(market_data.values())

        for mkt in market_data:
            if not isinstance(mkt, dict):
                continue

            market = self._parse_single_market(mkt)
            if market and market.key not in seen_keys:
                markets.append(market)
                seen_keys.add(market.key)

        # Also try flat odds fields (GS format)
        if not markets:
            markets = self._parse_flat_odds(game)

        return markets

    def _parse_single_market(self, mkt: dict) -> Optional[Market]:
        """Parse a single market object."""
        market_type = str(mkt.get("type") or mkt.get("marketType") or
                         mkt.get("name") or "").lower()

        outcomes_raw = mkt.get("outcomes") or mkt.get("selections") or []

        if "moneyline" in market_type or "winner" in market_type or "h2h" in market_type:
            outcomes = self._parse_outcomes(outcomes_raw)
            if len(outcomes) >= 2:
                return Market(key="h2h", outcomes=outcomes)
        elif "spread" in market_type or "handicap" in market_type:
            outcomes = self._parse_outcomes(outcomes_raw, include_points=True)
            if len(outcomes) >= 2:
                return Market(key="spreads", outcomes=outcomes)
        elif "total" in market_type or "over" in market_type:
            outcomes = self._parse_outcomes(outcomes_raw, include_points=True, is_total=True)
            if len(outcomes) >= 2:
                return Market(key="totals", outcomes=outcomes)

        return None

    def _parse_outcomes(
        self, outcomes: list, include_points: bool = False, is_total: bool = False
    ) -> List[Outcome]:
        """Parse outcome objects into Outcome models."""
        result = []
        for o in outcomes:
            if not isinstance(o, dict):
                continue

            name = str(o.get("name") or o.get("label") or o.get("description") or "")
            price = o.get("price") or o.get("odds") or o.get("americanOdds")

            if price is None:
                continue
            try:
                price = int(float(str(price)))
            except (ValueError, TypeError):
                continue

            point = None
            if include_points:
                point_val = o.get("point") or o.get("handicap") or o.get("line")
                if point_val is not None:
                    try:
                        point = float(str(point_val))
                    except (ValueError, TypeError):
                        pass

            if is_total:
                lower = name.lower()
                if "over" in lower:
                    name = "Over"
                elif "under" in lower:
                    name = "Under"

            result.append(Outcome(name=name, price=price, point=point))
        return result

    def _parse_flat_odds(self, game: dict) -> List[Market]:
        """Parse flat odds fields (GeniusSports format).

        GS uses fields like:
          market-3 = Moneyline, market-6 = Spread, market-5 = Total
        """
        markets = []

        # Try to find moneyline odds
        ml_home = self._safe_int(game.get("homeMoneyline") or game.get("ml2"))
        ml_away = self._safe_int(game.get("awayMoneyline") or game.get("ml1"))
        if ml_home is not None and ml_away is not None:
            home_name = game.get("homeTeam") or game.get("team2") or "Home"
            away_name = game.get("awayTeam") or game.get("team1") or "Away"
            if isinstance(home_name, dict):
                home_name = home_name.get("name", "Home")
            if isinstance(away_name, dict):
                away_name = away_name.get("name", "Away")
            markets.append(Market(
                key="h2h",
                outcomes=[
                    Outcome(name=str(home_name), price=ml_home),
                    Outcome(name=str(away_name), price=ml_away),
                ],
            ))

        # Try spread
        spread = self._safe_float(game.get("spread") or game.get("handicap"))
        sp_home = self._safe_int(game.get("homeSpreadOdds") or game.get("sp2"))
        sp_away = self._safe_int(game.get("awaySpreadOdds") or game.get("sp1"))
        if spread is not None and sp_home is not None and sp_away is not None:
            home_name = game.get("homeTeam") or game.get("team2") or "Home"
            away_name = game.get("awayTeam") or game.get("team1") or "Away"
            if isinstance(home_name, dict):
                home_name = home_name.get("name", "Home")
            if isinstance(away_name, dict):
                away_name = away_name.get("name", "Away")
            markets.append(Market(
                key="spreads",
                outcomes=[
                    Outcome(name=str(home_name), price=sp_home, point=-spread),
                    Outcome(name=str(away_name), price=sp_away, point=spread),
                ],
            ))

        # Try total
        total = self._safe_float(game.get("total") or game.get("totalPoints"))
        over_odds = self._safe_int(game.get("overOdds") or game.get("ov"))
        under_odds = self._safe_int(game.get("underOdds") or game.get("un"))
        if total is not None and over_odds is not None and under_odds is not None:
            markets.append(Market(
                key="totals",
                outcomes=[
                    Outcome(name="Over", price=over_odds, point=total),
                    Outcome(name="Under", price=under_odds, point=total),
                ],
            ))

        return markets

    def _resolve_sport_key(
        self, league: str, home: str, away: str, sport_id: int
    ) -> Optional[str]:
        """Determine sport_key from league name and team names."""
        if league:
            lower = league.lower().strip()
            # Exact match
            if lower in _LEAGUE_TO_KEY:
                return _LEAGUE_TO_KEY[lower]
            # Partial match
            for pattern, key in _LEAGUE_TO_KEY.items():
                if pattern in lower or lower in pattern:
                    return key

        # Fallback: use sport_id + team name heuristics
        if sport_id == 2:
            # Basketball: NBA vs NCAAB
            if home in _NBA_TEAMS or away in _NBA_TEAMS:
                return "basketball_nba"
            return "basketball_ncaab"
        elif sport_id == 8:
            # Tennis: ATP vs WTA (heuristic)
            return "tennis_atp"

        # Use first matching key for this sport_id
        keys = _SPORT_ID_TO_KEYS.get(sport_id, [])
        return keys[0] if keys else None

    @staticmethod
    def _parse_time(raw: str) -> str:
        """Parse various time formats to ISO 8601."""
        if not raw:
            return ""
        try:
            # Try ISO format
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            pass
        try:
            # Try epoch timestamp
            ts = float(raw)
            if ts > 1e12:
                ts /= 1000  # milliseconds
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            pass
        return raw

    @staticmethod
    def _safe_int(val) -> Optional[int]:
        if val is None or val == "":
            return None
        try:
            return int(float(str(val)))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        if val is None or val == "":
            return None
        try:
            return float(str(val))
        except (ValueError, TypeError):
            return None

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()
