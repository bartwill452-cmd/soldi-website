"""
BetMGM sportsbook scraper.

Uses httpx to directly query BetMGM's public cds-api/bettingoffer endpoints.
No browser required — the API is unauthenticated and returns JSON fixture data.

Strategy:
  1. Build cds-api URL with sportId and competitionId for each sport_key
  2. Try multiple state subdomains (va, nj, co, az) if geo-restricted
  3. Parse the fixture/game/selection response into OddsEvent format
  4. Cache results with 120s TTL
"""

import asyncio
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from market_keys import classify_market_type, detect_period_suffix
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

# ── BetMGM cds-api sport/competition IDs per sport_key ───────────────
_MGM_API_PARAMS: Dict[str, Dict[str, Any]] = {
    "basketball_nba":               {"sportIds": 7,  "competitionIds": 6004},
    "basketball_ncaab":             {"sportIds": 7,  "competitionIds": 264},
    "americanfootball_nfl":         {"sportIds": 11, "competitionIds": 35},
    "americanfootball_ncaaf":       {"sportIds": 11, "competitionIds": 211},
    "icehockey_nhl":                {"sportIds": 12, "competitionIds": 34},
    "baseball_mlb":                 {"sportIds": 23, "competitionIds": 75},
    "mma_mixed_martial_arts":       {"sportIds": 45, "competitionIds": 702},
    "boxing_boxing":                {"sportIds": 36},
    "soccer_epl":                   {"sportIds": 4,  "competitionIds": 102841},
    "soccer_spain_la_liga":         {"sportIds": 4,  "competitionIds": 102846},
    "soccer_germany_bundesliga":    {"sportIds": 4,  "competitionIds": 102842},
    "soccer_italy_serie_a":         {"sportIds": 4,  "competitionIds": 102843},
    "soccer_france_ligue_one":      {"sportIds": 4,  "competitionIds": 102840},
    "soccer_uefa_champs_league":    {"sportIds": 4,  "competitionIds": 702},
    "tennis_atp":                   {"sportIds": 5,  "competitionIds": 167},
    "tennis_wta":                   {"sportIds": 5,  "competitionIds": 168},
}

_STATE_SUBDOMAINS = ["va", "nj", "co", "az"]

_ACCESS_ID = "NmFjNmUwZjAtMGI3Yi00YzA3LTg3OTktNDgxMGIwM2YxZGVh"

_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "x-bwin-accessid": _ACCESS_ID,
}

_CACHE_TTL = 120  # seconds — prefetch loop takes ~80s to cycle all sports
_STALE_TTL = 900  # seconds — serve stale data up to 15 minutes (prefetch cycle ~13min)

# Unicode minus sign used in BetMGM odds display
_UNICODE_MINUS = "\u2212"  # −


def _parse_american_odds(odds_val: Any) -> Optional[int]:
    """Parse BetMGM american odds value (can be int, float, or string)."""
    if odds_val is None:
        return None
    if isinstance(odds_val, (int, float)):
        return int(odds_val)
    if isinstance(odds_val, str):
        cleaned = odds_val.replace(_UNICODE_MINUS, "-").replace("+", "").strip()
        try:
            return int(float(cleaned))
        except (ValueError, TypeError):
            return None
    return None


def _parse_decimal_to_american(decimal_odds: float) -> Optional[int]:
    """Convert decimal odds to American odds."""
    if decimal_odds is None or decimal_odds <= 1.0:
        return None
    if decimal_odds >= 2.0:
        return int(round((decimal_odds - 1) * 100))
    else:
        return int(round(-100 / (decimal_odds - 1)))


def _build_api_url(state: str, params: Dict[str, Any]) -> str:
    """Build a cds-api bettingoffer URL for the given state and sport params."""
    base = f"https://sports.{state}.betmgm.com/cds-api/bettingoffer/fixtures"
    query = {
        "x-bwin-accessid": _ACCESS_ID,
        "lang": "en-us",
        "country": "US",
        "userCountry": "US",
        "subdivision": f"US-{state.upper()}",
        "offerMapping": "All",
        "score498": "true",
        "sortBy": "Tags",
    }
    query.update(params)
    qs = "&".join(f"{k}={v}" for k, v in query.items())
    return f"{base}?{qs}"


class BetMGMSource(DataSource):
    """Fetches odds from BetMGM via direct HTTP requests to the cds-api."""

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None
        self._cache: Dict[str, Tuple[List[OddsEvent], float]] = {}
        self._prefetch_task: Optional[asyncio.Task] = None

    def _ensure_client(self) -> httpx.AsyncClient:
        """Return the shared httpx client, creating it if needed."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers=_HTTP_HEADERS,
                timeout=httpx.Timeout(30.0),
                follow_redirects=True,
            )
        return self._client

    def start_prefetch(self) -> None:
        """Start background prefetch of major sports."""
        self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    async def _prefetch_all(self) -> None:
        await asyncio.sleep(12)  # Stagger after DraftKings
        logger.info("BetMGM: Starting continuous background prefetch")
        all_sports = list(_MGM_API_PARAMS.keys())
        cycle = 0
        while True:
            cycle += 1
            for sport_key in all_sports:
                try:
                    events = await self._fetch_sport(sport_key)
                    self._cache[sport_key] = (events, time.time())
                    logger.info("BetMGM prefetch: %s complete (%d events)", sport_key, len(events))
                except Exception as e:
                    logger.warning("BetMGM prefetch %s failed: %s", sport_key, e)
                await asyncio.sleep(1)
            logger.info("BetMGM: Prefetch cycle #%d complete (%d sports)", cycle, len(all_sports))
            await asyncio.sleep(1)

    # ── Public API ────────────────────────────────────────────────────

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        headers = {"x-requests-remaining": "unlimited"}

        if bookmakers and "betmgm" not in bookmakers:
            return [], headers

        if sport_key not in _MGM_API_PARAMS:
            return [], headers

        # Always serve from cache — prefetch loop keeps it warm.
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _STALE_TTL:
            return cached[0], headers
        return [], headers

    # ── Fetching ──────────────────────────────────────────────────────

    async def _fetch_sport(self, sport_key: str) -> List[OddsEvent]:
        """Fetch fixtures for a sport via the cds-api, trying multiple states."""
        params = _MGM_API_PARAMS.get(sport_key)
        if params is None:
            return []

        client = self._ensure_client()

        for state in _STATE_SUBDOMAINS:
            url = _build_api_url(state, params)
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    events = self._parse_response(data, sport_key)
                    if events:
                        logger.debug(
                            "BetMGM: Got %d events for %s via %s subdomain",
                            len(events), sport_key, state,
                        )
                        return events
                else:
                    logger.debug(
                        "BetMGM: %s subdomain returned %d for %s",
                        state, resp.status_code, sport_key,
                    )
            except httpx.HTTPError as e:
                logger.debug("BetMGM: %s subdomain failed for %s: %s", state, sport_key, e)
                continue

        logger.warning("BetMGM: All state subdomains failed for %s", sport_key)
        return []

    # ── Parsing ───────────────────────────────────────────────────────

    def _parse_response(self, data: dict, sport_key: str) -> List[OddsEvent]:
        """Parse a BetMGM cds-api or ms-api response."""
        # BetMGM responses can have multiple formats:
        # 1. fixture-view: { fixture: { ... }, games: [...] }
        # 2. fixture-list: { fixtures: [...] }
        # 3. Widget/page: { widgets: [{ payload: { fixtures: [...] } }] }

        fixtures = []  # type: List[dict]

        # Format 1: Single fixture
        if "fixture" in data and isinstance(data["fixture"], dict):
            fixtures.append(data)

        # Format 2: Array of fixtures
        elif "fixtures" in data and isinstance(data["fixtures"], list):
            for f in data["fixtures"]:
                fixtures.append(f)

        # Format 3: Widget-based layout (common on sport landing pages)
        elif "widgets" in data and isinstance(data["widgets"], list):
            for widget in data["widgets"]:
                payload = widget.get("payload") or {}
                if "fixtures" in payload:
                    for f in payload["fixtures"]:
                        fixtures.append(f)
                elif "fixture" in payload:
                    fixtures.append(payload)

        # Format 4: The response IS a list of fixtures directly
        elif isinstance(data, list):
            fixtures = data

        # Format 5: items array (some page layouts)
        elif "items" in data and isinstance(data["items"], list):
            for item in data["items"]:
                if "fixture" in item:
                    fixtures.append(item)
                elif "games" in item:
                    fixtures.append(item)

        if not fixtures:
            return []

        sport_title = get_sport_title(sport_key)
        events = []  # type: List[OddsEvent]

        for fixture_data in fixtures:
            event = self._parse_fixture(fixture_data, sport_key, sport_title)
            if event:
                events.append(event)

        return events

    def _parse_fixture(
        self, data: dict, sport_key: str, sport_title: str
    ) -> Optional[OddsEvent]:
        """Parse a single BetMGM fixture into an OddsEvent."""
        # Extract fixture metadata
        fixture = data.get("fixture") or data
        if not isinstance(fixture, dict):
            return None

        # Get participants / team names
        participants = fixture.get("participants") or []
        name = fixture.get("name") or fixture.get("displayName", "")
        start_date = fixture.get("startDate") or fixture.get("cutOffDate", "")

        home_team = ""
        away_team = ""

        for p in participants:
            p_name = p.get("name") or p.get("displayName", "")
            source = (p.get("source") or {})
            is_home = source.get("isHome", False)
            if is_home:
                home_team = p_name
            else:
                if not away_team:
                    away_team = p_name

        # Fallback: parse from fixture name "Team A at Team B" or "Team A vs Team B"
        if not home_team or not away_team:
            if " at " in name:
                parts = name.split(" at ", 1)
                away_team = away_team or parts[0].strip()
                home_team = home_team or parts[1].strip()
            elif " vs " in name.lower():
                parts = re.split(r"\s+vs\.?\s+", name, maxsplit=1, flags=re.IGNORECASE)
                if len(parts) >= 2:
                    away_team = away_team or parts[0].strip()
                    home_team = home_team or parts[1].strip()
            elif " - " in name:
                parts = name.split(" - ", 1)
                if len(parts) >= 2:
                    home_team = home_team or parts[0].strip()
                    away_team = away_team or parts[1].strip()

        if not home_team or not away_team:
            return None

        home_team = resolve_team_name(home_team)
        away_team = resolve_team_name(away_team)

        # Parse markets from games array
        games = data.get("games") or data.get("optionMarkets") or []
        mgm_markets = []  # type: List[Market]
        seen_keys = set()  # type: set

        for game in games:
            if not isinstance(game, dict):
                continue

            # Skip invisible markets
            if not game.get("visibility", "Visible") == "Visible":
                if game.get("visibility") not in (None, "Visible"):
                    continue

            market = self._parse_game_market(game, home_team, away_team)
            if market and market.key not in seen_keys:
                mgm_markets.append(market)
                seen_keys.add(market.key)

        # Also check splitFixtures for additional markets (halves, quarters, etc.)
        split_fixtures = data.get("splitFixtures") or []
        for sf in split_fixtures:
            sf_games = sf.get("games") or sf.get("optionMarkets") or []
            for game in sf_games:
                if not isinstance(game, dict):
                    continue
                market = self._parse_game_market(game, home_team, away_team)
                if market and market.key not in seen_keys:
                    mgm_markets.append(market)
                    seen_keys.add(market.key)

        if not mgm_markets:
            return None

        # Build deep-link URL
        fixture_id = fixture.get("id") or fixture.get("fixtureId", "")
        event_url = "https://sports.nj.betmgm.com"
        if fixture_id:
            event_url = f"https://sports.nj.betmgm.com/en/sports/events/{fixture_id}"

        cid = canonical_event_id(sport_key, home_team, away_team, start_date)
        return OddsEvent(
            id=cid,
            sport_key=sport_key,
            sport_title=sport_title,
            commence_time=start_date,
            home_team=home_team,
            away_team=away_team,
            bookmakers=[
                Bookmaker(
                    key="betmgm",
                    title="BetMGM",
                    markets=mgm_markets,
                    event_url=event_url,
                )
            ],
        )

    def _parse_game_market(
        self, game: dict, home_team: str, away_team: str
    ) -> Optional[Market]:
        """Parse a single BetMGM game/market into our Market model."""
        market_name = game.get("name") or game.get("templateName", "")
        if not market_name:
            return None

        # Classify market name into canonical key
        market_key = self._classify_market_name(market_name)
        if not market_key:
            return None

        # Parse results/options/selections
        results = game.get("results") or game.get("options") or []
        if not results:
            return None

        outcomes = []  # type: List[Outcome]

        for result in results:
            if not isinstance(result, dict):
                continue

            # Get odds — BetMGM uses americanOdds or odds (decimal)
            american_odds = result.get("americanOdds")
            price = _parse_american_odds(american_odds)

            if price is None:
                # Try decimal odds conversion
                decimal_odds = result.get("odds") or result.get("price")
                if decimal_odds is not None:
                    try:
                        price = _parse_decimal_to_american(float(decimal_odds))
                    except (ValueError, TypeError):
                        pass

            if price is None:
                continue

            # Get outcome name
            result_name = result.get("name") or result.get("displayName", "")

            # Get points (spread/total line)
            attr = result.get("attr") or result.get("line")
            points = None
            if attr is not None:
                try:
                    points = float(str(attr))
                except (ValueError, TypeError):
                    pass

            # Also check market-level attr for totals
            if points is None and market_key.startswith("totals"):
                game_attr = game.get("attr")
                if game_attr is not None:
                    try:
                        points = float(str(game_attr))
                    except (ValueError, TypeError):
                        pass

            # Normalize outcome names
            name = self._normalize_outcome_name(
                result_name, market_key, home_team, away_team
            )

            outcomes.append(Outcome(name=name, price=price, point=points))

        if len(outcomes) < 2:
            return None

        return Market(key=market_key, outcomes=outcomes)

    def _classify_market_name(self, name: str) -> Optional[str]:
        """Classify a BetMGM market name into a canonical market key."""
        lower = name.lower()

        # Period detection
        suffix = ""
        if "1st half" in lower or "first half" in lower:
            suffix = "_h1"
        elif "2nd half" in lower or "second half" in lower:
            suffix = "_h2"
        elif "1st quarter" in lower or "1st qtr" in lower:
            suffix = "_q1"
        elif "2nd quarter" in lower or "2nd qtr" in lower:
            suffix = "_q2"
        elif "3rd quarter" in lower or "3rd qtr" in lower:
            suffix = "_q3"
        elif "4th quarter" in lower or "4th qtr" in lower:
            suffix = "_q4"
        elif "1st period" in lower:
            suffix = "_p1"
        elif "2nd period" in lower:
            suffix = "_p2"
        elif "3rd period" in lower:
            suffix = "_p3"
        elif "first 5" in lower or "1st 5" in lower:
            suffix = "_f5"
        elif "1st set" in lower:
            suffix = "_s1"
        elif "2nd set" in lower:
            suffix = "_s2"
        elif "3rd set" in lower:
            suffix = "_s3"

        # Market type classification
        if "moneyline" in lower or "money line" in lower:
            return "h2h" + suffix
        elif "match result" in lower or "match winner" in lower:
            return "h2h" + suffix
        elif "fight winner" in lower or "bout winner" in lower:
            return "h2h"
        elif "go the distance" in lower or "goes the distance" in lower:
            return "fight_to_go_distance"
        elif "total rounds" in lower:
            return "totals"
        elif "spread" in lower or "handicap" in lower:
            # Skip alternate/alt lines
            if "alternate" in lower or "alt " in lower:
                return None
            return "spreads" + suffix
        elif "puck line" in lower:
            return "spreads" + suffix
        elif "run line" in lower:
            return "spreads" + suffix
        elif "total" in lower and "team" not in lower:
            # Skip alternate totals
            if "alternate" in lower or "alt " in lower:
                return None
            return "totals" + suffix
        elif "over/under" in lower or "over / under" in lower:
            return "totals" + suffix

        return None

    def _normalize_outcome_name(
        self, name: str, market_key: str, home_team: str, away_team: str
    ) -> str:
        """Normalize outcome name for consistency."""
        lower = name.lower()

        if market_key == "fight_to_go_distance":
            if "yes" in lower:
                return "Yes"
            elif "no" in lower:
                return "No"
            return name

        if market_key.startswith("totals"):
            if "over" in lower:
                return "Over"
            elif "under" in lower:
                return "Under"

        if market_key.startswith("h2h") or market_key.startswith("spreads"):
            # Try to match to home/away team
            home_lower = home_team.lower()
            away_lower = away_team.lower()

            # Check if the outcome name contains significant team words
            home_words = [w for w in home_lower.split() if len(w) > 2]
            away_words = [w for w in away_lower.split() if len(w) > 2]

            is_home = any(w in lower for w in home_words)
            is_away = any(w in lower for w in away_words)

            if is_home and not is_away:
                return home_team
            elif is_away and not is_home:
                return away_team

            if "draw" in lower or "tie" in lower:
                return "Draw"

        return name

    # ── Event Merging ─────────────────────────────────────────────────

    def _merge_events(
        self,
        existing: List[OddsEvent],
        new_events: List[OddsEvent],
    ) -> List[OddsEvent]:
        """Merge new events into existing list, combining BetMGM markets."""
        by_id = {}  # type: Dict[str, OddsEvent]
        for ev in existing:
            by_id[ev.id] = ev

        for ev in new_events:
            if ev.id in by_id:
                old_ev = by_id[ev.id]
                old_mgm = None
                for bm in old_ev.bookmakers:
                    if bm.key == "betmgm":
                        old_mgm = bm
                        break
                if old_mgm and ev.bookmakers:
                    new_mgm = ev.bookmakers[0]
                    existing_keys = {m.key for m in old_mgm.markets}
                    for m in new_mgm.markets:
                        if m.key not in existing_keys:
                            old_mgm.markets.append(m)
                            existing_keys.add(m.key)
            else:
                by_id[ev.id] = ev

        return list(by_id.values())

    # ── Cleanup ───────────────────────────────────────────────────────

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        self._client = None
