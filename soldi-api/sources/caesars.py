"""
Caesars sportsbook scraper (formerly William Hill).

Uses direct HTTP calls to api.americanwagering.com to fetch odds data.

Strategy:
  1. Call api.americanwagering.com directly for each sport
  2. Try multiple state jurisdictions (nj, va, az, co, il, in, oh, pa) if primary fails
  3. Parse the events/markets/selections response into OddsEvent format
  4. Cache results with 120s TTL

NOTE: Caesars is geo-restricted to US states where it's licensed. This
scraper will return 0 events when run from outside the US. For US-based
deployment, the scraper provides full market data (spreads, totals,
halves, quarters). ESPN already provides basic Caesars moneylines via
provider ID 38 (williamhill_us).
"""

import asyncio
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

# ── Direct API config ────────────────────────────────────────────────
_API_BASE_TEMPLATE = "https://api.americanwagering.com/regions/us/locations/{state}/brands/czr/sb/v3"

# Sport slugs for direct API calls
_CZR_SPORT_SLUGS = {
    "basketball_nba": "basketball/competitions/nba",
    "basketball_ncaab": "basketball/competitions/ncaa-basketball",
    "americanfootball_nfl": "american-football/competitions/nfl",
    "americanfootball_ncaaf": "american-football/competitions/ncaa-football",
    "icehockey_nhl": "ice-hockey/competitions/nhl",
    "baseball_mlb": "baseball/competitions/mlb",
    "mma_mixed_martial_arts": "mma/competitions/ufc",
    "soccer_epl": "soccer/competitions/england-premier-league",
    "soccer_spain_la_liga": "soccer/competitions/spain-la-liga",
    "soccer_germany_bundesliga": "soccer/competitions/germany-bundesliga",
    "soccer_italy_serie_a": "soccer/competitions/italy-serie-a",
    "soccer_france_ligue_one": "soccer/competitions/france-ligue-1",
    "soccer_uefa_champs_league": "soccer/competitions/uefa-champions-league",
    "tennis_atp": "tennis/competitions/atp",
    "tennis_wta": "tennis/competitions/wta",
    "boxing_boxing": "boxing/competitions/boxing",
}

# Supported sport keys (same set as slugs)
_SUPPORTED_SPORTS = set(_CZR_SPORT_SLUGS.keys())

_CACHE_TTL = 120  # seconds — prefetch loop takes ~80s to cycle all sports
_STALE_TTL = 900  # seconds — serve stale data up to 15 minutes (prefetch cycle ~13min)
_JURISDICTION = "nj"

# Jurisdictions to try in order if primary fails
_JURISDICTIONS = ["nj", "va", "az", "co", "il", "in", "oh", "pa"]


def _decimal_to_american(decimal_odds: float) -> Optional[int]:
    """Convert decimal odds to American odds."""
    if decimal_odds is None or decimal_odds <= 1.0:
        return None
    if decimal_odds >= 2.0:
        return int(round((decimal_odds - 1) * 100))
    else:
        return int(round(-100 / (decimal_odds - 1)))


class CaesarsSource(DataSource):
    """Fetches odds from Caesars via direct HTTP API calls."""

    def __init__(self):
        self._cache = {}  # type: Dict[str, Tuple[List[OddsEvent], float]]
        self._prefetch_task = None  # type: Any
        self._http_client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json",
            },
        )
        # Track which jurisdiction works best
        self._working_jurisdiction = _JURISDICTION  # type: str

    def start_prefetch(self) -> None:
        """Start background prefetch of major sports."""
        self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    async def _prefetch_all(self) -> None:
        await asyncio.sleep(16)  # Stagger after BetMGM
        logger.info("Caesars: Starting continuous background prefetch")
        all_sports = list(_SUPPORTED_SPORTS)
        cycle = 0
        while True:
            cycle += 1
            for sport_key in all_sports:
                try:
                    events = await self._fetch_direct_api(sport_key)
                    self._cache[sport_key] = (events, time.time())
                    logger.info("Caesars prefetch: %s complete (%d events)", sport_key, len(events))
                except Exception as e:
                    logger.warning("Caesars prefetch %s failed: %s", sport_key, e)
                await asyncio.sleep(1)
            logger.info("Caesars: Prefetch cycle #%d complete (%d sports)", cycle, len(all_sports))
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

        if bookmakers and "williamhill_us" not in bookmakers:
            return [], headers

        if sport_key not in _SUPPORTED_SPORTS:
            return [], headers

        # Always serve from cache — prefetch loop keeps it warm.
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _STALE_TTL:
            return cached[0], headers
        return [], headers

    # ── Direct API Fetching ───────────────────────────────────────────

    async def _fetch_direct_api(self, sport_key: str) -> List[OddsEvent]:
        """Fetch events directly from api.americanwagering.com.

        Tries multiple state jurisdictions if the primary one fails.
        """
        slug = _CZR_SPORT_SLUGS.get(sport_key)
        if not slug:
            return []

        # Try working jurisdiction first, then fall back to others
        jurisdictions = [self._working_jurisdiction] + [
            j for j in _JURISDICTIONS if j != self._working_jurisdiction
        ]

        for state in jurisdictions:
            try:
                api_base = _API_BASE_TEMPLATE.format(state=state)
                url = f"{api_base}/{slug}/events"
                response = await self._http_client.get(url)
                if response.status_code != 200:
                    continue

                data = response.json()
                if not isinstance(data, (list, dict)):
                    continue

                events = self._parse_api_response(data, sport_key)
                if events:
                    # Remember which jurisdiction worked
                    if state != self._working_jurisdiction:
                        logger.info(
                            "Caesars: Switching working jurisdiction from %s to %s",
                            self._working_jurisdiction, state,
                        )
                        self._working_jurisdiction = state
                    return events
            except Exception as e:
                logger.debug("Caesars direct API failed for %s/%s: %s", state, sport_key, e)
                continue

        return []

    # ── Parsing ───────────────────────────────────────────────────────

    def _parse_api_response(self, data: Any, sport_key: str) -> List[OddsEvent]:
        """Parse a Caesars/americanwagering API response."""
        events_raw = []  # type: List[dict]

        if isinstance(data, list):
            # Direct list of events
            events_raw = data
        elif isinstance(data, dict):
            # Could be a single event
            if "markets" in data and "name" in data:
                events_raw = [data]
            # Or a wrapper with events list
            elif "events" in data:
                events_raw = data["events"]
            # Or competitions wrapper
            elif "competitions" in data:
                for comp in data["competitions"]:
                    if "events" in comp:
                        events_raw.extend(comp["events"])

        if not events_raw:
            return []

        sport_title = get_sport_title(sport_key)
        events = []  # type: List[OddsEvent]

        for ev_data in events_raw:
            event = self._parse_event(ev_data, sport_key, sport_title)
            if event:
                events.append(event)

        return events

    def _parse_event(
        self, data: dict, sport_key: str, sport_title: str
    ) -> Optional[OddsEvent]:
        """Parse a single Caesars event into an OddsEvent."""
        event_name = data.get("name", "")
        # Remove pipe separators from name
        event_name = event_name.replace("|", "").strip()
        start_time = data.get("startTime") or data.get("startDate", "")

        # Extract team names from participants or name
        competitors = data.get("competitors") or data.get("participants") or []
        home_team = ""
        away_team = ""

        for comp in competitors:
            comp_name = comp.get("name") or comp.get("displayName", "")
            is_home = comp.get("home", False) or comp.get("isHome", False)
            if is_home:
                home_team = comp_name
            else:
                if not away_team:
                    away_team = comp_name

        # Fallback: parse from event name
        if not home_team or not away_team:
            if " @ " in event_name:
                parts = event_name.split(" @ ", 1)
                away_team = away_team or parts[0].strip()
                home_team = home_team or parts[1].strip()
            elif " at " in event_name:
                parts = event_name.split(" at ", 1)
                away_team = away_team or parts[0].strip()
                home_team = home_team or parts[1].strip()
            elif " vs " in event_name.lower():
                parts = re.split(
                    r"\s+vs\.?\s+", event_name, maxsplit=1, flags=re.IGNORECASE
                )
                if len(parts) >= 2:
                    home_team = home_team or parts[0].strip()
                    away_team = away_team or parts[1].strip()
            elif " - " in event_name:
                parts = event_name.split(" - ", 1)
                home_team = home_team or parts[0].strip()
                away_team = away_team or parts[1].strip()

        if not home_team or not away_team:
            return None

        home_team = resolve_team_name(home_team)
        away_team = resolve_team_name(away_team)

        # Parse markets
        raw_markets = data.get("markets") or []
        czr_markets = []  # type: List[Market]
        seen_keys = set()  # type: set

        for mkt_data in raw_markets:
            if not isinstance(mkt_data, dict):
                continue

            # Skip inactive or hidden markets
            if not mkt_data.get("active", True):
                continue
            if not mkt_data.get("display", True):
                continue

            market = self._parse_market(mkt_data, home_team, away_team)
            if market and market.key not in seen_keys:
                czr_markets.append(market)
                seen_keys.add(market.key)

        if not czr_markets:
            return None

        # Build event URL
        event_id = data.get("id", "")
        event_url = "https://sportsbook.caesars.com"
        if event_id:
            event_url = (
                f"https://sportsbook.caesars.com/us/{_JURISDICTION}/"
                f"bet/{event_id}"
            )

        cid = canonical_event_id(sport_key, home_team, away_team, start_time)
        return OddsEvent(
            id=cid,
            sport_key=sport_key,
            sport_title=sport_title,
            commence_time=start_time,
            home_team=home_team,
            away_team=away_team,
            bookmakers=[
                Bookmaker(
                    key="williamhill_us",
                    title="Caesars",
                    markets=czr_markets,
                    event_url=event_url,
                )
            ],
        )

    def _parse_market(
        self, mkt_data: dict, home_team: str, away_team: str
    ) -> Optional[Market]:
        """Parse a single Caesars market."""
        market_name = mkt_data.get("name") or mkt_data.get("templateName", "")
        if not market_name:
            return None

        # Classify market
        market_key = self._classify_market_name(market_name)
        if not market_key:
            # Try team total detection with team names
            market_key = self._classify_team_total(market_name, home_team, away_team)
        if not market_key:
            return None

        # Parse selections
        selections = mkt_data.get("selections") or []
        if not selections:
            return None

        # Get market-level line
        market_line = mkt_data.get("line")

        outcomes = []  # type: List[Outcome]

        for sel in selections:
            if not isinstance(sel, dict):
                continue

            # Get odds - Caesars uses price.d (decimal), price.a (american), or price.f (fractional)
            price_data = sel.get("price") or {}
            price = None

            # Try American odds first
            american = price_data.get("a")
            if american is not None:
                try:
                    price = int(float(str(american)))
                except (ValueError, TypeError):
                    pass

            # Fallback to decimal
            if price is None:
                decimal_odds = price_data.get("d")
                if decimal_odds is not None:
                    try:
                        price = _decimal_to_american(float(decimal_odds))
                    except (ValueError, TypeError):
                        pass

            # Last resort: top-level odds
            if price is None:
                top_odds = sel.get("odds") or sel.get("americanOdds")
                if top_odds is not None:
                    try:
                        price = int(float(str(top_odds)))
                    except (ValueError, TypeError):
                        pass

            if price is None:
                continue

            # Get outcome name
            sel_name = sel.get("name") or sel.get("displayName", "")

            # Get points/line
            points = None
            sel_line = sel.get("line") or sel.get("handicap")
            if sel_line is not None:
                try:
                    points = float(str(sel_line))
                except (ValueError, TypeError):
                    pass

            # Fallback to market-level line for totals
            if points is None and market_line is not None and market_key.startswith("totals"):
                try:
                    points = float(str(market_line))
                except (ValueError, TypeError):
                    pass

            # Normalize name
            name = self._normalize_outcome_name(
                sel_name, market_key, home_team, away_team
            )

            outcomes.append(Outcome(name=name, price=price, point=points))

        if len(outcomes) < 2:
            return None

        return Market(key=market_key, outcomes=outcomes)

    def _classify_market_name(self, name: str) -> Optional[str]:
        """Classify a Caesars market name into a canonical market key."""
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
        elif "total" in lower:
            # Skip alternate totals
            if "alternate" in lower or "alt " in lower:
                return None
            # Team totals contain "team" or specific team references
            if "team" in lower:
                # Try to determine home/away from name
                if "home" in lower:
                    return "team_total_home" + suffix
                elif "away" in lower:
                    return "team_total_away" + suffix
                # Generic team total - skip (need context to determine side)
                return None
            return "totals" + suffix
        elif "over/under" in lower:
            return "totals" + suffix

        return None

    def _classify_team_total(
        self, name: str, home_team: str, away_team: str,
    ) -> Optional[str]:
        """Classify team totals that need team name context."""
        lower = name.lower()
        if "total" not in lower:
            return None

        # Detect period suffix
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

        # Check for team name in market name
        home_lower = home_team.lower()
        away_lower = away_team.lower()

        is_home = False
        is_away = False
        for word in home_lower.split():
            if len(word) > 2 and word in lower:
                is_home = True
                break
        for word in away_lower.split():
            if len(word) > 2 and word in lower:
                is_away = True
                break

        if is_home and not is_away:
            return "team_total_home" + suffix
        elif is_away and not is_home:
            return "team_total_away" + suffix

        return None

    def _normalize_outcome_name(
        self, name: str, market_key: str, home_team: str, away_team: str
    ) -> str:
        """Normalize outcome name."""
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
            home_lower = home_team.lower()
            away_lower = away_team.lower()

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
        """Merge new events into existing list, combining Caesars markets."""
        by_id = {}  # type: Dict[str, OddsEvent]
        for ev in existing:
            by_id[ev.id] = ev

        for ev in new_events:
            if ev.id in by_id:
                old_ev = by_id[ev.id]
                old_czr = None
                for bm in old_ev.bookmakers:
                    if bm.key == "williamhill_us":
                        old_czr = bm
                        break
                if old_czr and ev.bookmakers:
                    new_czr = ev.bookmakers[0]
                    existing_keys = {m.key for m in old_czr.markets}
                    for m in new_czr.markets:
                        if m.key not in existing_keys:
                            old_czr.markets.append(m)
                            existing_keys.add(m.key)
            else:
                by_id[ev.id] = ev

        return list(by_id.values())

    # ── Cleanup ───────────────────────────────────────────────────────

    async def close(self) -> None:
        """Close HTTP client."""
        await self._http_client.aclose()
