"""
Caesars sportsbook scraper (formerly William Hill).

Uses Playwright headless browser to navigate to Caesars sportsbook pages
and passively capture the api.americanwagering.com API responses.

Strategy:
  1. Try direct API call to api.americanwagering.com (fastest, no browser)
  2. Fallback: Navigate to sportsbook.caesars.com/us/{state}/bet/{sport}/{league}
  3. Intercept XHR responses from americanwagering.com
  4. Parse the events/markets/selections response into OddsEvent format
  5. Cache results with 120s TTL

NOTE: Caesars is geo-restricted to US states where it's licensed. This
scraper will return 0 events when run from outside the US. For US-based
deployment, the scraper provides full market data (spreads, totals,
halves, quarters). ESPN already provides basic Caesars moneylines via
provider ID 38 (williamhill_us).
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
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

# ── Caesars sportsbook navigation URLs per sport_key ─────────────────
# Try multiple URL patterns: some versions work without geo-restriction
_CZR_SPORT_URLS = {
    "basketball_nba": "https://sportsbook.caesars.com/us/nj/bet/basketball/nba",
    "basketball_ncaab": "https://sportsbook.caesars.com/us/nj/bet/basketball/ncaa-basketball",
    "americanfootball_nfl": "https://sportsbook.caesars.com/us/nj/bet/football/nfl",
    "americanfootball_ncaaf": "https://sportsbook.caesars.com/us/nj/bet/football/ncaa-football",
    "icehockey_nhl": "https://sportsbook.caesars.com/us/nj/bet/hockey/nhl",
    "baseball_mlb": "https://sportsbook.caesars.com/us/nj/bet/baseball/mlb",
    "mma_mixed_martial_arts": "https://sportsbook.caesars.com/us/nj/bet/mma/ufc",
    "boxing_boxing": "https://sportsbook.caesars.com/us/nj/bet/boxing",
    "soccer_epl": "https://sportsbook.caesars.com/us/nj/bet/soccer/england-premier-league",
    "soccer_spain_la_liga": "https://sportsbook.caesars.com/us/nj/bet/soccer/spain-la-liga",
    "soccer_germany_bundesliga": "https://sportsbook.caesars.com/us/nj/bet/soccer/germany-bundesliga",
    "soccer_italy_serie_a": "https://sportsbook.caesars.com/us/nj/bet/soccer/italy-serie-a",
    "soccer_france_ligue_one": "https://sportsbook.caesars.com/us/nj/bet/soccer/france-ligue-1",
    "soccer_uefa_champs_league": "https://sportsbook.caesars.com/us/nj/bet/soccer/uefa-champions-league",
    "tennis_atp": "https://sportsbook.caesars.com/us/nj/bet/tennis",
    "tennis_wta": "https://sportsbook.caesars.com/us/nj/bet/tennis",
}

# Alternate state URLs to try if primary geo-blocks
_CZR_ALT_STATES = ["az", "co", "il", "in", "oh", "pa", "va"]

# ── Direct API config ────────────────────────────────────────────────
# Try multiple state locations — some may work from cloud IPs while others geo-block
_API_BASE_TEMPLATE = "https://api.americanwagering.com/regions/us/locations/{state}/brands/czr/sb/v3"
_API_STATES = ["nj", "az", "co", "oh", "va", "il", "in", "pa"]
_API_BASE = _API_BASE_TEMPLATE.format(state="nj")  # default

# Sport slugs for direct API calls (if available)
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

_CACHE_TTL = 120  # seconds — prefetch loop takes ~80s to cycle all sports
_STALE_TTL = 900  # seconds — serve stale data up to 15 minutes (prefetch cycle ~13min)
_JURISDICTION = "nj"


def _decimal_to_american(decimal_odds: float) -> Optional[int]:
    """Convert decimal odds to American odds."""
    if decimal_odds is None or decimal_odds <= 1.0:
        return None
    if decimal_odds >= 2.0:
        return int(round((decimal_odds - 1) * 100))
    else:
        return int(round(-100 / (decimal_odds - 1)))


class CaesarsSource(DataSource):
    """Fetches odds from Caesars via direct HTTP API or Playwright passive capture."""

    def __init__(self, http_only: bool = False):
        self._http_only = http_only
        self._browser = None  # type: Any
        self._context = None  # type: Any
        self._page = None  # type: Any
        self._pw = None  # type: Any
        self._lock = asyncio.Lock()
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
        # Track if direct API works to skip Playwright
        self._direct_api_works = None  # type: Optional[bool]
        # Timestamp when direct API was last marked as failed (retry after 5 min)
        self._direct_api_failed_at = 0.0

    def start_prefetch(self) -> None:
        """Start background prefetch of major sports."""
        self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    async def _prefetch_all(self) -> None:
        await asyncio.sleep(16)  # Stagger after BetMGM
        logger.info("Caesars: Starting continuous background prefetch")
        all_sports = list(_CZR_SPORT_URLS.keys())
        cycle = 0
        while True:
            cycle += 1
            for sport_key in all_sports:
                try:
                    url = _CZR_SPORT_URLS.get(sport_key)
                    if url is None:
                        continue
                    events = []  # type: list

                    # Try direct API first (faster, no browser needed)
                    should_try_api = (
                        self._direct_api_works is not False
                        or (time.time() - self._direct_api_failed_at) > 300
                    )
                    if should_try_api:
                        events = await self._fetch_direct_api(sport_key)
                        if events:
                            if not self._direct_api_works:
                                self._direct_api_works = True
                                logger.info("Caesars: Direct API works, skipping Playwright")

                    # Fallback to Playwright capture (skipped in http_only mode)
                    if not events and not self._http_only:
                        if self._direct_api_works is None or self._direct_api_works is True:
                            self._direct_api_works = False
                            self._direct_api_failed_at = time.time()
                            logger.info("Caesars: Direct API failed, using Playwright")
                        async with self._lock:
                            await self._ensure_browser()
                            events = await self._navigate_and_capture(url, sport_key)

                    self._cache[sport_key] = (events, time.time())
                    logger.info("Caesars prefetch: %s complete (%d events)", sport_key, len(events))
                except Exception as e:
                    logger.warning("Caesars prefetch %s failed: %s", sport_key, e)
                await asyncio.sleep(1)
            logger.info("Caesars: Prefetch cycle #%d complete (%d sports)", cycle, len(all_sports))
            await asyncio.sleep(1)

    async def _ensure_browser(self) -> None:
        if self._page is not None:
            return
        try:
            from playwright.async_api import async_playwright
            self._pw = await async_playwright().start()

            launch_args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
            try:
                self._browser = await self._pw.chromium.launch(
                    headless=True,
                    channel="chrome",
                    args=launch_args,
                )
            except Exception:
                self._browser = await self._pw.chromium.launch(
                    headless=True,
                    args=launch_args,
                )

            self._context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            try:
                from playwright_stealth import Stealth
                stealth = Stealth()
                await stealth.apply_stealth_async(self._context)
            except ImportError:
                await self._context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    window.chrome = {runtime: {}};
                """)
            self._page = await self._context.new_page()
            logger.info("Caesars: Playwright browser launched (stealth mode)")
        except Exception as e:
            logger.warning("Caesars: Failed to launch browser: %s", e)
            self._page = None

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

        url = _CZR_SPORT_URLS.get(sport_key)
        if url is None:
            return [], headers

        # Always serve from cache — prefetch loop keeps it warm.
        # Never fall through to Playwright navigation in the composite context.
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _STALE_TTL:
            return cached[0], headers
        return [], headers

    # ── Direct API Fetching ───────────────────────────────────────────

    async def _fetch_direct_api(self, sport_key: str) -> List[OddsEvent]:
        """Try to fetch events directly from api.americanwagering.com.

        Tries multiple US state API endpoints since some may work from cloud
        IPs while others are geo-restricted.  Once a working state is found,
        it's cached for future calls.
        """
        slug = _CZR_SPORT_SLUGS.get(sport_key)
        if not slug:
            return []

        # If we already know which state works, try it first
        states_to_try = list(_API_STATES)
        if hasattr(self, "_working_state") and self._working_state:
            states_to_try = [self._working_state] + [
                s for s in _API_STATES if s != self._working_state
            ]

        for state in states_to_try:
            try:
                api_base = _API_BASE_TEMPLATE.format(state=state)
                url = f"{api_base}/{slug}/events"
                response = await self._http_client.get(url)
                if response.status_code != 200:
                    continue  # Try next state

                data = response.json()
                if not isinstance(data, (list, dict)):
                    continue

                events = self._parse_api_response(data, sport_key)
                if events:
                    if not hasattr(self, "_working_state") or self._working_state != state:
                        self._working_state = state
                        logger.info(
                            "Caesars: State '%s' API works! %d events for %s",
                            state, len(events), sport_key,
                        )
                    return events
            except Exception:
                continue

        # All states failed
        if not hasattr(self, "_logged_all_states_failed"):
            self._logged_all_states_failed = True
            logger.warning("Caesars: All %d state APIs failed (geo-restricted)", len(_API_STATES))
        return []

    # ── Playwright Fetching ───────────────────────────────────────────

    async def _navigate_and_capture(
        self, url: str, sport_key: str
    ) -> List[OddsEvent]:
        """Navigate to a Caesars page and capture americanwagering API responses."""
        await self._ensure_browser()
        if self._page is None:
            return []

        captured = []  # type: List[dict]

        async def on_response(response):
            try:
                if response.status != 200:
                    return
                resp_url = response.url
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                # Capture from americanwagering.com (Caesars data API)
                # Also capture from caesars-owned APIs and any offering endpoints
                if any(pattern in resp_url for pattern in [
                    "americanwagering.com",
                    "/sb/", "/sportsbook/",
                    "offering", "events", "fixtures",
                    "sportscontent",
                ]):
                    body = await response.text()
                    data = json.loads(body)
                    # Only capture if it looks like event data
                    if isinstance(data, list) or (isinstance(data, dict) and any(
                        k in data for k in ["markets", "events", "competitions",
                                            "fixtures", "selections", "name"]
                    )):
                        captured.append(data)
                        logger.debug("Caesars: captured API response from %s", resp_url[:120])
            except Exception:
                pass

        got_data = asyncio.Event()
        original_on_response = on_response

        async def on_response_wrapped(response):
            await original_on_response(response)
            if captured:
                got_data.set()

        self._page.on("response", on_response_wrapped)
        try:
            await self._page.goto(url, timeout=30000, wait_until="load")
            try:
                await asyncio.wait_for(got_data.wait(), timeout=15.0)
            except asyncio.TimeoutError:
                pass
        except Exception as e:
            logger.warning("Caesars: Navigation to %s failed: %s", url, e)
            self._page.remove_listener("response", on_response_wrapped)
            return []

        self._page.remove_listener("response", on_response_wrapped)

        if not captured:
            logger.warning("Caesars: No API responses captured for %s (possible geo-block?)", url)
            return []

        all_events = []  # type: List[OddsEvent]
        for data in captured:
            events = self._parse_api_response(data, sport_key)
            all_events = self._merge_events(all_events, events)

        return all_events

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
        elif "1st inning" in lower or "first inning" in lower:
            suffix = "_i1"
        elif "1st period" in lower:
            suffix = "_p1"
        elif "2nd period" in lower:
            suffix = "_p2"
        elif "3rd period" in lower:
            suffix = "_p3"
        elif "first 5" in lower or "1st 5" in lower or "first five" in lower:
            suffix = "_f5"
        elif "first 7" in lower or "1st 7" in lower or "first seven" in lower:
            suffix = "_f7"
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
            return "total_rounds"
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
        elif "1st inning" in lower or "first inning" in lower:
            suffix = "_i1"
        elif "1st period" in lower:
            suffix = "_p1"
        elif "2nd period" in lower:
            suffix = "_p2"
        elif "3rd period" in lower:
            suffix = "_p3"
        elif "first 5" in lower or "1st 5" in lower:
            suffix = "_f5"
        elif "first 7" in lower or "1st 7" in lower:
            suffix = "_f7"

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

        if market_key.startswith("totals") or market_key.startswith("team_total") or market_key == "total_rounds":
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
        """Close browser and HTTP client."""
        await self._http_client.aclose()
        if self._page:
            try:
                await self._page.close()
            except Exception:
                pass
        if self._context:
            try:
                await self._context.close()
            except Exception:
                pass
        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
        if self._pw:
            try:
                await self._pw.stop()
            except Exception:
                pass
        self._page = None
        self._context = None
        self._browser = None
        self._pw = None
