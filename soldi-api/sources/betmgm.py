"""
BetMGM sportsbook scraper.

Uses Playwright headless browser to navigate to BetMGM's sportsbook pages
and passively capture the cds-api/bettingoffer responses. The REST API
at sportsapi.{state}.betmgm.com requires auth (401), so we intercept
browser traffic instead.

Strategy:
  1. Navigate to sports.betmgm.com/en/sports/{sport}/betting/{region}/{league}
  2. Intercept XHR responses from cds-api/bettingoffer endpoints
  3. Parse the fixture/game/selection response into OddsEvent format
  4. Cache results with 120s TTL

NOTE: BetMGM is geo-restricted to US states where it's licensed. This
scraper will return 0 events when run from outside the US. For US-based
deployment, the scraper will capture full market data (spreads, totals,
halves, quarters). ESPN already provides basic BetMGM moneylines via
provider ID 40.
"""

import asyncio
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from market_keys import classify_market_type, detect_period_suffix
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

# ── BetMGM sportsbook navigation URLs per sport_key ──────────────────
# Use the main domain (sports.betmgm.com) to avoid state-specific geo-redirects.
# The site redirects to the appropriate state based on location.
_MGM_SPORT_URLS = {
    "basketball_nba": "https://sports.betmgm.com/en/sports/basketball-7/betting/usa-9/nba-6004",
    "basketball_ncaab": "https://sports.betmgm.com/en/sports/basketball-7/betting/usa-9/ncaab-264",
    "americanfootball_nfl": "https://sports.betmgm.com/en/sports/football-11/betting/usa-9/nfl-35",
    "americanfootball_ncaaf": "https://sports.betmgm.com/en/sports/football-11/betting/usa-9/college-football-211",
    "icehockey_nhl": "https://sports.betmgm.com/en/sports/ice-hockey-12/betting/usa-9/nhl-34",
    "baseball_mlb": "https://sports.betmgm.com/en/sports/baseball-23/betting/usa-9/mlb-75",
    "mma_mixed_martial_arts": "https://sports.betmgm.com/en/sports/mma-45/betting/ufc-702",
    "boxing_boxing": "https://sports.betmgm.com/en/sports/boxing-36/betting",
    "soccer_epl": "https://sports.betmgm.com/en/sports/soccer-4/betting/england-14/premier-league-102841",
    "soccer_spain_la_liga": "https://sports.betmgm.com/en/sports/soccer-4/betting/spain-28/la-liga-102846",
    "soccer_germany_bundesliga": "https://sports.betmgm.com/en/sports/soccer-4/betting/germany-17/bundesliga-102842",
    "soccer_italy_serie_a": "https://sports.betmgm.com/en/sports/soccer-4/betting/italy-20/serie-a-102843",
    "soccer_france_ligue_one": "https://sports.betmgm.com/en/sports/soccer-4/betting/france-16/ligue-1-102840",
    "soccer_uefa_champs_league": "https://sports.betmgm.com/en/sports/soccer-4/betting/champions-league-702",
    "tennis_atp": "https://sports.betmgm.com/en/sports/tennis-5/betting/atp-167",
    "tennis_wta": "https://sports.betmgm.com/en/sports/tennis-5/betting/wta-168",
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


class BetMGMSource(DataSource):
    """Fetches odds from BetMGM via Playwright passive response capture."""

    def __init__(self):
        self._browser = None  # type: Any
        self._context = None  # type: Any
        self._page = None  # type: Any
        self._pw = None  # type: Any
        self._lock = asyncio.Lock()
        self._cache = {}  # type: Dict[str, Tuple[List[OddsEvent], float]]
        self._prefetch_task = None  # type: Any

    def start_prefetch(self) -> None:
        """Start background prefetch of major sports."""
        self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    async def _prefetch_all(self) -> None:
        await asyncio.sleep(12)  # Stagger after DraftKings
        logger.info("BetMGM: Starting continuous background prefetch")
        all_sports = list(_MGM_SPORT_URLS.keys())
        cycle = 0
        while True:
            cycle += 1
            for sport_key in all_sports:
                try:
                    url = _MGM_SPORT_URLS.get(sport_key)
                    if url is None:
                        continue
                    async with self._lock:
                        await self._ensure_browser()
                        events = await self._navigate_and_capture(url, sport_key)
                        self._cache[sport_key] = (events, time.time())
                    logger.info("BetMGM prefetch: %s complete (%d events)", sport_key, len(events))
                except Exception as e:
                    logger.warning("BetMGM prefetch %s failed: %s", sport_key, e)
                await asyncio.sleep(1)
            logger.info("BetMGM: Prefetch cycle #%d complete (%d sports)", cycle, len(all_sports))
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
            logger.info("BetMGM: Playwright browser launched (stealth mode)")
        except Exception as e:
            logger.warning("BetMGM: Failed to launch browser: %s", e)
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

        if bookmakers and "betmgm" not in bookmakers:
            return [], headers

        url = _MGM_SPORT_URLS.get(sport_key)
        if url is None:
            return [], headers

        # Always serve from cache — prefetch loop keeps it warm.
        # Never fall through to Playwright navigation in the composite context.
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _STALE_TTL:
            return cached[0], headers
        return [], headers

    # ── Fetching ──────────────────────────────────────────────────────

    async def _navigate_and_capture(
        self, url: str, sport_key: str
    ) -> List[OddsEvent]:
        """Navigate to a BetMGM page and capture cds-api responses."""
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
                # BetMGM uses cds-api for betting offer data
                # Also capture ms-api, offer-api, and other potential endpoints
                if any(pattern in resp_url for pattern in [
                    "cds-api", "bettingoffer", "ms-api", "sports-offer",
                    "offer/api", "fixture", "sportsapi", "sportscontent",
                ]):
                    body = await response.text()
                    data = json.loads(body)
                    captured.append(data)
                    logger.debug("BetMGM: captured API response from %s", resp_url[:120])
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
            logger.warning("BetMGM: Navigation to %s failed: %s", url, e)
            self._page.remove_listener("response", on_response_wrapped)
            return []

        self._page.remove_listener("response", on_response_wrapped)

        if not captured:
            logger.warning("BetMGM: No API responses captured for %s (possible geo-block?)", url)
            return []

        all_events = []  # type: List[OddsEvent]
        for data in captured:
            events = self._parse_response(data, sport_key)
            all_events = self._merge_events(all_events, events)

        return all_events

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
        """Close the browser."""
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
