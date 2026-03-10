"""
BetOnline sportsbook scraper.

Uses Playwright to establish a browser session (bypass Cloudflare), then
fetches odds data via in-page fetch() calls to BetOnline's offering API.
No login required — public odds are visible without authentication.

Architecture:
  1. Launch headless Chrome + stealth, navigate to ONE page to get CF cookies
  2. Use page.evaluate(fetch(...)) for all subsequent API calls (no more
     per-sport navigation which triggered CF blocks in multi-browser context)
  3. Parse the JSON responses with the same offering format as before
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome, PlayerProp
from sources.base import DataSource
from sources.sport_mapping import canonical_event_id, get_sport_title, resolve_team_name

logger = logging.getLogger(__name__)

SITE_URL = "https://www.betonline.ag"
API_URL = "https://api-offering.betonline.ag/api/offering/Sports/offering-by-league"

# OddsScreen sport_key → BetOnline API (sport, league) params
BETONLINE_API_PARAMS: Dict[str, Tuple[str, str]] = {
    "basketball_nba": ("basketball", "nba"),
    "americanfootball_nfl": ("football", "nfl"),
    "icehockey_nhl": ("hockey", "nhl"),
    "baseball_mlb": ("baseball", "mlb"),
    "basketball_ncaab": ("basketball", "ncaa"),
    "americanfootball_ncaaf": ("football", "ncaa"),
    "mma_mixed_martial_arts": ("martial-arts", "mma"),
    "boxing_boxing": ("boxing", "boxing"),
    "soccer_epl": ("soccer", "epl"),
    "soccer_spain_la_liga": ("soccer", "la-liga"),
    "soccer_germany_bundesliga": ("soccer", "bundesliga"),
    "soccer_italy_serie_a": ("soccer", "serie-a"),
    "soccer_france_ligue_one": ("soccer", "ligue-1"),
    "soccer_uefa_champs_league": ("soccer", "uefa-cl"),
    "tennis_atp": ("tennis", "atp"),
    "tennis_wta": ("tennis", "wta"),
    "soccer_usa_mls": ("soccer", "mls"),
}

# Sport_key → BetOnline URL paths (for event deep-linking only)
BETONLINE_SPORT_URLS: Dict[str, str] = {
    "basketball_nba": "sportsbook/basketball/nba",
    "americanfootball_nfl": "sportsbook/football/nfl",
    "icehockey_nhl": "sportsbook/hockey/nhl",
    "baseball_mlb": "sportsbook/baseball/mlb",
    "basketball_ncaab": "sportsbook/basketball/ncaa",
    "americanfootball_ncaaf": "sportsbook/football/ncaa",
    "mma_mixed_martial_arts": "sportsbook/martial-arts/ufc",
    "boxing_boxing": "sportsbook/boxing",
    "soccer_epl": "sportsbook/soccer/epl/english-premier-league",
    "soccer_spain_la_liga": "sportsbook/soccer/la-liga/spanish-la-liga",
    "soccer_germany_bundesliga": "sportsbook/soccer/bundesliga/german-bundesliga",
    "soccer_italy_serie_a": "sportsbook/soccer/serie-a/italian-serie-a",
    "soccer_france_ligue_one": "sportsbook/soccer/ligue-1/french-ligue-1",
    "soccer_uefa_champs_league": "sportsbook/soccer/uefa-cl/uefa-champions-league",
    "tennis_atp": "sportsbook/tennis",
    "tennis_wta": "sportsbook/tennis",
    "soccer_usa_mls": "sportsbook/soccer/mls",
}

# Keywords that indicate a futures market (skip these)
_FUTURES_KEYWORDS = frozenset([
    "wins", "champion", "coin toss", "futures", "conference",
    "division", "mvp", "award", "super bowl", "world series",
    "stanley cup", "nba finals", "pennant", "cy young", "heisman",
])

# Cache TTL: avoid fetching the same sport too often (seconds)
_CACHE_TTL = 45  # seconds — prefetch loop keeps cache warm every ~15s

# Sports that support period markets (1st half / 1st quarter / 1st period / innings)
_PERIOD_SPORTS = frozenset([
    "basketball_nba", "basketball_ncaab",
    "americanfootball_nfl", "americanfootball_ncaaf",
    "baseball_mlb", "icehockey_nhl",
])


class BetOnlineSource(DataSource):
    """Fetches odds from BetOnline via in-page fetch() API calls.

    Uses Playwright only to establish a browser session (CF cookies).
    All data fetching is done via page.evaluate(fetch(...)), which avoids
    the page navigation issues that caused 0 events in multi-browser contexts.

    In http_only mode, skips Playwright entirely and tries direct httpx
    POST requests to the BetOnline offering API.
    """

    def __init__(self, http_only: bool = False):
        self._http_only = http_only
        self._browser = None  # type: ignore
        self._context = None  # type: ignore
        self._page = None  # type: ignore
        self._pw = None  # type: ignore
        self._lock = asyncio.Lock()
        # Cache: sport_key → (events, timestamp)
        self._cache: Dict[str, Tuple[List[OddsEvent], float]] = {}
        # Props cache: "props:{sport_key}:{event_id}" → (props, timestamp)
        self._props_cache: Dict[str, Tuple[List[PlayerProp], float]] = {}
        self._prefetch_task = None  # type: ignore
        # Track consecutive zero-event cycles for browser health detection
        self._consecutive_zero_cycles: int = 0
        # HTTP client for http_only mode
        self._http_client: Optional[httpx.AsyncClient] = None

    def start_prefetch(self) -> None:
        """Start background prefetch of all supported sports (call after event loop is running)."""
        if self._http_only:
            self._prefetch_task = asyncio.ensure_future(self._prefetch_all_http())
        else:
            self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    # ------------------------------------------------------------------
    # HTTP-only mode: direct httpx calls (no Playwright)
    # ------------------------------------------------------------------

    async def _ensure_http_client(self) -> None:
        """Create httpx client for direct API calls."""
        if self._http_client is not None:
            return
        self._http_client = httpx.AsyncClient(
            timeout=20.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/131.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "gsetting": "bolsassite",
                "utc-offset": "300",
                "Origin": SITE_URL,
                "Referer": SITE_URL + "/sportsbook/basketball/nba",
            },
        )

    async def _api_call_http(
        self, sport: str, league: str, period: Optional[int] = None
    ) -> Optional[dict]:
        """Make a direct HTTP POST to the BetOnline offering API."""
        await self._ensure_http_client()
        try:
            body = {"Sport": sport, "League": league, "ScheduleText": None, "filterTime": 0}
            if period is not None:
                body["Period"] = period

            resp = await self._http_client.post(API_URL, json=body)
            if resp.status_code == 403:
                logger.warning("BetOnline HTTP: 403 Cloudflare block for %s/%s", sport, league)
                return None
            if resp.status_code != 200:
                logger.info("BetOnline HTTP: %d for %s/%s", resp.status_code, sport, league)
                return None

            data = resp.json()
            if not isinstance(data, dict):
                return None
            return data
        except Exception as e:
            logger.warning("BetOnline HTTP: API call error for %s/%s: %s", sport, league, e)
            return None

    async def _fetch_sport_http(self, sport_key: str, sport: str, league: str) -> List[OddsEvent]:
        """Fetch odds for one sport via direct HTTP."""
        full_game_data = await self._api_call_http(sport, league)
        if full_game_data is None:
            return []

        events = self._parse_offering(full_game_data, sport_key)

        # Fetch period markets for applicable sports
        if sport_key in _PERIOD_SPORTS and events:
            if sport_key.startswith("basketball") or sport_key.startswith("americanfootball"):
                h1_data = await self._api_call_http(sport, league, period=1)
                if h1_data:
                    h1_events = self._parse_offering(h1_data, sport_key, period_suffix="_h1")
                    self._merge_period_markets(events, h1_events)

                q1_data = await self._api_call_http(sport, league, period=3)
                if q1_data:
                    q1_events = self._parse_offering(q1_data, sport_key, period_suffix="_q1")
                    self._merge_period_markets(events, q1_events)

            if sport_key == "icehockey_nhl":
                p1_data = await self._api_call_http(sport, league, period=1)
                if p1_data:
                    p1_events = self._parse_offering(p1_data, sport_key, period_suffix="_p1")
                    self._merge_period_markets(events, p1_events)

            if sport_key == "baseball_mlb":
                i1_data = await self._api_call_http(sport, league, period=1)
                if i1_data:
                    i1_events = self._parse_offering(i1_data, sport_key, period_suffix="_i1")
                    self._merge_period_markets(events, i1_events)
                f5_data = await self._api_call_http(sport, league, period=3)
                if f5_data:
                    f5_events = self._parse_offering(f5_data, sport_key, period_suffix="_f5")
                    self._merge_period_markets(events, f5_events)
                f7_data = await self._api_call_http(sport, league, period=5)
                if f7_data:
                    f7_events = self._parse_offering(f7_data, sport_key, period_suffix="_f7")
                    self._merge_period_markets(events, f7_events)

        return events

    async def _prefetch_all_http(self) -> None:
        """Background HTTP-only prefetch loop."""
        await asyncio.sleep(5)
        logger.info("BetOnline: Starting HTTP-only background prefetch")
        cycle = 0
        while True:
            cycle += 1
            cycle_total = 0
            for sport_key, (sport, league) in BETONLINE_API_PARAMS.items():
                try:
                    events = await self._fetch_sport_http(sport_key, sport, league)
                    if events:
                        self._cache[sport_key] = (events, time.time())
                        cycle_total += len(events)
                    logger.info("BetOnline HTTP prefetch: %d events for %s", len(events), sport_key)
                except Exception as e:
                    logger.warning("BetOnline HTTP prefetch failed for %s: %s", sport_key, e)
                await asyncio.sleep(0.5)

            logger.info(
                "BetOnline HTTP: Prefetch cycle #%d complete (%d total events)", cycle, cycle_total,
            )

            # If 403 blocked, slow down retries
            if cycle_total == 0:
                await asyncio.sleep(120)
            else:
                await asyncio.sleep(15)

    async def _prefetch_all(self) -> None:
        """Background task: continuously warm up cache for all supported sports."""
        await asyncio.sleep(8)  # Stagger browser launch
        logger.info("BetOnline: Starting continuous background prefetch")
        cycle = 0
        while True:
            cycle += 1
            cycle_total_events = 0
            async with self._lock:
                try:
                    await self._ensure_browser()
                    if self._page is None:
                        logger.warning("BetOnline: No browser page, skipping cycle %d", cycle)
                        await asyncio.sleep(30)
                        continue

                    for sport_key, (sport, league) in BETONLINE_API_PARAMS.items():
                        try:
                            events = await self._fetch_sport_api(sport_key, sport, league)
                            self._cache[sport_key] = (events, time.time())
                            cycle_total_events += len(events)
                            logger.info("BetOnline prefetch: %d events for %s", len(events), sport_key)
                        except Exception as e:
                            logger.warning("BetOnline prefetch failed for %s: %s", sport_key, e)
                        # Small pause between API calls to avoid rate limiting
                        await asyncio.sleep(0.5)
                except Exception as e:
                    logger.warning("BetOnline prefetch error: %s", e)

            # Track browser health: if we get 0 events for 2+ consecutive cycles,
            # the browser session is likely stale — restart it
            if cycle_total_events == 0:
                self._consecutive_zero_cycles += 1
                if self._consecutive_zero_cycles >= 2:
                    logger.warning(
                        "BetOnline: %d consecutive zero-event cycles — restarting browser",
                        self._consecutive_zero_cycles,
                    )
                    await self._close_browser()
                    self._consecutive_zero_cycles = 0
                    await asyncio.sleep(10)
            else:
                self._consecutive_zero_cycles = 0

            logger.info(
                "BetOnline: Prefetch cycle #%d complete (%d total events)",
                cycle, cycle_total_events,
            )
            await asyncio.sleep(15)  # Keep cache warm — 17 sports × 0.5s ≈ 9s + 15s pause

    async def _ensure_browser(self) -> None:
        """Launch Playwright browser with stealth mode to bypass Cloudflare."""
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
                logger.info("BetOnline: Launched system Chrome")
            except Exception:
                self._browser = await self._pw.chromium.launch(
                    headless=True,
                    args=launch_args,
                )
                logger.info("BetOnline: Launched bundled Chromium (fallback)")

            self._context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            # Apply stealth evasions to bypass Cloudflare bot detection
            try:
                from playwright_stealth import Stealth
                stealth = Stealth()
                await stealth.apply_stealth_async(self._context)
            except ImportError:
                await self._context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    window.chrome = {runtime: {}};
                """)
            self._page = await self._context.new_page()
            logger.info("BetOnline: Playwright browser launched (stealth mode)")

            # Navigate to sportsbook to establish CF session cookies.
            # This is the ONLY page navigation we do — all subsequent
            # data fetching uses in-page fetch() calls.
            try:
                logger.info("BetOnline: Loading sportsbook page to establish session")
                await self._page.goto(
                    f"{SITE_URL}/sportsbook/basketball/nba",
                    timeout=45000,
                    wait_until="load",
                )
                # Wait for CF challenge to resolve and JS to execute
                await asyncio.sleep(10)
                title = await self._page.title()
                logger.info("BetOnline: Session established (title: %r)", title)
            except Exception as e:
                logger.warning("BetOnline: Session setup navigation failed: %s", e)

        except Exception as e:
            logger.warning("BetOnline: Failed to launch browser: %s", e)
            self._page = None

    # ------------------------------------------------------------------
    # Core API fetch method (in-page fetch)
    # ------------------------------------------------------------------

    async def _fetch_sport_api(
        self, sport_key: str, sport: str, league: str
    ) -> List[OddsEvent]:
        """Fetch odds for one sport via in-page fetch() API call.

        Makes the API call from within the browser's JavaScript context,
        which shares the session cookies and avoids CF blocks.

        For basketball/football, also fetches 1st-half and 1st-quarter
        lines via the Period parameter.
        """
        if self._page is None:
            return []

        # Fetch full-game lines
        full_game_data = await self._api_call(sport, league)
        if full_game_data is None:
            logger.info("BetOnline: API returned None for %s/%s", sport, league)
            return []

        # Log how many game offerings were returned for debugging
        game_offering = full_game_data.get("GameOffering")
        if game_offering is None:
            logger.info("BetOnline: %s/%s returned null GameOffering (no events)", sport, league)
        else:
            game_descs = game_offering.get("GamesDescription", []) if isinstance(game_offering, dict) else []
            logger.info("BetOnline: %s/%s returned %d game descriptions", sport, league, len(game_descs or []))

        events = self._parse_offering(full_game_data, sport_key)

        # Fetch period markets for applicable sports
        if sport_key in _PERIOD_SPORTS and events:
            # 1st Half (Period=1) — basketball, football, hockey
            if sport_key.startswith("basketball") or sport_key.startswith("americanfootball"):
                h1_data = await self._api_call(sport, league, period=1)
                if h1_data:
                    h1_events = self._parse_offering(h1_data, sport_key, period_suffix="_h1")
                    self._merge_period_markets(events, h1_events)

            # 1st Quarter (Period=3) — only basketball/football
            if sport_key.startswith("basketball") or sport_key.startswith("americanfootball"):
                q1_data = await self._api_call(sport, league, period=3)
                if q1_data:
                    q1_events = self._parse_offering(q1_data, sport_key, period_suffix="_q1")
                    self._merge_period_markets(events, q1_events)

            # 1st Period (Period=1) — hockey
            if sport_key == "icehockey_nhl":
                p1_data = await self._api_call(sport, league, period=1)
                if p1_data:
                    p1_events = self._parse_offering(p1_data, sport_key, period_suffix="_p1")
                    self._merge_period_markets(events, p1_events)

            # MLB innings/sub-periods
            if sport_key == "baseball_mlb":
                # 1st Inning (Period=1)
                i1_data = await self._api_call(sport, league, period=1)
                if i1_data:
                    i1_events = self._parse_offering(i1_data, sport_key, period_suffix="_i1")
                    self._merge_period_markets(events, i1_events)
                # First 5 Innings (Period=3)
                f5_data = await self._api_call(sport, league, period=3)
                if f5_data:
                    f5_events = self._parse_offering(f5_data, sport_key, period_suffix="_f5")
                    self._merge_period_markets(events, f5_events)
                # First 7 Innings (Period=5)
                f7_data = await self._api_call(sport, league, period=5)
                if f7_data:
                    f7_events = self._parse_offering(f7_data, sport_key, period_suffix="_f7")
                    self._merge_period_markets(events, f7_events)

        return events

    async def _api_call(
        self, sport: str, league: str, period: Optional[int] = None
    ) -> Optional[dict]:
        """Make a single API call via page.evaluate(fetch(...)).

        Returns the parsed JSON or None on failure.
        """
        if self._page is None:
            return None

        try:
            # Build the payload in JS
            js_code = """
                async ([sport, league, period]) => {
                    try {
                        const body = {Sport: sport, League: league, ScheduleText: null, filterTime: 0};
                        if (period !== null) body.Period = period;
                        const r = await fetch("%s", {
                            method: "POST",
                            headers: {
                                "Accept": "application/json",
                                "Content-Type": "application/json",
                                "gsetting": "bolsassite",
                                "utc-offset": "300",
                            },
                            body: JSON.stringify(body),
                        });
                        if (!r.ok) return {error: r.status};
                        return await r.json();
                    } catch(e) {
                        return {error: e.message};
                    }
                }
            """ % API_URL

            result = await self._page.evaluate(js_code, [sport, league, period])

            if not isinstance(result, dict):
                return None

            # Check for error responses
            if "error" in result:
                err = result["error"]
                if err == 403:
                    # Session may have expired — flag for browser restart
                    logger.warning(
                        "BetOnline: API returned 403 for %s/%s — session may be stale",
                        sport, league,
                    )
                else:
                    logger.debug("BetOnline: API error for %s/%s: %s", sport, league, err)
                return None

            return result

        except Exception as e:
            logger.warning("BetOnline: API call error for %s/%s: %s", sport, league, e)
            return None

    # ------------------------------------------------------------------
    # Public interface: get_odds
    # ------------------------------------------------------------------

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        if bookmakers and "betonlineag" not in bookmakers:
            return [], {"x-requests-remaining": "unlimited"}

        api_params = BETONLINE_API_PARAMS.get(sport_key)
        if api_params is None:
            return [], {"x-requests-remaining": "unlimited"}

        # Always serve from cache — prefetch loop keeps it warm.
        # Never fall through to Playwright navigation in the composite context.
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _CACHE_TTL:
            return cached[0], {"x-requests-remaining": "unlimited"}
        # Serve stale data (up to 5 min) if prefetch hasn't refreshed yet
        if cached:
            return cached[0], {"x-requests-remaining": "unlimited"}
        return [], {"x-requests-remaining": "unlimited"}

    # ------------------------------------------------------------------
    # JSON parsing (unchanged from original)
    # ------------------------------------------------------------------

    def _parse_offering(
        self, data: dict, sport_key: str, period_suffix: str = ""
    ) -> List[OddsEvent]:
        """Parse BetOnline's offering-by-league response into OddsEvent list.

        Args:
            period_suffix: If set (e.g. "_h1"), appended to market keys so that
                           1st-half lines become "h2h_h1", "spreads_h1", etc.
        """
        sport_title = get_sport_title(sport_key)
        events = []

        game_offering = data.get("GameOffering")
        if not game_offering:
            return []

        games_desc = game_offering.get("GamesDescription", [])

        for gd in games_desc:
            game = gd.get("Game")
            if not game:
                continue

            raw_away = game.get("AwayTeam", "").strip()
            raw_home = game.get("HomeTeam", "").strip()
            if not raw_away or not raw_home:
                continue
            away_team = resolve_team_name(raw_away)
            home_team = resolve_team_name(raw_home)

            # Skip futures
            schedule_text = (game.get("ScheduleText") or "").lower()
            if "future" in schedule_text:
                continue
            combined = (away_team + " " + home_team).lower()
            if any(kw in combined for kw in _FUTURES_KEYWORDS):
                continue

            # Parse commence time from WagerCutOff
            cutoff = game.get("WagerCutOff", "")
            commence_time = self._parse_time(cutoff)

            # Parse markets
            away_line = game.get("AwayLine", {})
            home_line = game.get("HomeLine", {})
            bol_markets = []

            # Moneyline
            ml_market = self._parse_moneyline(away_line, home_line, away_team, home_team)
            if ml_market:
                if period_suffix:
                    ml_market = Market(key="h2h" + period_suffix, outcomes=ml_market.outcomes)
                bol_markets.append(ml_market)

            # Spreads
            spread_market = self._parse_spread(away_line, home_line, away_team, home_team)
            if spread_market:
                if period_suffix:
                    spread_market = Market(key="spreads" + period_suffix, outcomes=spread_market.outcomes)
                bol_markets.append(spread_market)

            # Totals – game-level TotalLine (not per-side AwayLine)
            game_total = game.get("TotalLine", {})
            total_market = self._parse_total(game_total)
            if total_market:
                if period_suffix:
                    total_market = Market(key="totals" + period_suffix, outcomes=total_market.outcomes)
                bol_markets.append(total_market)

            # Team totals — per-side TotalLine inside AwayLine/HomeLine
            away_tt = away_line.get("TotalLine", {})
            home_tt = home_line.get("TotalLine", {})
            away_tt_market = self._parse_team_total(away_tt)
            if away_tt_market:
                away_tt_market = Market(key="team_total_away" + period_suffix, outcomes=away_tt_market.outcomes)
                bol_markets.append(away_tt_market)
            home_tt_market = self._parse_team_total(home_tt)
            if home_tt_market:
                home_tt_market = Market(key="team_total_home" + period_suffix, outcomes=home_tt_market.outcomes)
                bol_markets.append(home_tt_market)

            if not bol_markets:
                continue

            # Build event URL (use GameID for event-level deep linking)
            game_id = game.get("GameID") or game.get("GameId")
            sport_url = BETONLINE_SPORT_URLS.get(sport_key)
            if game_id and sport_url:
                event_url = f"{SITE_URL}/{sport_url}/game-{game_id}"
            elif sport_url:
                event_url = f"{SITE_URL}/{sport_url}"
            else:
                event_url = None

            cid = canonical_event_id(sport_key, home_team, away_team, commence_time)

            events.append(OddsEvent(
                id=cid,
                sport_key=sport_key,
                sport_title=sport_title,
                commence_time=commence_time,
                home_team=home_team,
                away_team=away_team,
                bookmakers=[
                    Bookmaker(
                        key="betonlineag",
                        title="BetOnline",
                        markets=bol_markets,
                        event_url=event_url,
                    )
                ],
            ))

        return events

    @staticmethod
    def _merge_period_markets(
        full_game_events: List[OddsEvent], h1_events: List[OddsEvent]
    ) -> None:
        """Merge 1st-half/quarter markets from *h1_events* into *full_game_events* in-place.

        Matches events by canonical ID and appends any period markets (h2h_h1,
        spreads_h1, totals_h1, etc.) to the bookmaker's market list.
        """
        h1_by_id = {}  # type: Dict[str, OddsEvent]
        for ev in h1_events:
            h1_by_id[ev.id] = ev

        for ev in full_game_events:
            h1_ev = h1_by_id.get(ev.id)
            if not h1_ev:
                continue
            fg_bol = None
            for bm in ev.bookmakers:
                if bm.key == "betonlineag":
                    fg_bol = bm
                    break
            if not fg_bol:
                continue
            h1_bol = None
            for bm in h1_ev.bookmakers:
                if bm.key == "betonlineag":
                    h1_bol = bm
                    break
            if not h1_bol:
                continue
            existing_keys = {m.key for m in fg_bol.markets}
            for mkt in h1_bol.markets:
                if mkt.key not in existing_keys:
                    fg_bol.markets.append(mkt)

    # ------------------------------------------------------------------
    # Market parsers
    # ------------------------------------------------------------------

    def _parse_moneyline(
        self,
        away_line: dict,
        home_line: dict,
        away_team: str,
        home_team: str,
    ) -> Optional[Market]:
        """Extract moneyline market from AwayLine/HomeLine."""
        away_ml = away_line.get("MoneyLine", {}).get("Line")
        home_ml = home_line.get("MoneyLine", {}).get("Line")

        if away_ml is None or home_ml is None:
            return None
        if away_ml == 0 and home_ml == 0:
            return None

        outcomes = [
            Outcome(name=home_team, price=int(home_ml)),
            Outcome(name=away_team, price=int(away_ml)),
        ]

        # Check for draw (soccer)
        draw_line = away_line.get("DrawLine", {}).get("Line")
        if draw_line and draw_line != 0:
            outcomes.append(Outcome(name="Draw", price=int(draw_line)))

        return Market(key="h2h", outcomes=outcomes)

    def _parse_spread(
        self,
        away_line: dict,
        home_line: dict,
        away_team: str,
        home_team: str,
    ) -> Optional[Market]:
        """Extract spread/handicap market."""
        away_spread = away_line.get("SpreadLine", {})
        home_spread = home_line.get("SpreadLine", {})

        away_point = away_spread.get("Point")
        away_odds = away_spread.get("Line")
        home_point = home_spread.get("Point")
        home_odds = home_spread.get("Line")

        if not away_point or not away_odds or not home_point or not home_odds:
            return None
        if away_point == 0 and home_point == 0:
            return None

        return Market(
            key="spreads",
            outcomes=[
                Outcome(name=home_team, price=int(home_odds), point=float(home_point)),
                Outcome(name=away_team, price=int(away_odds), point=float(away_point)),
            ],
        )

    def _parse_total(self, game_total: dict) -> Optional[Market]:
        """Extract totals (over/under) market from game-level TotalLine."""
        total = game_total.get("TotalLine", {})
        point = total.get("Point")
        over_odds = total.get("Over", {}).get("Line")
        under_odds = total.get("Under", {}).get("Line")

        if not point or not over_odds or not under_odds:
            return None
        if point == 0:
            return None

        return Market(
            key="totals",
            outcomes=[
                Outcome(name="Over", price=int(over_odds), point=float(point)),
                Outcome(name="Under", price=int(under_odds), point=float(point)),
            ],
        )

    def _parse_team_total(self, team_total: dict) -> Optional[Market]:
        """Extract team total (over/under) from per-side TotalLine."""
        total = team_total.get("TotalLine", {})
        if not total:
            total = team_total  # Fallback if nested differently
        point = total.get("Point")
        over_odds = total.get("Over", {}).get("Line")
        under_odds = total.get("Under", {}).get("Line")

        if not point or not over_odds or not under_odds:
            return None
        if point == 0:
            return None

        return Market(
            key="team_total",  # key overridden by caller
            outcomes=[
                Outcome(name="Over", price=int(over_odds), point=float(point)),
                Outcome(name="Under", price=int(under_odds), point=float(point)),
            ],
        )

    def _parse_time(self, cutoff: str) -> str:
        """Convert BetOnline's WagerCutOff to ISO 8601."""
        if not cutoff:
            return ""
        try:
            dt = datetime.fromisoformat(cutoff)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            return cutoff

    # ------------------------------------------------------------------
    # Player props (via in-page fetch — no page navigation)
    # ------------------------------------------------------------------

    async def get_player_props(
        self, sport_key: str, event_id: str
    ) -> List[PlayerProp]:
        """Fetch player props from BetOnline's prop builder section.

        Strategy:
        1. Navigate to the sport's props page
        2. Capture API responses (offering/prop endpoints) passively
        3. Parse captured JSON responses with multiple structure strategies
        4. If API capture yields nothing, scrape the rendered DOM as fallback
        """
        cache_key = "props:%s:%s" % (sport_key, event_id)
        cached = self._props_cache.get(cache_key)
        if cached and (time.time() - cached[1]) < _CACHE_TTL:
            return cached[0]

        url_path = BETONLINE_SPORT_URLS.get(sport_key)
        if not url_path:
            return []

        # Try to acquire lock; return stale cache or empty if busy
        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=2.0)
        except asyncio.TimeoutError:
            if cached:
                return cached[0]
            return []

        try:
            # Double-check cache after acquiring lock
            cached = self._props_cache.get(cache_key)
            if cached and (time.time() - cached[1]) < _CACHE_TTL:
                return cached[0]

            try:
                await self._ensure_browser()
                if self._page is None:
                    return []

                captured_responses = []  # type: List[Tuple[str, dict]]

                async def on_response(response):
                    url_lower = response.url.lower()
                    if response.status != 200:
                        return
                    if any(
                        kw in url_lower
                        for kw in ("prop", "player", "builder", "alt-line",
                                   "offering", "market", "odds", "event",
                                   "game", "bet-offer", "wager")
                    ):
                        try:
                            ct = response.headers.get("content-type", "")
                            if "json" in ct or "javascript" in ct or "text" in ct:
                                body = await response.text()
                                data = json.loads(body)
                                captured_responses.append((response.url, data))
                        except Exception:
                            pass

                self._page.on("response", on_response)
                try:
                    props_url = "%s/%s/props" % (SITE_URL, url_path)
                    await self._page.goto(props_url, timeout=30000)

                    for i in range(30):
                        if len(captured_responses) >= 2:
                            break
                        if captured_responses and i >= 20:
                            break
                        await asyncio.sleep(0.5)
                finally:
                    self._page.remove_listener("response", on_response)

                sport_url_base = "%s/%s" % (SITE_URL, url_path)
                props = []  # type: List[PlayerProp]

                # Strategy 1: Parse captured API responses
                for resp_url, resp_data in captured_responses:
                    parsed = self._parse_props_api(resp_data, sport_url_base)
                    if len(parsed) > len(props):
                        props = parsed

                # Strategy 2: Try offering-format responses for prop markets
                if not props:
                    for resp_url, resp_data in captured_responses:
                        parsed = self._parse_props_from_offering(
                            resp_data, sport_url_base
                        )
                        if len(parsed) > len(props):
                            props = parsed

                # Strategy 3: DOM scraping fallback
                if not props:
                    try:
                        await asyncio.sleep(2)
                        props = await self._scrape_props_dom(sport_url_base)
                    except Exception as e:
                        logger.debug("BetOnline: DOM prop scrape error: %s", e)

                self._props_cache[cache_key] = (props, time.time())
                logger.info(
                    "BetOnline: %d props for %s / %s (from %d API responses)",
                    len(props), sport_key, event_id, len(captured_responses),
                )

                # After props navigation, re-navigate to sportsbook to restore
                # the session context for subsequent API fetch() calls
                try:
                    await self._page.goto(
                        f"{SITE_URL}/sportsbook/basketball/nba",
                        timeout=30000,
                        wait_until="load",
                    )
                    await asyncio.sleep(5)
                except Exception:
                    pass

                return props

            except Exception as e:
                logger.warning("BetOnline: Props failed for %s: %s", sport_key, e)
                return []
        finally:
            self._lock.release()

    def _parse_props_api(
        self, data, sport_url_base: str
    ) -> List[PlayerProp]:
        """Parse BetOnline prop data from various JSON structures."""
        props = []  # type: List[PlayerProp]

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    props.extend(self._extract_prop_items(item, sport_url_base))
            return props

        if not isinstance(data, dict):
            return []

        for key in ("Props", "props", "PlayerProps", "playerProps",
                     "players", "Items", "items", "Data", "data",
                     "Offerings", "offerings", "Markets", "markets",
                     "Lines", "lines", "Bets", "bets", "Results", "results"):
            val = data.get(key)
            if isinstance(val, list) and len(val) > 0:
                for item in val:
                    if isinstance(item, dict):
                        props.extend(self._extract_prop_items(item, sport_url_base))
                if props:
                    return props
            elif isinstance(val, dict):
                for sub_key in ("Items", "items", "Props", "props",
                                "Lines", "lines", "Data", "data"):
                    sub_val = val.get(sub_key)
                    if isinstance(sub_val, list) and len(sub_val) > 0:
                        for item in sub_val:
                            if isinstance(item, dict):
                                props.extend(
                                    self._extract_prop_items(item, sport_url_base)
                                )
                        if props:
                            return props

        props.extend(self._extract_prop_items(data, sport_url_base))
        return props

    def _extract_prop_items(
        self, item: dict, sport_url_base: str
    ) -> List[PlayerProp]:
        """Extract PlayerProp(s) from a single JSON object."""
        props = []  # type: List[PlayerProp]

        player = ""
        for field in ("PlayerName", "playerName", "player", "name",
                       "Player", "participant", "Participant",
                       "Team1ID", "contestantName"):
            val = item.get(field)
            if val and isinstance(val, str) and len(val.strip()) > 1:
                player = val.strip()
                break

        if not player:
            for field in ("selections", "outcomes", "options", "bets",
                           "Selections", "Outcomes"):
                sub = item.get(field)
                if isinstance(sub, list):
                    for s in sub:
                        if isinstance(s, dict):
                            props.extend(
                                self._extract_prop_items(s, sport_url_base)
                            )
                    return props
            return []

        stat_raw = ""
        for field in ("StatType", "statType", "stat", "category", "market",
                       "MarketType", "marketType", "Type", "type",
                       "betType", "BetType", "Team2ID", "description"):
            val = item.get(field)
            if val and isinstance(val, str):
                stat_raw = val
                break
        stat_type = self._classify_stat(stat_raw)

        line = 0.0
        for field in ("Line", "line", "value", "handicap", "Handicap",
                       "point", "Point", "TotalPoints", "threshold",
                       "points", "spread"):
            val = item.get(field)
            if val is not None:
                try:
                    line = float(val)
                    if line > 0:
                        break
                except (ValueError, TypeError):
                    pass

        if line <= 0:
            return []

        over_odds = None
        for field in ("OverOdds", "overOdds", "over", "overPrice", "OverPrice",
                       "TtlPtsAdj1", "overLine"):
            val = item.get(field)
            if val is not None:
                try:
                    over_odds = int(float(val))
                    break
                except (ValueError, TypeError):
                    pass
        if over_odds is None:
            over_obj = item.get("Over") or item.get("over")
            if isinstance(over_obj, dict):
                for field in ("Line", "line", "Odds", "odds", "Price", "price"):
                    val = over_obj.get(field)
                    if val is not None:
                        try:
                            over_odds = int(float(val))
                            break
                        except (ValueError, TypeError):
                            pass

        under_odds = None
        for field in ("UnderOdds", "underOdds", "under", "underPrice", "UnderPrice",
                       "TtlPtsAdj2", "underLine"):
            val = item.get(field)
            if val is not None:
                try:
                    under_odds = int(float(val))
                    break
                except (ValueError, TypeError):
                    pass
        if under_odds is None:
            under_obj = item.get("Under") or item.get("under")
            if isinstance(under_obj, dict):
                for field in ("Line", "line", "Odds", "odds", "Price", "price"):
                    val = under_obj.get(field)
                    if val is not None:
                        try:
                            under_odds = int(float(val))
                            break
                        except (ValueError, TypeError):
                            pass

        if over_odds is None and under_odds is None:
            return []

        if over_odds is not None:
            props.append(PlayerProp(
                player_name=player,
                stat_type=stat_type,
                line=line,
                price=over_odds,
                description="Over",
                bookmaker_key="betonlineag",
                bookmaker_title="BetOnline",
                event_url=sport_url_base,
            ))
        if under_odds is not None:
            props.append(PlayerProp(
                player_name=player,
                stat_type=stat_type,
                line=line,
                price=under_odds,
                description="Under",
                bookmaker_key="betonlineag",
                bookmaker_title="BetOnline",
                event_url=sport_url_base,
            ))

        return props

    def _parse_props_from_offering(
        self, data: dict, sport_url_base: str
    ) -> List[PlayerProp]:
        """Parse props from BetOnline's GameOffering format."""
        props = []  # type: List[PlayerProp]

        if not isinstance(data, dict):
            return []

        game_offering = data.get("GameOffering")
        if not game_offering:
            return []

        games_desc = game_offering.get("GamesDescription", [])
        if not isinstance(games_desc, list):
            return []

        for gd in games_desc:
            if not isinstance(gd, dict):
                continue
            game = gd.get("Game")
            if not game or not isinstance(game, dict):
                continue

            schedule_text = (game.get("ScheduleText") or "").lower()
            if "prop" not in schedule_text:
                desc = (game.get("Description") or "").lower()
                game_type = (game.get("GameType") or "").lower()
                if "prop" not in desc and "player" not in desc and "prop" not in game_type:
                    continue

            player = (game.get("AwayTeam") or "").strip()
            if not player:
                continue

            stat_raw = (game.get("HomeTeam") or game.get("Description") or "").strip()
            stat_type = self._classify_stat(stat_raw)

            game_total = game.get("TotalLine", {})
            total = game_total.get("TotalLine", {}) if isinstance(game_total, dict) else {}
            point = total.get("Point", 0)
            if not point:
                point = game_total.get("Point", 0)
            if not point:
                continue
            line = float(point)
            if line <= 0:
                continue

            over_line = total.get("Over", {}).get("Line")
            under_line = total.get("Under", {}).get("Line")

            if over_line is not None:
                props.append(PlayerProp(
                    player_name=player,
                    stat_type=stat_type,
                    line=line,
                    price=int(over_line),
                    description="Over",
                    bookmaker_key="betonlineag",
                    bookmaker_title="BetOnline",
                    event_url=sport_url_base,
                ))
            if under_line is not None:
                props.append(PlayerProp(
                    player_name=player,
                    stat_type=stat_type,
                    line=line,
                    price=int(under_line),
                    description="Under",
                    bookmaker_key="betonlineag",
                    bookmaker_title="BetOnline",
                    event_url=sport_url_base,
                ))

        return props

    async def _scrape_props_dom(self, sport_url_base: str) -> List[PlayerProp]:
        """Scrape player props from the rendered BetOnline prop builder DOM."""
        if self._page is None:
            return []

        try:
            raw_props = await self._page.evaluate("""
                () => {
                    const results = [];

                    // Strategy 1: Look for prop rows in table-like structures
                    const rows = document.querySelectorAll(
                        'tr, [class*="prop"], [class*="player"], [data-testid*="prop"]'
                    );

                    for (const row of rows) {
                        const text = row.textContent || '';
                        const ouMatch = text.match(
                            /(?:O(?:ver)?|U(?:nder)?)\s+(\d+\.?\d*)\s+([+-]\d+)/gi
                        );
                        if (!ouMatch || ouMatch.length < 1) continue;

                        const nameEl = row.querySelector(
                            '[class*="name"], [class*="player"], td:first-child, p:first-child'
                        );
                        const playerName = nameEl ? nameEl.textContent.trim() : '';
                        if (!playerName || playerName.length < 3) continue;

                        for (const match of ouMatch) {
                            const parts = match.match(
                                /(O(?:ver)?|U(?:nder)?)\s+(\d+\.?\d*)\s+([+-]\d+)/i
                            );
                            if (!parts) continue;
                            results.push({
                                player: playerName,
                                side: parts[1].startsWith('O') ? 'Over' : 'Under',
                                line: parseFloat(parts[2]),
                                odds: parseInt(parts[3]),
                                stat: ''
                            });
                        }
                    }

                    // Strategy 2: MUI-style buttons
                    if (results.length === 0) {
                        const cards = document.querySelectorAll(
                            '[class*="card"], [class*="Card"], [class*="prop-row"]'
                        );
                        for (const card of cards) {
                            const buttons = card.querySelectorAll('button');
                            if (buttons.length < 2) continue;

                            let playerName = '';
                            const nameEls = card.querySelectorAll(
                                'p, span, h3, h4, [class*="name"], [class*="player"]'
                            );
                            for (const el of nameEls) {
                                const t = el.textContent.trim();
                                if (t.length >= 3 && /^[A-Za-z.\s'-]+$/.test(t)
                                    && t.split(' ').length >= 2) {
                                    playerName = t;
                                    break;
                                }
                            }
                            if (!playerName) continue;

                            for (const btn of buttons) {
                                const btnParts = Array.from(btn.querySelectorAll('p, span'))
                                    .map(el => el.textContent.trim());
                                if (btnParts.length < 1) continue;

                                for (let i = 0; i < btnParts.length; i++) {
                                    const part = btnParts[i];
                                    const ouParts = part.match(
                                        /^(O|U|Over|Under)\s+(\d+\.?\d*)$/i
                                    );
                                    if (ouParts) {
                                        const side = ouParts[1].startsWith('O')
                                            ? 'Over' : 'Under';
                                        const line = parseFloat(ouParts[2]);
                                        const oddsStr = btnParts[i + 1] || '';
                                        const odds = parseInt(
                                            oddsStr.replace('+', '').replace(',', '')
                                        );
                                        if (!isNaN(odds) && !isNaN(line) && line > 0) {
                                            results.push({
                                                player: playerName,
                                                side: side,
                                                line: line,
                                                odds: odds,
                                                stat: ''
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }

                    return results;
                }
            """)

            if not raw_props or not isinstance(raw_props, list):
                return []

            props = []  # type: List[PlayerProp]
            for item in raw_props:
                if not isinstance(item, dict):
                    continue
                player = (item.get("player") or "").strip()
                if not player or len(player) < 3:
                    continue
                line = item.get("line", 0)
                odds = item.get("odds", 0)
                side = item.get("side", "Over")
                stat_raw = item.get("stat", "")
                if not line or not odds:
                    continue

                stat_type = self._classify_stat(stat_raw) if stat_raw else "other"

                props.append(PlayerProp(
                    player_name=player,
                    stat_type=stat_type,
                    line=float(line),
                    price=int(odds),
                    description=side,
                    bookmaker_key="betonlineag",
                    bookmaker_title="BetOnline",
                    event_url=sport_url_base,
                ))

            logger.info("BetOnline: DOM prop scrape found %d props", len(props))
            return props

        except Exception as e:
            logger.warning("BetOnline: DOM prop scrape error: %s", e)
            return []

    # ------------------------------------------------------------------
    # Stat classifier
    # ------------------------------------------------------------------

    @staticmethod
    def _classify_stat(raw: str) -> str:
        """Map BetOnline's stat label to a canonical stat_type key."""
        raw_lower = raw.lower()

        # Combo stats (check before individual)
        if ("point" in raw_lower or "pts" in raw_lower) and (
            "rebound" in raw_lower or "reb" in raw_lower
        ) and ("assist" in raw_lower or "ast" in raw_lower):
            return "pts_reb_ast"
        if ("point" in raw_lower or "pts" in raw_lower) and (
            "rebound" in raw_lower or "reb" in raw_lower
        ):
            return "pts_reb"
        if ("point" in raw_lower or "pts" in raw_lower) and (
            "assist" in raw_lower or "ast" in raw_lower
        ):
            return "pts_ast"
        if ("rebound" in raw_lower or "reb" in raw_lower) and (
            "assist" in raw_lower or "ast" in raw_lower
        ):
            return "reb_ast"
        if ("steal" in raw_lower) and ("block" in raw_lower):
            return "stl_blk"

        # Individual stats
        if "three" in raw_lower or "3pt" in raw_lower or "3-pt" in raw_lower:
            return "threes"
        if "point" in raw_lower or "pts" in raw_lower:
            return "points"
        if "rebound" in raw_lower or "reb" in raw_lower:
            return "rebounds"
        if "assist" in raw_lower or "ast" in raw_lower:
            return "assists"
        if "steal" in raw_lower:
            return "steals"
        if "block" in raw_lower and "shot" not in raw_lower:
            return "blocks"
        if "strikeout" in raw_lower:
            return "strikeouts"
        if "hit" in raw_lower:
            return "hits"
        if "rbi" in raw_lower:
            return "rbis"
        if "run" in raw_lower:
            return "runs"
        if "shot" in raw_lower and "goal" in raw_lower:
            return "shots_on_goal"
        if "goal" in raw_lower:
            return "goals"
        if "touchdown" in raw_lower or "td" in raw_lower:
            return "touchdowns"
        if "yard" in raw_lower and "pass" in raw_lower:
            return "pass_yards"
        if "yard" in raw_lower and "rush" in raw_lower:
            return "rush_yards"
        if "yard" in raw_lower and "rec" in raw_lower:
            return "rec_yards"
        if "reception" in raw_lower or "rec" in raw_lower:
            return "receptions"
        if "completion" in raw_lower:
            return "pass_completions"
        if "interception" in raw_lower:
            return "interceptions"
        if "sog" in raw_lower:
            return "shots_on_goal"
        if "save" in raw_lower:
            return "saves"
        return "other"

    # ------------------------------------------------------------------
    # Browser lifecycle
    # ------------------------------------------------------------------

    async def _close_browser(self) -> None:
        """Close and reset the browser for recovery."""
        logger.info("BetOnline: Closing browser for restart")
        try:
            if self._page:
                await self._page.close()
            if self._context:
                await self._context.close()
            if self._browser:
                await self._browser.close()
            if self._pw:
                await self._pw.stop()
        except Exception as e:
            logger.debug("BetOnline: Error closing browser: %s", e)
        finally:
            self._page = None
            self._context = None
            self._browser = None
            self._pw = None

    async def close(self) -> None:
        """Shut down the Playwright browser."""
        await self._close_browser()
