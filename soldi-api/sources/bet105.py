"""
Bet105 sportsbook scraper.

Scrapes odds from Bet105's GeniusSports-powered prematch platform at
ppm.bet105.ag. This subdomain is publicly accessible without login —
it renders an AngularJS SPA that receives live odds via WebSocket from
pandora.ganchrow.com. We load the page in headless Playwright, wait for
the WebSocket data to render into the DOM, and extract events via DOM
scraping.

No login or credentials required!

GS Betting sport IDs (ppm.bet105.ag):
  1 = Baseball, 2 = Basketball (NBA + NCAAB), 3 = Football,
  4 = Hockey, 5 = Soccer, 7 = Golf, 8 = Tennis, 13 = Boxing,
  27 = MMA, 214 = World Cup, 88888477 = Futures

DOM structure per event (.event-list__item):
  - Top team (data-testid="top-team-details")  = away
  - Bottom team (data-testid="bottom-team-details") = home
  - Markets (.offerings):
      market-6 = Spread   (desc="+6", price="-105")
      market-5 = Total    (desc="o218.5", price="-108")
      market-3 = Moneyline (desc="", price="+202")
  - League sections: each .event-list corresponds to a league
    within a sport (e.g., NBA, NCAAB, Exhibition, EPL, etc.)
"""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

PPM_URL = "https://ppm.bet105.ag/live/"

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

# ── League name (from DOM / Angular scope) -> sport_key ──────────────────────
# All keys are lowercase for case-insensitive matching.
_LEAGUE_TO_KEY = {
    # Soccer
    # Soccer (display names + Angular iconName format)
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
    # Baseball — spring training / exhibition still counts as MLB
    "mlb": "baseball_mlb",
    "exhibition": "baseball_mlb",
    "spring training": "baseball_mlb",
    "preseason": "baseball_mlb",
    # MMA — includes specific UFC event names that rotate
    "ufc": "mma_mixed_martial_arts",
    "mma": "mma_mixed_martial_arts",
    "pfl": "mma_mixed_martial_arts",
    "ufc 325": "mma_mixed_martial_arts",
    "ufc 326": "mma_mixed_martial_arts",
    "ufc 327": "mma_mixed_martial_arts",
    "ufc 328": "mma_mixed_martial_arts",
    "ufc fight night": "mma_mixed_martial_arts",
    "ufc fn": "mma_mixed_martial_arts",
    "ufc early prelims": "mma_mixed_martial_arts",
    "ufc prelims": "mma_mixed_martial_arts",
    "bellator": "mma_mixed_martial_arts",
    "bellator mma": "mma_mixed_martial_arts",
    "one championship": "mma_mixed_martial_arts",
    "one fc": "mma_mixed_martial_arts",
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

# ── Known MLB teams (for baseball filtering) ────────────────────────────────
_MLB_TEAMS = frozenset([
    "Arizona Diamondbacks", "Atlanta Braves", "Baltimore Orioles",
    "Boston Red Sox", "Chicago Cubs", "Chicago White Sox",
    "Cincinnati Reds", "Cleveland Guardians", "Colorado Rockies",
    "Detroit Tigers", "Houston Astros", "Kansas City Royals",
    "Los Angeles Angels", "Los Angeles Dodgers", "Miami Marlins",
    "Milwaukee Brewers", "Minnesota Twins", "New York Mets",
    "New York Yankees", "Oakland Athletics", "Philadelphia Phillies",
    "Pittsburgh Pirates", "San Diego Padres", "San Francisco Giants",
    "Seattle Mariners", "St. Louis Cardinals", "Tampa Bay Rays",
    "Texas Rangers", "Toronto Blue Jays", "Washington Nationals",
])

# ── Keywords that indicate a futures market (skip these) ─────────────────────
_FUTURES_KEYWORDS = frozenset([
    "wins", "champion", "coin toss", "futures", "conference",
    "division", "mvp", "award", "super bowl", "world series",
    "stanley cup", "nba finals", "pennant", "cy young", "heisman",
])

# ── Soccer navigation ────────────────────────────────────────────────────────
# Soccer on ppm.bet105.ag requires two clicks:
#   1. Country sub-menu (England, Spain, etc.)
#   2. League panel (EPL, La Liga, etc.) — panels are collapsed by default
# Maps sport_key -> (country CSS suffix, league panel ID)
_SOCCER_NAV = {
    "soccer_epl": ("68_5", "122_68"),                    # England → EPL
    "soccer_spain_la_liga": ("25_5", "2332_25"),          # Spain → La Liga
    "soccer_italy_serie_a": ("16_5", "178_16"),           # Italy → Serie A
    "soccer_germany_bundesliga": ("29_5", "30_29"),       # Germany → Bundesliga
    "soccer_france_ligue_one": ("7_5", "130_7"),          # France → Ligue 1
    "soccer_uefa_champs_league": ("398_5", "39_398"),      # UCL country → UCL
}  # type: Dict[str, Tuple[str, str]]

# Keep backward compat alias
_SOCCER_COUNTRIES = {k: v[0] for k, v in _SOCCER_NAV.items()}

# Cache TTL in seconds — kept short for live odds freshness.
# The scraper navigates to the /live/ page, so results include
# in-play events that update rapidly via WebSocket.
_CACHE_TTL = 45  # seconds — prefetch loop keeps cache warm every ~20s

# ── JavaScript: extract all events from the rendered DOM ─────────────────────
# Iterates over .event-list sections (one per league), finds the league name
# via Angular scope or nearby DOM headers, then extracts events within.
_JS_EXTRACT = r"""
() => {
    const results = [];
    const activeSport = document.querySelector(".sports-menu__item.active a");
    const activeSportId = activeSport ? activeSport.getAttribute("data-id") : "";

    // Find all event-list sections (one per league)
    const eventLists = document.querySelectorAll(".event-list");

    eventLists.forEach(list => {
        let leagueName = "";

        // Strategy 1: Angular scope — most reliable for league data
        try {
            const scope = angular.element(list).scope();
            if (scope) {
                const lg = scope.league || (scope.$parent && scope.$parent.league);
                if (lg) {
                    leagueName = lg.iconName || lg.name || lg.id || "";
                }
            }
        } catch(e) {}

        // Strategy 2: Look for a header element before the event-list
        if (!leagueName) {
            let el = list.previousElementSibling;
            for (let i = 0; i < 5 && el; i++) {
                const text = el.textContent.trim();
                if (text && text.length > 1 && text.length < 80 &&
                    !text.match(/^(Today|Tomorrow)/i) &&
                    !text.match(/^\d{1,2}:\d{2}/) &&
                    !text.match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d/i)) {
                    leagueName = text;
                    break;
                }
                el = el.previousElementSibling;
            }
        }

        // Strategy 3: Check parent for a header child
        if (!leagueName) {
            const parent = list.parentElement;
            if (parent) {
                for (let ci = 0; ci < parent.children.length; ci++) {
                    const child = parent.children[ci];
                    if (child === list) break;
                    const h = child.querySelector("h1,h2,h3,h4,h5,h6,.league-name,.league-title,.title");
                    if (h) {
                        const t = h.textContent.trim();
                        if (t && t.length < 80) leagueName = t;
                    }
                }
            }
        }

        // Extract events from this section
        const items = list.querySelectorAll(".event-list__item");
        items.forEach(item => {
            const ev = { sportId: activeSportId, league: leagueName };

            // Team names
            const top = item.querySelector('[data-testid="top-team-details"]');
            const bot = item.querySelector('[data-testid="bottom-team-details"]');
            ev.awayTeam = top ? top.textContent.trim() : "";
            ev.homeTeam = bot ? bot.textContent.trim() : "";
            if (!ev.awayTeam || !ev.homeTeam) return;

            // Event ID from link
            const link = item.querySelector("a.event-list__item__details");
            ev.eventId = link ? link.getAttribute("href").replace("#!event/", "") : "";

            // Date/time
            ev.dateTime = "";
            const dtContainer = item.querySelector(".event-list__item__details__date, [class*='date']");
            if (dtContainer) {
                const parts = [];
                dtContainer.querySelectorAll("time, span, p").forEach(el => {
                    const t = el.textContent.trim();
                    if (t) parts.push(t);
                });
                ev.dateTime = parts.join(" ");
            }
            if (!ev.dateTime) {
                const allText = item.textContent;
                const timeMatch = allText.match(/(?:^|\D)(\d{1,2}:\d{2}\s*[AP]M)/i);
                const dateMatch = allText.match(/((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})/i);
                const todayMatch = allText.match(/Today/i);
                if (timeMatch) {
                    ev.dateTime = timeMatch[1];
                    if (dateMatch) ev.dateTime += " " + dateMatch[1];
                    else if (todayMatch) ev.dateTime += " Today";
                }
            }

            // Markets
            ev.markets = [];
            const offerings = item.querySelectorAll(".offerings");
            offerings.forEach(o => {
                const classes = Array.from(o.classList);
                let mType = "unknown";
                for (const c of classes) {
                    if (c.match(/^market-\d+$/)) { mType = c; break; }
                }
                const lines = [];
                o.querySelectorAll(".odd").forEach(odd => {
                    const desc = odd.querySelector(".odds-description");
                    const price = odd.querySelector(".emphasis");
                    lines.push({
                        d: desc ? desc.textContent.trim() : "",
                        p: price ? price.textContent.trim() : ""
                    });
                });
                if (lines.length > 0) {
                    ev.markets.push({ t: mType, l: lines });
                }
            });

            results.push(ev);
        });
    });

    return results;
}
"""


class Bet105Source(DataSource):
    """Fetches odds from Bet105 via DOM scraping of ppm.bet105.ag.

    No login required.  Launches a headless Playwright browser, navigates
    to each sport page, waits for the AngularJS SPA to render via
    WebSocket, and extracts events from the DOM.

    For sports that share a GS sport ID (e.g., soccer leagues, tennis
    ATP/WTA), a single page fetch caches results for ALL sibling sport_keys.
    """

    def __init__(self, email: str = "", password: str = ""):
        # email/password kept for backward-compat constructor but unused
        self._browser = None  # type: Any
        self._context = None  # type: Any
        self._page = None  # type: Any
        self._pw = None  # type: Any
        self._lock = asyncio.Lock()
        self._cache = {}  # type: Dict[str, Tuple[List[OddsEvent], float]]
        self._prefetch_task = None  # type: Any
        self._current_sport_id = None  # type: Optional[int]

    # ------------------------------------------------------------------
    # Prefetch
    # ------------------------------------------------------------------

    def start_prefetch(self) -> None:
        """Start background prefetch of all supported sports."""
        self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    async def _prefetch_all(self) -> None:
        await asyncio.sleep(8)  # Let other sources initialize first
        logger.info("Bet105: Starting continuous background prefetch")
        cycle = 0
        while True:
            cycle += 1
            async with self._lock:
                try:
                    await self._ensure_browser()
                    if self._page is None:
                        await asyncio.sleep(30)
                        continue
                    # Fetch once per GS sport ID (except soccer which needs per-league)
                    fetched_ids = set()  # type: set
                    for sport_key in _SPORT_MAP:
                        sport_id = _SPORT_MAP[sport_key]
                        # Soccer needs separate fetch per league (country nav)
                        if sport_id == 5:
                            try:
                                await self._fetch_soccer(sport_key)
                            except Exception as e:
                                logger.warning(
                                    "Bet105 prefetch failed for %s: %s",
                                    sport_key, e,
                                )
                            continue
                        if sport_id in fetched_ids:
                            continue
                        fetched_ids.add(sport_id)
                        try:
                            await self._fetch_sport(sport_key)
                            total = sum(
                                len(self._cache.get(k, ([], 0))[0])
                                for k, sid in _SPORT_MAP.items()
                                if sid == sport_id
                            )
                            logger.info(
                                "Bet105 prefetch: %d events for sport ID %d",
                                total, sport_id,
                            )
                        except Exception as e:
                            logger.warning(
                                "Bet105 prefetch failed for sport ID %d: %s",
                                sport_id, e,
                            )
                except Exception as e:
                    logger.warning("Bet105 prefetch error: %s", e)
            logger.info("Bet105: Prefetch cycle #%d complete", cycle)
            await asyncio.sleep(20)  # Keep cache warm more frequently

    # ------------------------------------------------------------------
    # Browser lifecycle
    # ------------------------------------------------------------------

    async def _ensure_browser(self) -> None:
        if self._page is not None:
            return
        try:
            from playwright.async_api import async_playwright
            self._pw = await async_playwright().start()
            self._browser = await self._pw.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
            self._context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
            )
            self._page = await self._context.new_page()
            # Navigate to the PPM site and let the SPA initialise
            await self._page.goto(
                PPM_URL, timeout=60000, wait_until="domcontentloaded",
            )
            await asyncio.sleep(8)  # AngularJS boot + WebSocket connect
            self._current_sport_id = None
            logger.info("Bet105: Playwright browser launched, PPM SPA loaded")
        except Exception as e:
            logger.warning("Bet105: Failed to launch browser: %s", e)
            await self._close_browser()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

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

        # Always serve from cache — prefetch loop keeps it warm.
        # Never fall through to Playwright navigation in the composite context.
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _CACHE_TTL:
            return cached[0], headers
        # Serve stale data if prefetch hasn't refreshed yet
        if cached:
            return cached[0], headers
        return [], headers

    # ------------------------------------------------------------------
    # Fetching
    # ------------------------------------------------------------------

    async def _fetch_sport(self, sport_key: str) -> None:
        """Navigate to a sport page and scrape all events.

        Classifies events by league and caches results for ALL sport_keys
        that share the same GS sport ID.
        For soccer (sport_id=5), delegates to _fetch_soccer() instead.
        """
        sport_id = _SPORT_MAP[sport_key]
        if sport_id == 5:
            await self._fetch_soccer(sport_key)
            return
        sibling_keys = _SPORT_ID_TO_KEYS.get(sport_id, [sport_key])

        # Click the sport tab if not already on it
        if self._current_sport_id != sport_id:
            try:
                selector = 'a[data-id="{}"]'.format(sport_id)
                await self._page.click(selector, timeout=5000)
            except Exception:
                # Sport tab doesn't exist (e.g. NFL offseason)
                logger.warning(
                    "Bet105: Sport tab %d not found, skipping", sport_id,
                )
                now = time.time()
                for k in sibling_keys:
                    self._cache[k] = ([], now)
                return
            self._current_sport_id = sport_id
            # Wait for WebSocket odds data to populate in the DOM.
            # The SPA renders event containers quickly but odds arrive
            # later via WebSocket from pandora.ganchrow.com.
            # Scroll to bottom to trigger lazy-loaded sections, then scroll back.
            await asyncio.sleep(3)
            await self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
            await self._page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(3)
        else:
            # Already on the sport, just wait a short time for any updates
            await asyncio.sleep(1)

        # Verify the correct sport is active (prevents stale data)
        try:
            active_id = await self._page.evaluate("""
                () => {
                    const a = document.querySelector(".sports-menu__item.active a");
                    return a ? a.getAttribute("data-id") : "";
                }
            """)
            if str(active_id) != str(sport_id):
                logger.debug(
                    "Bet105: Expected sport %d but active is %s, skipping",
                    sport_id, active_id,
                )
                self._current_sport_id = None
                now = time.time()
                for k in sibling_keys:
                    self._cache[k] = ([], now)
                return
        except Exception:
            pass

        # Extract events via DOM
        try:
            raw_events = await self._page.evaluate(_JS_EXTRACT)
        except Exception as e:
            logger.warning("Bet105: DOM extraction failed: %s", e)
            now = time.time()
            for k in sibling_keys:
                self._cache[k] = ([], now)
            return

        if not raw_events:
            logger.warning("Bet105: No events in DOM for sport %d", sport_id)
            now = time.time()
            for k in sibling_keys:
                self._cache[k] = ([], now)
            return

        logger.info("Bet105: %d raw events extracted for sport %d", len(raw_events), sport_id)

        # Debug: log sample event details for MMA/Boxing to help diagnose missing events
        if sport_id in (27, 13):
            for ev in raw_events[:3]:
                logger.info(
                    "Bet105 MMA/Boxing sample: league='%s' away='%s' home='%s' "
                    "markets=%s",
                    ev.get("league", ""),
                    ev.get("awayTeam", ""),
                    ev.get("homeTeam", ""),
                    [m.get("t") for m in ev.get("markets", [])],
                )
        # Classify events by sport_key and cache all siblings
        classified = self._classify_and_parse(raw_events, sport_id)
        now = time.time()
        for key, events in classified.items():
            self._cache[key] = (events, now)
            if events:
                logger.info("Bet105: %d events for %s", len(events), key)

        # Cache empty results for sibling keys with no events
        for k in sibling_keys:
            if k not in classified:
                self._cache[k] = ([], now)

    async def _fetch_soccer(self, sport_key: str) -> None:
        """Fetch soccer events via Country -> League panel navigation.

        Soccer on ppm.bet105.ag uses collapsed league panels:
          1. Click Soccer sport tab
          2. Click Country sub-menu (England, Spain, etc.)
          3. Click league panel heading to expand it (EPL, La Liga, etc.)
          4. Wait for events to load inside the expanded panel
          5. Extract events from DOM
        """
        nav = _SOCCER_NAV.get(sport_key)
        if not nav:
            self._cache[sport_key] = ([], time.time())
            return

        country_id, league_id = nav
        logger.info(
            "Bet105: Fetching soccer %s (country %s, league %s)",
            sport_key, country_id, league_id,
        )

        # Step 1: Click the Soccer sport tab if not already on it
        if self._current_sport_id != 5:
            try:
                await self._page.click('a[data-id="5"]', timeout=5000)
            except Exception:
                logger.warning("Bet105: Soccer sport tab not found")
                self._cache[sport_key] = ([], time.time())
                return
            self._current_sport_id = 5
            await asyncio.sleep(3)

        # Step 2: Click the country sub-menu
        country_selector = "a.item-country-{}".format(country_id)
        try:
            await self._page.click(country_selector, timeout=5000)
        except Exception as exc:
            logger.warning(
                "Bet105: Soccer country %s not found for %s: %s",
                country_id, sport_key, exc,
            )
            self._cache[sport_key] = ([], time.time())
            return
        await asyncio.sleep(2)

        # Step 3: Ensure the league panel is expanded.
        # Panel headings toggle open/close, so we use JS to force-expand
        # by setting the panel body height to 'auto' and triggering the
        # Angular scope if needed.  If the panel doesn't exist at all,
        # we fall back to clicking the heading once.
        panel_exists = await self._page.evaluate(
            '(lid) => !!document.querySelector(\'div.panel[league-id="\' + lid + \'"]\');',
            league_id,
        )
        if not panel_exists:
            logger.warning(
                "Bet105: Soccer league panel %s not found for %s",
                league_id, sport_key,
            )
            self._cache[sport_key] = ([], time.time())
            return

        # Force-expand panel via JS (avoids toggle issues)
        await self._page.evaluate(
            """(lid) => {
                const panel = document.querySelector('div.panel[league-id="' + lid + '"]');
                if (!panel) return;
                // Expand the panel body by removing height restriction
                const body = panel.querySelector('.panel-body, .panel-collapse');
                if (body) {
                    body.style.height = 'auto';
                    body.style.overflow = 'visible';
                    body.style.display = 'block';
                }
                // Also try Angular: toggle the league's expanded state
                try {
                    const scope = angular.element(panel).scope();
                    if (scope && scope.league && !scope.league.isExpanded) {
                        scope.league.isExpanded = true;
                        scope.$apply();
                    }
                } catch(e) {}
                // Also click the heading if body has no event items yet
                const items = panel.querySelectorAll('.event-list__item');
                if (items.length === 0) {
                    const heading = panel.querySelector('.panel-heading');
                    if (heading) heading.click();
                }
            }""",
            league_id,
        )

        await asyncio.sleep(5)  # Wait for events to load inside the panel

        # Step 4: Extract events from the expanded panel
        # Build a JS function that extracts events from the specific league panel
        js_panel_extract = (
            '(leagueId) => {\n'
            '  const results = [];\n'
            '  const panel = document.querySelector(\'div.panel[league-id="\' + leagueId + \'"]\');\n'
            '  if (!panel) return results;\n'
            '  const items = panel.querySelectorAll(".event-list__item");\n'
            '  items.forEach(item => {\n'
            '    const ev = { sportId: "5", league: "" };\n'
            '    const top = item.querySelector(\'[data-testid="top-team-details"]\');\n'
            '    const bot = item.querySelector(\'[data-testid="bottom-team-details"]\');\n'
            '    ev.awayTeam = top ? top.textContent.trim() : "";\n'
            '    ev.homeTeam = bot ? bot.textContent.trim() : "";\n'
            '    if (!ev.awayTeam || !ev.homeTeam) return;\n'
            '    const title = panel.querySelector(".panel-title");\n'
            '    ev.league = title ? title.textContent.trim() : "";\n'
            '    const link = item.querySelector("a.event-list__item__details");\n'
            '    ev.eventId = link ? link.getAttribute("href").replace("#!event/", "") : "";\n'
            '    ev.dateTime = "";\n'
            '    const dtContainer = item.querySelector(".event-list__item__details__date, [class*=\'date\']");\n'
            '    if (dtContainer) {\n'
            '      const parts = [];\n'
            '      dtContainer.querySelectorAll("time, span, p").forEach(el => {\n'
            '        const t = el.textContent.trim();\n'
            '        if (t) parts.push(t);\n'
            '      });\n'
            '      ev.dateTime = parts.join(" ");\n'
            '    }\n'
            '    if (!ev.dateTime) {\n'
            '      const allText = item.textContent;\n'
            '      const timeMatch = allText.match(/(?:^|\\D)(\\d{1,2}:\\d{2}\\s*[AP]M)/i);\n'
            '      const dateMatch = allText.match(/((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+\\d{1,2})/i);\n'
            '      if (timeMatch) { ev.dateTime = timeMatch[1]; if (dateMatch) ev.dateTime += " " + dateMatch[1]; }\n'
            '    }\n'
            '    ev.markets = [];\n'
            '    const offerings = item.querySelectorAll(".offerings");\n'
            '    offerings.forEach(o => {\n'
            '      const classes = Array.from(o.classList);\n'
            '      let mType = "unknown";\n'
            '      for (const c of classes) { if (c.match(/^market-\\d+$/)) { mType = c; break; } }\n'
            '      const lines = [];\n'
            '      o.querySelectorAll(".odd").forEach(odd => {\n'
            '        const desc = odd.querySelector(".odds-description");\n'
            '        const price = odd.querySelector(".emphasis");\n'
            '        lines.push({ d: desc ? desc.textContent.trim() : "", p: price ? price.textContent.trim() : "" });\n'
            '      });\n'
            '      if (lines.length > 0) { ev.markets.push({ t: mType, l: lines }); }\n'
            '    });\n'
            '    results.push(ev);\n'
            '  });\n'
            '  return results;\n'
            '}'
        )

        try:
            raw_events = await self._page.evaluate(
                js_panel_extract, league_id,
            )
        except Exception as e:
            logger.warning("Bet105: Soccer panel extraction failed: %s", e)
            self._cache[sport_key] = ([], time.time())
            return

        if not raw_events:
            logger.info("Bet105: No events in soccer panel %s for %s", league_id, sport_key)
            self._cache[sport_key] = ([], time.time())
            return

        logger.info(
            "Bet105: %d raw soccer events for %s (panel %s)",
            len(raw_events), sport_key, league_id,
        )

        # Parse events directly (no classification needed - we know the sport_key)
        events = self._parse_soccer_events(raw_events, sport_key)
        now = time.time()
        self._cache[sport_key] = (events, now)
        if events:
            logger.info("Bet105: %d events for %s", len(events), sport_key)

    def _parse_soccer_events(
        self,
        raw_events: List[dict],
        sport_key: str,
    ) -> List[OddsEvent]:
        """Parse soccer events directly (sport_key already known)."""
        sport_title = get_sport_title(sport_key)
        events = []  # type: List[OddsEvent]

        for raw in raw_events:
            away_raw = raw.get("awayTeam", "")
            home_raw = raw.get("homeTeam", "")
            if not away_raw or not home_raw:
                continue

            away_team = resolve_team_name(away_raw)
            home_team = resolve_team_name(home_raw)

            # Skip futures
            combined = (away_team + " " + home_team).lower()
            if any(kw in combined for kw in _FUTURES_KEYWORDS):
                continue

            # Parse markets
            bet_markets = []  # type: List[Market]
            for mkt in raw.get("markets", []):
                market = self._parse_dom_market(mkt, away_team, home_team)
                if market:
                    bet_markets.append(market)

            if not bet_markets:
                continue

            commence_time = _parse_dom_datetime(raw.get("dateTime", ""))
            cid = canonical_event_id(
                sport_key, home_team, away_team, commence_time,
            )

            event_id = raw.get("eventId", "")
            event_url = (
                "https://ppm.bet105.ag/live/?#!/event/{}".format(event_id)
                if event_id else None
            )

            events.append(OddsEvent(
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
                        markets=bet_markets,
                        event_url=event_url,
                    )
                ],
            ))

        return events

    # ------------------------------------------------------------------
    # Classification & Parsing
    # ------------------------------------------------------------------

    def _classify_and_parse(
        self,
        raw_events: List[dict],
        sport_id: int,
    ) -> Dict[str, List[OddsEvent]]:
        """Classify raw DOM events into sport_keys and parse into OddsEvent.

        Returns a dict of {sport_key: [OddsEvent, ...]}.
        """
        result = {}  # type: Dict[str, List[OddsEvent]]
        logged_leagues = set()  # type: set


        for raw in raw_events:
            away_raw = raw.get("awayTeam", "")
            home_raw = raw.get("homeTeam", "")
            if not away_raw or not home_raw:
                continue

            # Strip gender tags like "(W)" or "(M)" from names (common in MMA)
            away_clean = re.sub(r"\s*\([WMwm]\)\s*$", "", away_raw).strip()
            home_clean = re.sub(r"\s*\([WMwm]\)\s*$", "", home_raw).strip()
            away_team = resolve_team_name(away_clean)
            home_team = resolve_team_name(home_clean)

            # Skip futures
            combined = (away_team + " " + home_team).lower()
            if any(kw in combined for kw in _FUTURES_KEYWORDS):
                continue

            # Determine which sport_key this event belongs to
            league_raw = raw.get("league", "")
            sport_key = self._classify_event(
                sport_id, league_raw, away_team, home_team,
            )
            if sport_key is None:
                # Log unmatched leagues once for debugging
                if league_raw and league_raw not in logged_leagues:
                    logged_leagues.add(league_raw)
                    logger.warning(
                        "Bet105: Unmatched league '%s' for sport ID %d (teams: %s vs %s)",
                        league_raw, sport_id, away_team, home_team,
                    )
                elif not league_raw and sport_id == 5 and league_raw not in logged_leagues:
                    logged_leagues.add("__empty_soccer__")
                    logger.warning(
                        "Bet105: Empty league name for sport ID 5 (soccer), sample: %s vs %s",
                        away_team, home_team,
                    )
                continue

            # Parse markets
            bet_markets = []  # type: List[Market]
            for mkt in raw.get("markets", []):
                market = self._parse_dom_market(mkt, away_team, home_team)
                if market:
                    bet_markets.append(market)

            if not bet_markets:
                pass
                continue

            # Parse date/time
            commence_time = _parse_dom_datetime(raw.get("dateTime", ""))

            cid = canonical_event_id(
                sport_key, home_team, away_team, commence_time,
            )

            event_id = raw.get("eventId", "")
            event_url = (
                "https://ppm.bet105.ag/live/?#!/event/{}".format(event_id)
                if event_id else None
            )

            sport_title = get_sport_title(sport_key)
            event = OddsEvent(
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
                        markets=bet_markets,
                        event_url=event_url,
                    )
                ],
            )
            result.setdefault(sport_key, []).append(event)

        return result

    def _classify_event(
        self,
        sport_id: int,
        league_raw: str,
        away_team: str,
        home_team: str,
    ) -> Optional[str]:
        """Determine which sport_key an event belongs to.

        Returns None if the event should be skipped (no matching sport_key).
        """
        sibling_keys = _SPORT_ID_TO_KEYS.get(sport_id, [])
        if not sibling_keys:
            return None

        # ── Single-key sports (no league differentiation needed) ─────
        if len(sibling_keys) == 1:
            key = sibling_keys[0]
            # For baseball, filter by MLB teams to exclude non-MLB games
            if key == "baseball_mlb":
                if away_team not in _MLB_TEAMS or home_team not in _MLB_TEAMS:
                    return None
            return key

        # ── Multi-key sports: classify by league name & team names ───

        # 1. Try league name matching (exact)
        if league_raw:
            league_lower = league_raw.lower().strip()
            matched = _LEAGUE_TO_KEY.get(league_lower)
            if matched and matched in sibling_keys:
                # Additional team filtering for baseball exhibition
                if matched == "baseball_mlb":
                    if away_team not in _MLB_TEAMS or home_team not in _MLB_TEAMS:
                        return None
                return matched

            # 2. Try partial/contains matching
            for pattern, key in _LEAGUE_TO_KEY.items():
                if key not in sibling_keys:
                    continue
                if pattern in league_lower or league_lower in pattern:
                    if key == "baseball_mlb":
                        if away_team not in _MLB_TEAMS or home_team not in _MLB_TEAMS:
                            return None
                    return key

        # 3. Team-name-based classification (fallback)
        if sport_id == 2:  # Basketball
            both_nba = away_team in _NBA_TEAMS and home_team in _NBA_TEAMS
            return "basketball_nba" if both_nba else "basketball_ncaab"

        if sport_id == 8:  # Tennis — default to ATP if no WTA league tag
            return "tennis_atp"

        if sport_id == 1:  # Baseball — MLB team check
            if away_team in _MLB_TEAMS and home_team in _MLB_TEAMS:
                return "baseball_mlb"
            return None

        # 4. MMA/Boxing sport IDs — all events belong to the same sport_key
        if sport_id == 27:  # MMA
            return "mma_mixed_martial_arts"
        if sport_id == 13:  # Boxing
            return "boxing_boxing"

        # 5. For soccer without a matched league, skip
        # (we only want to include known major leagues)
        if sport_id == 5:
            return None

        # Fallback: use first sibling key
        return sibling_keys[0] if sibling_keys else None

    # ------------------------------------------------------------------
    # Market parsing
    # ------------------------------------------------------------------

    def _parse_dom_market(
        self,
        mkt: dict,
        away_team: str,
        home_team: str,
    ) -> Optional[Market]:
        """Parse a single market from DOM extraction data.

        mkt = {"t": "market-6", "l": [{"d": "+6", "p": "-105"}, ...]}
        """
        mtype = mkt.get("t", "")
        lines = mkt.get("l", [])
        if len(lines) < 2:
            return None

        # market-6 = Spread
        if mtype == "market-6":
            return self._parse_spread_market(lines, away_team, home_team)
        # market-5 = Total
        elif mtype == "market-5":
            return self._parse_total_market(lines)
        # market-3 = Moneyline
        elif mtype == "market-3":
            return self._parse_ml_market(lines, away_team, home_team)
        else:
            # Try to infer: if desc is empty, it's ML; if desc has o/u, total
            d0 = lines[0].get("d", "")
            if not d0:
                return self._parse_ml_market(lines, away_team, home_team)
            elif d0.startswith("o") or d0.startswith("u"):
                return self._parse_total_market(lines)
            else:
                return self._parse_spread_market(lines, away_team, home_team)

    def _parse_ml_market(
        self,
        lines: List[dict],
        away_team: str,
        home_team: str,
    ) -> Optional[Market]:
        """Parse moneyline from DOM lines. Top=away, bottom=home."""
        away_price = _parse_odds(lines[0].get("p", ""))
        home_price = _parse_odds(lines[1].get("p", ""))
        if away_price is None or home_price is None:
            return None
        if away_price == 0 and home_price == 0:
            return None
        return Market(
            key="h2h",
            outcomes=[
                Outcome(name=home_team, price=home_price),
                Outcome(name=away_team, price=away_price),
            ],
        )

    def _parse_spread_market(
        self,
        lines: List[dict],
        away_team: str,
        home_team: str,
    ) -> Optional[Market]:
        """Parse spread from DOM. desc="+6", price="-105"."""
        away_point = _parse_point(lines[0].get("d", ""))
        away_price = _parse_odds(lines[0].get("p", ""))
        home_point = _parse_point(lines[1].get("d", ""))
        home_price = _parse_odds(lines[1].get("p", ""))
        if away_point is None or away_price is None:
            return None
        if home_point is None or home_price is None:
            return None
        return Market(
            key="spreads",
            outcomes=[
                Outcome(name=home_team, price=home_price, point=home_point),
                Outcome(name=away_team, price=away_price, point=away_point),
            ],
        )

    def _parse_total_market(
        self,
        lines: List[dict],
    ) -> Optional[Market]:
        """Parse total (over/under) from DOM. desc="o218.5", price="-108"."""
        over_desc = lines[0].get("d", "")
        under_desc = lines[1].get("d", "")
        over_price = _parse_odds(lines[0].get("p", ""))
        under_price = _parse_odds(lines[1].get("p", ""))
        if over_price is None or under_price is None:
            return None

        # Extract point from "o218.5" or "u218.5"
        over_point = _parse_total_point(over_desc)
        under_point = _parse_total_point(under_desc)
        point = over_point or under_point
        if point is None:
            return None

        return Market(
            key="totals",
            outcomes=[
                Outcome(name="Over", price=over_price, point=point),
                Outcome(name="Under", price=under_price, point=point),
            ],
        )

    # ------------------------------------------------------------------
    # Browser cleanup
    # ------------------------------------------------------------------

    async def _close_browser(self) -> None:
        for attr in ("_page", "_context", "_browser"):
            obj = getattr(self, attr, None)
            if obj is not None:
                try:
                    await obj.close()
                except Exception:
                    pass
                setattr(self, attr, None)
        if self._pw is not None:
            try:
                await self._pw.stop()
            except Exception:
                pass
            self._pw = None
        self._current_sport_id = None

    async def close(self) -> None:
        try:
            await self._close_browser()
        except Exception as e:
            logger.warning("Bet105: Error closing browser: %s", e)


# =====================================================================
# Module-level helpers
# =====================================================================

def _parse_odds(raw: str) -> Optional[int]:
    """Parse an American odds string like '+202', '-105', '-1068'."""
    if not raw:
        return None
    raw = raw.strip().replace(",", "")
    try:
        return int(raw)
    except ValueError:
        # Try float conversion
        try:
            return int(float(raw))
        except (ValueError, TypeError):
            return None


def _parse_point(raw: str) -> Optional[float]:
    """Parse a spread point like '+6', '-3.5', '+15'."""
    if not raw:
        return None
    raw = raw.strip()
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_total_point(raw: str) -> Optional[float]:
    """Parse a total point from 'o218.5' or 'u218.5'."""
    if not raw:
        return None
    raw = raw.strip().lower()
    if raw.startswith("o") or raw.startswith("u"):
        raw = raw[1:]
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_dom_datetime(raw: str) -> str:
    """Parse a date/time string from the Bet105 DOM.

    Examples:
      "7:10 PM Feb 25"  -> "2026-02-25T00:10:00Z" (ET -> UTC)
      "11:10 PM Today"  -> today's date with that time
      "8:10 PMFeb 25"   -> same, with no space before month
      ""                -> ""

    The PPM site displays times in US Eastern.
    """
    if not raw or not raw.strip():
        return ""
    raw = raw.strip()

    # Extract time part: "7:10 PM" or "11:10 PM"
    time_match = re.search(r"(\d{1,2}):(\d{2})\s*(AM|PM)", raw, re.IGNORECASE)
    if not time_match:
        return ""
    hour = int(time_match.group(1))
    minute = int(time_match.group(2))
    ampm = time_match.group(3).upper()
    if ampm == "PM" and hour != 12:
        hour += 12
    elif ampm == "AM" and hour == 12:
        hour = 0

    # Extract date part: "Feb 25", "Mar 1", or "Today"
    now = datetime.now(timezone.utc)
    date_match = re.search(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})",
        raw, re.IGNORECASE,
    )
    if date_match:
        month_str = date_match.group(1).capitalize()
        day = int(date_match.group(2))
        months = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
            "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
        }
        month = months.get(month_str, now.month)
        year = now.year
        # If the month is in the past and more than 6 months ago, assume next year
        if month < now.month - 6:
            year += 1
    elif "today" in raw.lower():
        year = now.year
        month = now.month
        day = now.day
    else:
        # Can't determine date, use today
        year = now.year
        month = now.month
        day = now.day

    try:
        # Build datetime in ET (UTC-5), then convert to UTC
        dt_et = datetime(year, month, day, hour, minute, 0)
        # ET is UTC-5 (EST) or UTC-4 (EDT). Use UTC-5 as approximation.
        dt_utc = dt_et.replace(hour=(dt_et.hour + 5) % 24)
        if dt_et.hour + 5 >= 24:
            from datetime import timedelta
            dt_utc = dt_utc + timedelta(days=1)
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, OverflowError):
        return ""
