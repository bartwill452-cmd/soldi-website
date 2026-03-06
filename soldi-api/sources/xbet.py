"""
XBet sportsbook scraper.

Uses Playwright headless browser to load xbet.ag/sportsbook/ and parse
odds directly from the server-rendered HTML DOM.

XBet renders ALL sports on a single page as HTML (no JSON API). Each game
is wrapped in a `div.game-line` with Schema.org `Event` markup containing
team names, start times, and odds (spread, moneyline, total).

Website: https://www.xbet.ag/sportsbook/
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

SITE_URL = "https://www.xbet.ag"

# Map XBet league titles (from .league-title text or nav links) → our sport_key
_LEAGUE_TO_SPORT: Dict[str, str] = {
    # Major leagues
    "NBA": "basketball_nba",
    "NCAA Basketball": "basketball_ncaab",
    "NCAA Football": "americanfootball_ncaaf",
    "NFL": "americanfootball_nfl",
    "NHL": "icehockey_nhl",
    "MLB": "baseball_mlb",
    "MLB Spring Training": "baseball_mlb",
    # Combat sports
    "UFC": "mma_mixed_martial_arts",
    "MMA": "mma_mixed_martial_arts",
    "Boxing": "boxing_boxing",
    # Soccer
    "English Premier League": "soccer_epl",
    "EPL": "soccer_epl",
    "La Liga": "soccer_spain_la_liga",
    "Spanish La Liga": "soccer_spain_la_liga",
    "Bundesliga": "soccer_germany_bundesliga",
    "German Bundesliga": "soccer_germany_bundesliga",
    "Serie A": "soccer_italy_serie_a",
    "Italian Serie A": "soccer_italy_serie_a",
    "Ligue 1": "soccer_france_ligue_one",
    "French Ligue 1": "soccer_france_ligue_one",
    "UEFA Champions League": "soccer_uefa_champs_league",
    "Champions League": "soccer_uefa_champs_league",
    "MLS": "soccer_usa_mls",
    # Tennis
    "ATP Lines": "tennis_atp",
    "ATP": "tennis_atp",
    "WTA Lines": "tennis_wta",
    "WTA": "tennis_wta",
    "Tennis": "tennis_atp",
    # Skip these
    "NCAA Women, Regular Season": None,
    "PGA Round Matchups": None,
    "Featured": None,  # skip the main featured section (it's an aggregate)
}

# Keywords that indicate a futures market (skip these)
_FUTURES_KEYWORDS = frozenset([
    "wins", "champion", "coin toss", "futures", "conference",
    "division", "mvp", "award", "super bowl", "world series",
    "stanley cup", "nba finals", "pennant", "cy young", "heisman",
    "1st half", "1st quarter", "2nd half", "3rd quarter", "4th quarter",
])

# Sports we want to scrape
_SUPPORTED_SPORTS = {
    "basketball_nba", "basketball_ncaab",
    "americanfootball_nfl", "americanfootball_ncaaf",
    "icehockey_nhl", "baseball_mlb",
    "mma_mixed_martial_arts", "boxing_boxing",
    "soccer_epl", "soccer_spain_la_liga", "soccer_germany_bundesliga",
    "soccer_italy_serie_a", "soccer_france_ligue_one",
    "soccer_uefa_champs_league", "soccer_usa_mls",
    "tennis_atp", "tennis_wta",
}

# Cache TTL for parsed data
_CACHE_TTL = 60  # seconds — prefetch loop keeps cache warm every ~30s


class XBetSource(DataSource):
    """Fetches odds from XBet by parsing the server-rendered sportsbook HTML.

    XBet renders all sports on a single page (/sportsbook/). This source
    navigates there with Playwright and parses the DOM to extract odds.
    """

    def __init__(self):
        self._browser = None  # type: Any
        self._context = None  # type: Any
        self._page = None  # type: Any
        self._pw = None  # type: Any
        self._lock = asyncio.Lock()
        # Cache: sport_key -> (events, timestamp)
        self._cache: Dict[str, Tuple[List[OddsEvent], float]] = {}
        self._prefetch_task = None  # type: Any
        # Timestamp of last full page load
        self._last_load: float = 0.0

    def start_prefetch(self) -> None:
        """Start background prefetch (call after event loop is running)."""
        self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    async def _prefetch_all(self) -> None:
        """Background task: load the sportsbook page and parse all sports."""
        await asyncio.sleep(4)  # Let other sources initialize first
        logger.info("XBet: Starting continuous background prefetch")
        cycle = 0
        while True:
            cycle += 1
            try:
                async with self._lock:
                    await self._ensure_browser()
                    if self._page is None:
                        logger.warning("XBet: No browser page, skipping cycle %d", cycle)
                        await asyncio.sleep(30)
                        continue

                    # Load the main sportsbook page
                    all_events = await self._load_and_parse_all()

                    # Distribute events into per-sport caches
                    sport_counts = {}  # type: Dict[str, int]
                    for sport_key, events in all_events.items():
                        self._cache[sport_key] = (events, time.time())
                        sport_counts[sport_key] = len(events)

                    total = sum(sport_counts.values())
                    sports_str = " ".join(
                        f"{k.split('_', 1)[-1]}:{v}" for k, v in sorted(sport_counts.items()) if v > 0
                    )
                    logger.info(
                        "XBet prefetch cycle #%d: %d events across %d sports (%s)",
                        cycle, total, len(sport_counts), sports_str,
                    )

            except Exception as e:
                logger.warning("XBet prefetch error: %s: %s", type(e).__name__, e)
                # Reset browser on errors to recover
                await self._close_browser()

            await asyncio.sleep(30)  # Full page load takes ~5s; refresh every 30s for fresher data

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        headers = {"x-requests-remaining": "unlimited"}

        if bookmakers and "xbet" not in bookmakers:
            return [], headers

        if sport_key not in _SUPPORTED_SPORTS:
            return [], headers

        # Return from cache
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _CACHE_TTL:
            return cached[0], headers

        # Cache miss — prefetch hasn't run yet or cache expired
        logger.debug(
            "XBet cache miss for %s (cached=%s)", sport_key, cached is not None,
            )
        return [], headers

    # ------------------------------------------------------------------
    # Page loading & DOM parsing
    # ------------------------------------------------------------------

    async def _load_and_parse_all(self) -> Dict[str, List[OddsEvent]]:
        """Navigate to sportsbook and parse all games from the DOM."""
        if self._page is None:
            return {}

        try:
            await self._page.goto(
                f"{SITE_URL}/sportsbook/",
                timeout=60000,
                wait_until="load",
            )
            # Give JS time to render any dynamic content
            await asyncio.sleep(3)
        except Exception as e:
            logger.warning("XBet: Navigation error: %s: %s", type(e).__name__, e)
            return {}

        self._last_load = time.time()

        # Parse all game-line elements from the DOM.
        # XBet renders all sports on one page inside FEATURED sections.
        # Some sections have proper league titles (.league-title a),
        # others have empty titles but can be mapped via nav link IDs.
        raw_games = await self._page.evaluate("""
            () => {
                // Step 1: Build nav link ID → sport name mapping
                const navMap = {};
                const navLinks = document.querySelectorAll('a[href*="FEATURED-"]');
                for (const a of navLinks) {
                    const m = a.href.match(/FEATURED-(\\d+)/);
                    if (m) {
                        const text = a.textContent.trim();
                        if (text && !navMap[m[1]]) navMap[m[1]] = text;
                    }
                }

                // Step 2: Parse all FEATURED sections
                const result = [];
                const sections = document.querySelectorAll('[id*="FEATURED"]');
                const seenIds = new Set();  // deduplicate sections with same ID

                for (const section of sections) {
                    // Skip the aggregate "scroll-title-FEATURED" section (contains all games)
                    if (section.id === 'scroll-title-FEATURED') continue;

                    // Extract section ID number
                    const idMatch = section.id.match(/FEATURED-(\\d+)/);
                    const sectionId = idMatch ? idMatch[1] : '';

                    // Skip sections without a numeric ID (empty suffix = aggregate)
                    if (!sectionId) continue;

                    // Skip if we already processed this ID (XBet duplicates sections)
                    if (seenIds.has(sectionId)) continue;
                    seenIds.add(sectionId);

                    // Determine league name: prefer DOM title, fallback to nav mapping
                    let league = '';
                    const titleEl = section.querySelector('.league-title a') || section.querySelector('.league-title');
                    if (titleEl) {
                        league = titleEl.textContent.trim();
                    }
                    if (!league && sectionId && navMap[sectionId]) {
                        league = navMap[sectionId];
                    }

                    // Find all game-line elements in this section
                    const gameLines = section.querySelectorAll('.game-line');
                    for (const gl of gameLines) {
                        // Get Schema.org event data
                        const nameEl = gl.querySelector('[itemprop="name"]');
                        const startEl = gl.querySelector('[itemprop="startDate"]');
                        if (!nameEl) continue;

                        const matchName = (nameEl.getAttribute('content') || nameEl.textContent || '').trim();
                        const startDate = startEl ? (startEl.getAttribute('content') || startEl.textContent || '').trim() : '';
                        if (!matchName || matchName === 'Join Xbet Today') continue;

                        // Get odds from button/cell elements
                        const oddsCells = gl.querySelectorAll(
                            '.game-line__cell--spread, .game-line__cell--winner, .game-line__cell--total, ' +
                            '[class*="odds"], [class*="line-value"], .od__container button, ' +
                            'button.game-line__cell'
                        );
                        const oddsTexts = [];
                        for (const cell of oddsCells) {
                            const txt = cell.textContent.trim().replace(/\\s+/g, ' ');
                            if (txt && (txt.match(/[+-]\\d+/) || txt.match(/[OU]\\s*\\d/))) {
                                oddsTexts.push(txt);
                            }
                        }

                        // Fallback: extract odds patterns from od__container text
                        if (oddsTexts.length === 0) {
                            const odContainer = gl.querySelector('.od__container');
                            if (odContainer) {
                                const text = odContainer.textContent || '';
                                const patterns = text.match(/[+-]\\d{1,4}(?:\\.\\d)?(?:\\s+-\\d{2,4})?|[OU]\\s*\\d+\\.?\\d*\\s*-?\\d{2,4}/g);
                                if (patterns) {
                                    for (const p of patterns) {
                                        oddsTexts.push(p.trim());
                                    }
                                }
                            }
                        }

                        result.push({
                            league: league,
                            matchName: matchName,
                            startDate: startDate,
                            odds: oddsTexts,
                        });
                    }
                }
                return result;
            }
        """)

        # Parse raw data into OddsEvent objects, grouped by sport
        events_by_sport: Dict[str, List[OddsEvent]] = {}
        seen_event_ids: set = set()  # deduplicate across sections

        for raw in raw_games:
            league = raw.get("league", "")
            sport_key = self._resolve_league(league)
            if not sport_key:
                continue

            match_name = raw.get("matchName", "")
            start_date = raw.get("startDate", "")
            odds_texts = raw.get("odds", [])

            # Parse team names from "Team1 v Team2"
            teams = self._parse_match_name(match_name)
            if not teams:
                continue
            away_team, home_team = teams

            # Skip futures
            combined = (away_team + " " + home_team + " " + league).lower()
            if any(kw in combined for kw in _FUTURES_KEYWORDS):
                continue

            # Parse start time
            commence_time = self._parse_start_date(start_date)

            # Parse odds
            markets_list = self._parse_odds(odds_texts, away_team, home_team, sport_key)
            if not markets_list:
                continue

            cid = canonical_event_id(sport_key, home_team, away_team, commence_time)

            # Deduplicate (XBet renders some games in multiple sections)
            if cid in seen_event_ids:
                continue
            seen_event_ids.add(cid)

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
                        key="xbet",
                        title="XBet",
                        markets=markets_list,
                    )
                ],
            )

            if sport_key not in events_by_sport:
                events_by_sport[sport_key] = []
            events_by_sport[sport_key].append(event)

        return events_by_sport

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _resolve_league(self, league_text: str) -> Optional[str]:
        """Map an XBet league title to our sport_key."""
        if not league_text:
            return None
        # Exact match first
        sport_key = _LEAGUE_TO_SPORT.get(league_text)
        if sport_key is not None:
            return sport_key if sport_key in _SUPPORTED_SPORTS else None

        # Partial match
        lower = league_text.lower()
        for title, key in _LEAGUE_TO_SPORT.items():
            if title.lower() in lower or lower in title.lower():
                return key if key and key in _SUPPORTED_SPORTS else None

        return None

    @staticmethod
    def _parse_match_name(name: str) -> Optional[Tuple[str, str]]:
        """Parse 'Away Team v Home Team' into (away, home)."""
        if not name:
            return None
        # Split on " v " (XBet uses " v " separator)
        parts = name.split(" v ", 1)
        if len(parts) != 2:
            # Try " vs " as fallback
            parts = name.split(" vs ", 1)
        if len(parts) != 2:
            return None
        away = resolve_team_name(parts[0].strip())
        home = resolve_team_name(parts[1].strip())
        if not away or not home:
            return None
        return away, home

    @staticmethod
    def _parse_start_date(raw: str) -> str:
        """Parse XBet date format '2026 - 03 - 05 18:00-05:00' to ISO 8601."""
        if not raw:
            return ""
        # XBet format: "2026 - 03 - 05 18:00-05:00"
        # Clean up spaces around hyphens in the date portion
        cleaned = raw.strip()
        # Try to parse with the space-hyphen format
        try:
            # Remove spaces around date hyphens: "2026 - 03 - 05" → "2026-03-05"
            m = re.match(
                r"(\d{4})\s*-\s*(\d{2})\s*-\s*(\d{2})\s+(\d{2}:\d{2})([-+]\d{2}:\d{2})?",
                cleaned,
            )
            if m:
                date_str = f"{m.group(1)}-{m.group(2)}-{m.group(3)}T{m.group(4)}:00"
                tz_str = m.group(5) or "-05:00"  # Default to EST if no tz
                dt = datetime.fromisoformat(date_str + tz_str)
                return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            pass
        return raw

    def _parse_odds(
        self,
        odds_texts: List[str],
        away_team: str,
        home_team: str,
        sport_key: str,
    ) -> List[Market]:
        """Parse odds texts into Market objects.

        Expected pattern (6 items):
            [away_spread, away_ml, away_total, home_spread, home_ml, home_total]
        Where:
            spread = "+9 -110" or "-9 -110"
            ml = "+301" or "-400"
            total = "O 229.5 -110" or "U 229.5 -110"
        """
        markets = []  # type: List[Market]

        if len(odds_texts) < 6:
            # Try to parse whatever we have
            if len(odds_texts) >= 2:
                # At least 2 = might be just moneylines
                ml_away = self._safe_int(odds_texts[0])
                ml_home = self._safe_int(odds_texts[1])
                if ml_away is not None and ml_home is not None:
                    markets.append(Market(
                        key="h2h",
                        outcomes=[
                            Outcome(name=home_team, price=ml_home),
                            Outcome(name=away_team, price=ml_away),
                        ],
                    ))
            return markets

        # Standard 6-item pattern: spread, ml, total for away, then home
        # Away team odds
        away_spread_text = odds_texts[0]  # "+9 -110"
        away_ml_text = odds_texts[1]       # "+301"
        away_total_text = odds_texts[2]    # "O 229.5 -110"
        # Home team odds
        home_spread_text = odds_texts[3]  # "-9 -110"
        home_ml_text = odds_texts[4]       # "-400"
        home_total_text = odds_texts[5]    # "U 229.5 -110"

        # --- Moneyline ---
        away_ml = self._safe_int(away_ml_text)
        home_ml = self._safe_int(home_ml_text)
        if away_ml is not None and home_ml is not None:
            outcomes = [
                Outcome(name=home_team, price=home_ml),
                Outcome(name=away_team, price=away_ml),
            ]
            # Check for draw (soccer): if there are 9 odds, index 6 might be draw
            if len(odds_texts) >= 9 and sport_key.startswith("soccer_"):
                draw_ml = self._safe_int(odds_texts[6])
                if draw_ml is not None:
                    outcomes.append(Outcome(name="Draw", price=draw_ml))
            markets.append(Market(key="h2h", outcomes=outcomes))

        # --- Spreads ---
        away_sp = self._parse_spread_text(away_spread_text)
        home_sp = self._parse_spread_text(home_spread_text)
        if away_sp and home_sp:
            markets.append(Market(
                key="spreads",
                outcomes=[
                    Outcome(name=home_team, price=home_sp[1], point=home_sp[0]),
                    Outcome(name=away_team, price=away_sp[1], point=away_sp[0]),
                ],
            ))

        # --- Totals ---
        over = self._parse_total_text(away_total_text)
        under = self._parse_total_text(home_total_text)
        if over and under:
            # Use the same point value (they should match)
            point = over[0]
            markets.append(Market(
                key="totals",
                outcomes=[
                    Outcome(name="Over", price=over[1], point=point),
                    Outcome(name="Under", price=under[1], point=point),
                ],
            ))

        return markets

    @staticmethod
    def _parse_spread_text(text: str) -> Optional[Tuple[float, int]]:
        """Parse '+9 -110' into (9.0, -110)."""
        m = re.match(r"([+-]?\d+\.?\d*)\s+([+-]\d+)", text.strip())
        if m:
            try:
                return float(m.group(1)), int(m.group(2))
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _parse_total_text(text: str) -> Optional[Tuple[float, int]]:
        """Parse 'O 229.5 -110' or 'U 229.5 -110' into (229.5, -110)."""
        m = re.match(r"[OU]\s*(\d+\.?\d*)\s+([+-]\d+)", text.strip())
        if m:
            try:
                return float(m.group(1)), int(m.group(2))
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _safe_int(text: str) -> Optional[int]:
        """Extract first integer from text like '+301' or '-400'."""
        if not text:
            return None
        m = re.search(r"([+-]?\d+)", text.strip())
        if m:
            try:
                return int(m.group(1))
            except (ValueError, TypeError):
                return None
        return None

    # ------------------------------------------------------------------
    # Browser management
    # ------------------------------------------------------------------

    async def _ensure_browser(self) -> None:
        """Launch Playwright browser with stealth mode."""
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
                logger.info("XBet: Launched system Chrome")
            except Exception:
                self._browser = await self._pw.chromium.launch(
                    headless=True,
                    args=launch_args,
                )
                logger.info("XBet: Launched bundled Chromium (fallback)")

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
                await Stealth().apply_stealth_async(self._context)
            except ImportError:
                await self._context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    window.chrome = {runtime: {}};
                """)
            self._page = await self._context.new_page()
            logger.info("XBet: Playwright browser launched (stealth mode)")
        except Exception as e:
            logger.warning("XBet: Failed to launch browser: %s", e)
            self._page = None

    async def _close_browser(self) -> None:
        """Close and reset the browser for recovery."""
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
            logger.debug("XBet: Error closing browser: %s", e)
        finally:
            self._page = None
            self._context = None
            self._browser = None
            self._pw = None

    async def close(self) -> None:
        """Shut down the Playwright browser."""
        await self._close_browser()
