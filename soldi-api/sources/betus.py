"""
BetUS sportsbook scraper.

Scrapes odds from betus.com.pa by fetching their sportsbook HTML pages
and parsing the game-block DOM structure. BetUS uses server-rendered
HTML (no SPA), so we can scrape with a headless browser + DOM parsing.

No login required — public odds are visible without authentication.

DOM structure:
  - Main wrapper: div.game-block (SINGLE container for ALL games)
  - Per-game container: div.game-tbl
  - Away team: div.visitor -> span#awayName (inside awayTeamContainer)
  - Home team: div.home -> span#homeName (inside homeTeamContainer)
  - Markets: div.line-container with a.bet-link for each odds value
  - Line order per team row: Spread, Moneyline, Total (in column order)
  - Time: span.time inside .gamelines-info-row
  - ASP.NET WebForms server-rendered HTML, no SPA/XHR for odds data
"""

import asyncio
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

SITE_URL = "https://www.betus.com.pa"

# OddsScreen sport_key -> BetUS URL slug
_SPORT_SLUGS: Dict[str, str] = {
    "basketball_nba": "basketball/nba",
    "basketball_ncaab": "basketball/college-basketball",
    "americanfootball_nfl": "football/nfl",
    "americanfootball_ncaaf": "football/college-football",
    "icehockey_nhl": "hockey/nhl",
    "baseball_mlb": "baseball/mlb",
    "mma_mixed_martial_arts": "martial-arts/ufc",
    "boxing_boxing": "boxing",
    "soccer_epl": "soccer/english-premier-league",
    "soccer_spain_la_liga": "soccer/spanish-la-liga",
    "soccer_germany_bundesliga": "soccer/german-bundesliga",
    "soccer_italy_serie_a": "soccer/italian-serie-a",
    "soccer_france_ligue_one": "soccer/french-ligue-1",
    "soccer_uefa_champs_league": "soccer/champions-league",
    "tennis_atp": "tennis",
    "tennis_wta": "tennis",
}

_CACHE_TTL = 90  # seconds
_STALE_TTL = 600  # serve stale up to 10 min (prefetch cycles are long ~4-7 min)

# JS to extract all game data from the DOM.
# BetUS uses ASP.NET WebForms with server-rendered HTML.
# Each game is in a .game-tbl div (NOT .game-block, which is a wrapper).
_JS_EXTRACT = """
() => {
    const results = [];
    const games = document.querySelectorAll('.game-tbl');

    games.forEach(game => {
        const ev = {};

        // Team names — visitor (away) and home sections
        const visitorSection = game.querySelector('.visitor');
        const homeSection = game.querySelector('.home');
        if (!visitorSection || !homeSection) return;

        // Team names: look for awayName/homeName spans, or fallback to .team .t-desc links
        const awayNameEl = visitorSection.querySelector('[id*="awayName"] a, [id*="awayName"], .team.t-desc a, .team a');
        const homeNameEl = homeSection.querySelector('[id*="homeName"] a, [id*="homeName"], .team.t-desc a, .team a');

        ev.away = awayNameEl ? awayNameEl.textContent.trim() : '';
        ev.home = homeNameEl ? homeNameEl.textContent.trim() : '';
        if (!ev.away || !ev.home) return;

        // Game time — from the info row above the team rows
        const timeEl = game.querySelector('.time span, .time');
        ev.time = timeEl ? timeEl.textContent.trim() : '';

        // Markets: extract from bet-link anchors inside line-container divs
        // Column order per row: Spread | Moneyline | Total | Team Total
        // Visitor row = .visitor-lines .line-container
        // Home row = .home-lines .line-container

        const visitorLines = visitorSection.querySelector('.visitor-lines');
        const homeLines = homeSection.querySelector('.home-lines');

        // Get all line-container divs (spread, moneyline, total, team-total)
        const vContainers = visitorLines ? visitorLines.querySelectorAll('.line-container, .line-container-teamtotal') : [];
        const hContainers = homeLines ? homeLines.querySelectorAll('.line-container, .line-container-teamtotal') : [];

        ev.spread = { away: {}, home: {} };
        ev.total = {};
        ev.money = { away: null, home: null };

        // Parse line text from a container's bet-link
        function parseBetLink(container) {
            if (!container) return null;
            const link = container.querySelector('a.bet-link');
            if (!link) return null;
            // Get direct text only (not nested spans like "Added")
            let text = '';
            link.childNodes.forEach(node => {
                if (node.nodeType === 3) text += node.textContent;
            });
            text = text.trim();
            if (!text) text = link.textContent.trim();
            return text || null;
        }

        // Parse spread text: "+4 -115" or "-1½ +105"
        function parseSpread(text) {
            if (!text) return {};
            text = text.replace(/½/g, '.5');
            const match = text.match(/([+-]?\\d+\\.?\\d*)\\s*([+-]\\d{3,4})/);
            if (match) return { point: parseFloat(match[1]), odds: parseInt(match[2]) };
            return {};
        }

        // Parse moneyline text: "+600" or "-900"
        function parseML(text) {
            if (!text) return null;
            const match = text.match(/([+-]\\d{3,4})/);
            return match ? parseInt(match[1]) : null;
        }

        // Parse total text: "O 228½ -105" or "U 228½ -115"
        function parseTotal(text) {
            if (!text) return {};
            text = text.replace(/½/g, '.5');
            const match = text.match(/[oOuU]\\s*([\\d.]+)\\s*([+-]\\d{3,4})/);
            if (match) return { point: parseFloat(match[1]), odds: parseInt(match[2]) };
            return {};
        }

        // Column order: 0=Spread, 1=Moneyline, 2=Total, 3=TeamTotal
        if (vContainers.length >= 3) {
            ev.spread.away = parseSpread(parseBetLink(vContainers[0]));
            ev.money.away = parseML(parseBetLink(vContainers[1]));
            ev.total.over = parseTotal(parseBetLink(vContainers[2]));
        }
        if (hContainers.length >= 3) {
            ev.spread.home = parseSpread(parseBetLink(hContainers[0]));
            ev.money.home = parseML(parseBetLink(hContainers[1]));
            ev.total.under = parseTotal(parseBetLink(hContainers[2]));
        }

        results.push(ev);
    });

    return results;
}
"""


class BetUSSource(DataSource):
    """Fetches odds from BetUS via Playwright DOM scraping."""

    def __init__(self):
        self._browser = None  # type: Any
        self._context = None  # type: Any
        self._page = None  # type: Any
        self._pw = None  # type: Any
        self._lock = asyncio.Lock()
        self._cache: Dict[str, Tuple[List[OddsEvent], float]] = {}
        self._prefetch_task = None  # type: Any

    def start_prefetch(self) -> None:
        self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    async def _prefetch_all(self) -> None:
        await asyncio.sleep(20)  # Stagger after other scrapers
        logger.info("BetUS: Starting continuous background prefetch")
        cycle = 0
        while True:
            cycle += 1
            async with self._lock:
                try:
                    await self._ensure_browser()
                    if self._page is None:
                        await asyncio.sleep(30)
                        continue
                    for sport_key, slug in _SPORT_SLUGS.items():
                        try:
                            events = await self._fetch_sport(sport_key, slug)
                            # Only update cache if we got events (preserve
                            # previous good data when page fails to load)
                            if events:
                                self._cache[sport_key] = (events, time.time())
                            logger.info("BetUS prefetch: %d events for %s", len(events), sport_key)
                        except Exception as e:
                            logger.warning("BetUS prefetch %s failed: %s", sport_key, e)
                        await asyncio.sleep(2)
                except Exception as e:
                    logger.warning("BetUS prefetch error: %s", e)
            logger.info("BetUS: Prefetch cycle #%d complete", cycle)
            await asyncio.sleep(30)

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
                    "--disable-dev-shm-usage",
                ],
            )
            self._context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
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
            logger.info("BetUS: Playwright browser launched")
        except Exception as e:
            logger.warning("BetUS: Failed to launch browser: %s", e)
            self._page = None

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        headers = {"x-requests-remaining": "unlimited"}

        if bookmakers and "betus" not in bookmakers:
            return [], headers

        if sport_key not in _SPORT_SLUGS:
            return [], headers

        # Serve from cache
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _STALE_TTL:
            return cached[0], headers
        return [], headers

    async def _fetch_sport(self, sport_key: str, slug: str) -> List[OddsEvent]:
        """Navigate to BetUS sport page and scrape game blocks."""
        if self._page is None:
            return []

        url = f"{SITE_URL}/sportsbook/{slug}"
        try:
            await self._page.goto(url, timeout=45000, wait_until="load")
        except Exception as e:
            logger.warning("BetUS: Navigation to %s failed: %s", url, e)
            return []

        # Cloudflare bot protection: BetUS triggers a JS challenge after
        # the first page load.  Wait for it to auto-solve (the challenge
        # runs JS in the browser and redirects when complete).
        for attempt in range(6):  # up to 30 seconds
            is_cf = await self._page.evaluate("""
                () => document.body.innerText.includes('security verification')
                    || document.body.innerText.includes('security service')
                    || document.body.innerText.includes('Checking your browser')
                    || document.body.innerText.includes('Just a moment')
                    || document.querySelector('#challenge-running') !== null
                    || document.querySelector('.cf-browser-verification') !== null
            """)
            if not is_cf:
                break
            if attempt == 0:
                logger.info("BetUS: Cloudflare challenge detected for %s, waiting...", sport_key)
            await asyncio.sleep(5)
        else:
            logger.warning("BetUS: Cloudflare challenge did not resolve for %s", sport_key)
            return []

        # Wait for game containers to appear after CF challenge clears
        try:
            await self._page.wait_for_selector(
                ".game-tbl, .bn-lines, .game-block", timeout=15000,
            )
        except Exception:
            # May genuinely have no games — fall through to extraction
            await asyncio.sleep(3)

        # Quick DOM check: what's on the page?
        try:
            dom_check = await self._page.evaluate("""
                () => {
                    const body = document.body;
                    const html = body.innerHTML;
                    // Get first 500 chars of visible text (skip scripts/styles)
                    let textContent = body.innerText || '';
                    textContent = textContent.replace(/\\s+/g, ' ').trim().substring(0, 300);
                    return {
                        url: window.location.href,
                        title: document.title,
                        gameTbls: document.querySelectorAll('.game-tbl').length,
                        gameBlocks: document.querySelectorAll('.game-block').length,
                        bnLines: document.querySelectorAll('.bn-lines').length,
                        bodyLen: html.length,
                        textSnippet: textContent,
                    };
                }
            """)
            logger.info(
                "BetUS DOM check %s: url=%s gameTbls=%d gameBlocks=%d bodyLen=%d text=%.200s",
                sport_key, dom_check.get("url", "?"),
                dom_check.get("gameTbls", 0), dom_check.get("gameBlocks", 0),
                dom_check.get("bodyLen", 0), dom_check.get("textSnippet", ""),
            )
        except Exception as e:
            logger.warning("BetUS: DOM check failed for %s: %s", sport_key, e)

        try:
            raw_events = await self._page.evaluate(_JS_EXTRACT)
        except Exception as e:
            logger.warning("BetUS: DOM extraction failed for %s: %s", sport_key, e)
            return []

        if not raw_events:
            logger.info("BetUS: No events found for %s (0 game-tbl matched)", sport_key)
            return []

        sport_title = get_sport_title(sport_key)
        events = []

        for ev in raw_events:
            away_team = resolve_team_name(ev.get("away", "").strip())
            home_team = resolve_team_name(ev.get("home", "").strip())
            if not away_team or not home_team:
                continue

            betus_markets = []

            # Moneyline
            away_ml = ev.get("money", {}).get("away")
            home_ml = ev.get("money", {}).get("home")
            if away_ml and home_ml and away_ml != 0 and home_ml != 0:
                betus_markets.append(Market(
                    key="h2h",
                    outcomes=[
                        Outcome(name=home_team, price=int(home_ml)),
                        Outcome(name=away_team, price=int(away_ml)),
                    ],
                ))

            # Spreads
            away_sp = ev.get("spread", {}).get("away", {})
            home_sp = ev.get("spread", {}).get("home", {})
            if (away_sp.get("point") is not None and away_sp.get("odds") is not None
                    and home_sp.get("point") is not None and home_sp.get("odds") is not None):
                betus_markets.append(Market(
                    key="spreads",
                    outcomes=[
                        Outcome(name=home_team, price=int(home_sp["odds"]), point=float(home_sp["point"])),
                        Outcome(name=away_team, price=int(away_sp["odds"]), point=float(away_sp["point"])),
                    ],
                ))

            # Totals
            total_over = ev.get("total", {}).get("over", {})
            total_under = ev.get("total", {}).get("under", {})
            if (total_over.get("point") is not None and total_over.get("odds") is not None
                    and total_under.get("point") is not None and total_under.get("odds") is not None):
                betus_markets.append(Market(
                    key="totals",
                    outcomes=[
                        Outcome(name="Over", price=int(total_over["odds"]), point=float(total_over["point"])),
                        Outcome(name="Under", price=int(total_under["odds"]), point=float(total_under["point"])),
                    ],
                ))

            if not betus_markets:
                continue

            commence_time = self._parse_time(ev.get("time", ""))
            cid = canonical_event_id(sport_key, home_team, away_team, commence_time)
            event_url = f"{SITE_URL}/sportsbook/{slug}"

            events.append(OddsEvent(
                id=cid,
                sport_key=sport_key,
                sport_title=sport_title,
                commence_time=commence_time,
                home_team=home_team,
                away_team=away_team,
                bookmakers=[
                    Bookmaker(
                        key="betus",
                        title="BetUS",
                        markets=betus_markets,
                        event_url=event_url,
                    )
                ],
            ))

        return events

    @staticmethod
    def _parse_time(time_str: str) -> str:
        """Parse BetUS time string to ISO 8601."""
        if not time_str:
            return ""
        # BetUS shows times like "03/08 7:00 PM ET"
        try:
            from datetime import datetime, timedelta, timezone as tz
            # Clean up the string
            time_str = time_str.strip()
            time_str = re.sub(r"\s+ET\s*$", "", time_str, flags=re.IGNORECASE)
            time_str = re.sub(r"\s+EST\s*$", "", time_str, flags=re.IGNORECASE)
            time_str = re.sub(r"\s+EDT\s*$", "", time_str, flags=re.IGNORECASE)

            # Try common BetUS formats
            from datetime import datetime as dt_cls
            now = dt_cls.now()
            for fmt in [
                "%m/%d %I:%M %p",
                "%m/%d/%Y %I:%M %p",
                "%b %d %I:%M %p",
                "%m/%d %H:%M",
            ]:
                try:
                    parsed = dt_cls.strptime(time_str, fmt)
                    # Add current year if not in format
                    if parsed.year == 1900:
                        parsed = parsed.replace(year=now.year)
                    # Eastern time (UTC-5)
                    et = tz(timedelta(hours=-5))
                    parsed = parsed.replace(tzinfo=et)
                    return parsed.astimezone(tz.utc).isoformat().replace("+00:00", "Z")
                except ValueError:
                    continue
        except Exception:
            pass
        return ""

    async def close(self) -> None:
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
