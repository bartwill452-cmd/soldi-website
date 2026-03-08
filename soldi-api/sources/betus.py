"""
BetUS sportsbook scraper.

Scrapes odds from betus.com.pa by fetching their sportsbook HTML pages
and parsing the game-block DOM structure. BetUS uses server-rendered
HTML (no SPA), so we can scrape with a headless browser + DOM parsing.

No login required — public odds are visible without authentication.

DOM structure per game (.game-block):
  - Team names: span#awayName, span#homeName
  - Markets: div.line-container with Spread, Total, Money columns
  - Each column has .team-line elements for away/home
  - Date: span.time
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
_STALE_TTL = 300  # serve stale up to 5 min

# JS to extract all game data from the DOM
_JS_EXTRACT = """
() => {
    const results = [];
    const blocks = document.querySelectorAll('.game-block, .gameblock, [class*="game-block"]');

    blocks.forEach(block => {
        const ev = {};

        // Team names
        const awayEl = block.querySelector('#awayName, [id*="awayName"], .away-name, .team-name:first-child');
        const homeEl = block.querySelector('#homeName, [id*="homeName"], .home-name, .team-name:last-child');

        if (!awayEl || !homeEl) {
            // Try alternate: look for team rows
            const rows = block.querySelectorAll('.team-line, .team-row, tr');
            if (rows.length >= 2) {
                const awayText = rows[0].querySelector('.team-name, td:first-child');
                const homeText = rows[1].querySelector('.team-name, td:first-child');
                ev.away = awayText ? awayText.textContent.trim() : '';
                ev.home = homeText ? homeText.textContent.trim() : '';
            } else {
                return;
            }
        } else {
            ev.away = awayEl.textContent.trim();
            ev.home = homeEl.textContent.trim();
        }

        if (!ev.away || !ev.home) return;

        // Date/time
        const timeEl = block.querySelector('.time, .game-time, .date-time, [class*="time"]');
        ev.time = timeEl ? timeEl.textContent.trim() : '';

        // Parse markets from line-container or table structure
        ev.spread = { away: {}, home: {} };
        ev.total = {};
        ev.money = { away: null, home: null };

        // Look for spread, total, money columns
        const containers = block.querySelectorAll('.line-container, .odds-container, .market-column');
        containers.forEach((c, index) => {
            const headerEl = c.querySelector('.line-header, .header, th, h4, h5');
            const header = headerEl ? headerEl.textContent.trim().toLowerCase() : '';

            const lines = c.querySelectorAll('.team-line, .odd-line, .line, td');

            if (header.includes('spread') || header.includes('handicap') || (index === 0 && !header && containers.length === 3)) {
                if (lines.length >= 2) {
                    ev.spread.away = parseLine(lines[0]);
                    ev.spread.home = parseLine(lines[1]);
                }
            } else if (header.includes('total') || header.includes('o/u') || header.includes('over') || header.includes('under') || header.includes('points') || (index === 1 && !header && containers.length === 3)) {
                if (lines.length >= 2) {
                    ev.total.over = parseLine(lines[0]);
                    ev.total.under = parseLine(lines[1]);
                }
            } else if (header.includes('money') || header.includes('ml') || header.includes('winner') || (index === 2 && !header && containers.length === 3)) {
                if (lines.length >= 2) {
                    ev.money.away = parseOdds(lines[0]);
                    ev.money.home = parseOdds(lines[1]);
                }
            }
        });

        // Alternate: parse from table rows (some BetUS pages use tables)
        if (!ev.money.away && !ev.money.home) {
            const allText = block.textContent;
            // Try to find American odds patterns like +150, -200
            const oddsPattern = /([+-]\\d{3,4})/g;
            const allOdds = allText.match(oddsPattern) || [];
            // If we have at least 6 numbers, assume: away_spread_odds, away_total_odds, away_ml, home_spread_odds, home_total_odds, home_ml
            if (allOdds.length >= 6) {
                ev.money.away = parseInt(allOdds[2]);
                ev.money.home = parseInt(allOdds[5]);
            } else if (allOdds.length >= 2) {
                ev.money.away = parseInt(allOdds[0]);
                ev.money.home = parseInt(allOdds[1]);
            }

            // Try spread parsing from text
            const spreadPattern = /([+-]?\\d+\\.?\\d*)\\s*([+-]\\d{3})/g;
            let spreadMatch;
            const spreads = [];
            const tempText = allText;
            while ((spreadMatch = spreadPattern.exec(tempText)) !== null) {
                spreads.push({ point: parseFloat(spreadMatch[1]), odds: parseInt(spreadMatch[2]) });
            }
            if (spreads.length >= 2 && !ev.spread.away.point) {
                ev.spread.away = spreads[0];
                ev.spread.home = spreads[1];
            }
        }

        results.push(ev);
    });

    function parseLine(el) {
        if (!el) return {};
        const text = el.textContent.trim();
        // Look for point + odds pattern: "+6.5 -110" or "o218.5 -108"
        const match = text.match(/([+-]?[oOuU]?\\d+\\.?\\d*)\\s*([+-]\\d{3,4})/);
        if (match) {
            let point = match[1].replace(/^[oOuU]/, '');
            return { point: parseFloat(point), odds: parseInt(match[2]) };
        }
        // Just odds
        const oddsMatch = text.match(/([+-]\\d{3,4})/);
        if (oddsMatch) return { odds: parseInt(oddsMatch[1]) };
        return {};
    }

    function parseOdds(el) {
        if (!el) return null;
        const text = el.textContent.trim();
        const match = text.match(/([+-]\\d{3,4})/);
        return match ? parseInt(match[1]) : null;
    }

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
            await self._page.goto(url, timeout=30000, wait_until="load")
            await asyncio.sleep(5)  # Wait for content to render
        except Exception as e:
            logger.warning("BetUS: Navigation to %s failed: %s", url, e)
            return []

        try:
            raw_events = await self._page.evaluate(_JS_EXTRACT)
        except Exception as e:
            logger.warning("BetUS: DOM extraction failed for %s: %s", sport_key, e)
            return []

        if not raw_events:
            logger.debug("BetUS: No events found for %s", sport_key)
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
