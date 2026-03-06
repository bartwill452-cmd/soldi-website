"""
DraftKings sportsbook scraper.

Uses Playwright headless browser to navigate to DraftKings' sportsbook pages
and passively capture the sportsbook-nash API responses. This bypasses the
geo/bot protection that blocks direct HTTP API access (403).

Strategy:
  1. Navigate to sportsbook.draftkings.com/leagues/{sport} pages
  2. Intercept XHR responses from sportsbook-nash.draftkings.com
  3. Parse the flat {events, markets, selections} response
  4. Navigate to subcategory pages (?category=player-points, etc.) to get
     player props and team totals
  5. For tennis, dynamically discover tournament URLs from the hub page
  6. For team totals, query the API directly with subcategoryId 4609 (NBA)
     since team total data lives in a separate subcategory
  7. Cache results with 120s TTL
"""

import asyncio
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

# ── DraftKings sportsbook navigation URLs per sport_key ──────────────
_DK_SPORT_URLS = {
    "basketball_nba": "https://sportsbook.draftkings.com/leagues/basketball/nba",
    "basketball_ncaab": "https://sportsbook.draftkings.com/leagues/basketball/ncaab",
    "americanfootball_nfl": "https://sportsbook.draftkings.com/leagues/football/nfl",
    "americanfootball_ncaaf": "https://sportsbook.draftkings.com/leagues/football/college-football",
    "icehockey_nhl": "https://sportsbook.draftkings.com/leagues/hockey/nhl",
    "baseball_mlb": "https://sportsbook.draftkings.com/leagues/baseball/mlb",
    "mma_mixed_martial_arts": "https://sportsbook.draftkings.com/leagues/mma/ufc",
    "boxing_boxing": "https://sportsbook.draftkings.com/leagues/mma/boxing",
    "soccer_epl": "https://sportsbook.draftkings.com/leagues/soccer/england---premier-league",
    "soccer_spain_la_liga": "https://sportsbook.draftkings.com/leagues/soccer/spain---la-liga",
    "soccer_germany_bundesliga": "https://sportsbook.draftkings.com/leagues/soccer/germany---bundesliga",
    "soccer_italy_serie_a": "https://sportsbook.draftkings.com/leagues/soccer/italy---serie-a",
    "soccer_france_ligue_one": "https://sportsbook.draftkings.com/leagues/soccer/france---ligue-1",
    "soccer_usa_mls": "https://sportsbook.draftkings.com/leagues/soccer/mls",
    "soccer_uefa_champs_league": "https://sportsbook.draftkings.com/leagues/soccer/uefa-champions-league",
    "tennis_atp": "https://sportsbook.draftkings.com/leagues/tennis/atp",
    "tennis_wta": "https://sportsbook.draftkings.com/leagues/tennis/wta",
}

# Player prop category pages to scrape (appended as ?category= on league page)
_PLAYER_PROP_CATS = [
    "player-points",
    "player-rebounds",
    "player-assists",
    "player-threes",
    "player-combos",
    "player-defense",
]

# Half / quarter category pages to scrape for period-specific markets
_PERIOD_CATS = [
    "1st-half",
    "2nd-half",
    "1st-quarter",
    "2nd-quarter",
    "3rd-quarter",
    "4th-quarter",
]

# Sports that only use halves (no quarters) — skip quarter pages for these
_HALVES_ONLY_SPORTS = {
    "basketball_ncaab",
    "basketball_euroleague",
}

# Sports that have player prop pages on DraftKings
_SPORTS_WITH_PLAYER_PROPS = {
    "basketball_nba",
    "basketball_ncaab",
    "icehockey_nhl",
    "baseball_mlb",
    "americanfootball_nfl",
    "americanfootball_ncaaf",
}

# Sports that have half/quarter pages on DraftKings
_SPORTS_WITH_PERIODS = {
    "basketball_nba",
    "basketball_ncaab",
    "icehockey_nhl",
    "americanfootball_nfl",
    "americanfootball_ncaaf",
    "baseball_mlb",
}

# Tennis hub page for dynamic tournament discovery
_TENNIS_HUB = "https://sportsbook.draftkings.com/sports/tennis"

_CACHE_TTL = 120  # seconds — prefetch loop takes ~80s to cycle all sports
_STALE_TTL = 900  # seconds — serve stale data up to 15 minutes (prefetch cycle ~13min)

# Unicode minus sign used in DraftKings displayOdds
_UNICODE_MINUS = "\u2212"  # −


def _parse_dk_american_odds(odds_str: str) -> Optional[int]:
    """Parse DraftKings american odds string, handling unicode minus sign."""
    if not odds_str:
        return None
    cleaned = odds_str.replace(_UNICODE_MINUS, "-").strip()
    if cleaned.startswith("+"):
        cleaned = cleaned[1:]
    try:
        return int(cleaned)
    except ValueError:
        return None


class DraftKingsSource(DataSource):
    """Fetches odds from DraftKings via Playwright passive response capture."""

    def __init__(self):
        self._browser = None  # type: Any
        self._context = None  # type: Any
        self._page = None  # type: Any
        self._pw = None  # type: Any
        self._lock = asyncio.Lock()
        self._cache = {}  # type: Dict[str, Tuple[List[OddsEvent], float]]
        self._prefetch_task = None  # type: Any
        # Cache discovered tennis tournament URLs
        self._tennis_urls = {}  # type: Dict[str, List[str]]
        self._tennis_urls_ts = 0.0

    def start_prefetch(self) -> None:
        """Start background prefetch of major sports."""
        self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    async def _prefetch_all(self) -> None:
        await asyncio.sleep(3)
        logger.info("DraftKings: Starting continuous background prefetch")
        # Prioritize simple sports (MMA/boxing = main page only) first,
        # then heavier sports with sub-pages (props, periods)
        all_sports = [
            "mma_mixed_martial_arts",
            "boxing_boxing",
            "icehockey_nhl",
            "baseball_mlb",
            "basketball_nba",
            "basketball_ncaab",
            "americanfootball_nfl",
            "americanfootball_ncaaf",
            "soccer_epl",
            "soccer_spain_la_liga",
            "soccer_germany_bundesliga",
            "soccer_italy_serie_a",
            "soccer_france_ligue_one",
            "soccer_usa_mls",
            "soccer_uefa_champs_league",
            "tennis_atp",
            "tennis_wta",
        ]
        cycle = 0
        while True:
            cycle += 1
            for sport_key in all_sports:
                try:
                    async with self._lock:
                        await self._ensure_browser()
                        is_tennis = sport_key in ("tennis_atp", "tennis_wta")
                        if is_tennis:
                            events = await self._fetch_tennis(sport_key)
                        else:
                            url = _DK_SPORT_URLS.get(sport_key)
                            if url is None:
                                continue
                            events = await self._fetch_sport(url, sport_key)
                        self._cache[sport_key] = (events, time.time())
                    logger.info("DraftKings prefetch: %s complete (%d events)", sport_key, len(events))
                except Exception as e:
                    logger.warning("DraftKings prefetch %s failed: %s", sport_key, e)
                await asyncio.sleep(0.5)
            logger.info("DraftKings: Prefetch cycle #%d complete (%d sports)", cycle, len(all_sports))
            await asyncio.sleep(1)

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
            logger.info("DraftKings: Playwright browser launched")
        except Exception as e:
            logger.warning("DraftKings: Failed to launch browser: %s", e)
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

        if bookmakers and "draftkings" not in bookmakers:
            return [], headers

        is_tennis = sport_key in ("tennis_atp", "tennis_wta")
        if not is_tennis:
            url = _DK_SPORT_URLS.get(sport_key)
            if url is None:
                return [], headers

        # Always serve from cache — prefetch loop keeps it warm.
        # Never fall through to Playwright navigation in the composite context.
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _STALE_TTL:
            return cached[0], headers
        return [], headers

    async def get_player_props(
        self, sport_key: str, event_id: str
    ) -> list:
        """Player props not yet implemented per-event for DraftKings."""
        return []

    # ── Fetching ──────────────────────────────────────────────────────

    async def _fetch_sport(
        self, base_url: str, sport_key: str,
    ) -> List[OddsEvent]:
        """Fetch game lines + player props + team totals for a sport."""
        # 1. Main game lines (Moneyline, Spread, Total)
        events = await self._navigate_and_capture(base_url, sport_key)

        # 2. Team totals via subcategory page (skip for combat sports — no team totals)
        if sport_key not in ("mma_mixed_martial_arts", "boxing_boxing"):
            team_url = base_url + "?category=team-props"
            team_events = await self._navigate_and_capture(
                team_url, sport_key, is_team_props=True
            )
            events = self._merge_events(events, team_events)

        # 3. Half / quarter period pages (for supported sports)
        if sport_key in _SPORTS_WITH_PERIODS:
            for period_cat in _PERIOD_CATS:
                # Skip quarter pages for halves-only sports (NCAAB, etc.)
                if sport_key in _HALVES_ONLY_SPORTS and "quarter" in period_cat:
                    continue
                period_url = base_url + "?category=" + period_cat
                period_events = await self._navigate_and_capture(
                    period_url, sport_key
                )
                events = self._merge_events(events, period_events)

        # 4. Player props (for supported sports)
        if sport_key in _SPORTS_WITH_PLAYER_PROPS:
            for prop_cat in _PLAYER_PROP_CATS:
                prop_url = base_url + "?category=" + prop_cat
                prop_events = await self._navigate_and_capture(
                    prop_url, sport_key,
                    is_player_props=True,
                    prop_category=prop_cat,
                )
                events = self._merge_events(events, prop_events)

        return events

    async def _fetch_tennis(self, sport_key: str) -> List[OddsEvent]:
        """Discover and scrape tennis tournament pages dynamically."""
        prefix = "atp" if sport_key == "tennis_atp" else "wta"

        # Discover tournament URLs from the hub (cache for 10 minutes)
        if time.time() - self._tennis_urls_ts > 600:
            await self._discover_tennis_urls()

        urls = self._tennis_urls.get(prefix, [])
        if not urls:
            logger.info("DraftKings: No %s tennis URLs discovered", prefix.upper())
            return []

        all_events = []  # type: List[OddsEvent]
        for url in urls:
            events = await self._navigate_and_capture(url, sport_key)
            all_events = self._merge_events(all_events, events)

        return all_events

    async def _discover_tennis_urls(self) -> None:
        """Navigate to the tennis hub and extract tournament league links."""
        await self._ensure_browser()
        if self._page is None:
            return

        try:
            await self._page.goto(
                _TENNIS_HUB, timeout=15000, wait_until="domcontentloaded"
            )
            await asyncio.sleep(2)

            links = await self._page.evaluate("""
                () => {
                    const results = [];
                    document.querySelectorAll('a[href*="/leagues/tennis/"]').forEach(a => {
                        const href = a.href;
                        if (href.includes('doubles') || href.includes('specials') ||
                            href.includes('futures') || href.includes('login')) return;
                        if (!results.includes(href)) results.push(href);
                    });
                    return results;
                }
            """)

            atp_urls = []  # type: List[str]
            wta_urls = []  # type: List[str]
            for link in links:
                lower = link.lower()
                if any(x in lower for x in ["/atp", "australian-open-men",
                       "french-open-men", "wimbledon-men", "us-open-men"]):
                    atp_urls.append(link)
                elif any(x in lower for x in ["/wta", "australian-open-women",
                         "french-open-women", "wimbledon-women", "us-open-women"]):
                    wta_urls.append(link)
                elif "/challenger" in lower:
                    pass  # Skip challengers
                # Unclassified tournament links are skipped

            self._tennis_urls = {"atp": atp_urls, "wta": wta_urls}
            self._tennis_urls_ts = time.time()
            logger.info(
                "DraftKings: Discovered %d ATP, %d WTA tennis tournament URLs",
                len(atp_urls), len(wta_urls),
            )
        except Exception as e:
            logger.warning("DraftKings: Tennis URL discovery failed: %s", e)

    async def _navigate_and_capture(
        self,
        url: str,
        sport_key: str,
        is_player_props: bool = False,
        is_team_props: bool = False,
        prop_category: str = "",
    ) -> List[OddsEvent]:
        """Navigate to a URL and capture the sportsbook-nash API response."""
        await self._ensure_browser()
        if self._page is None:
            return []

        captured = []  # type: List[dict]
        got_data = asyncio.Event()

        async def on_response(response):
            try:
                if response.status != 200:
                    return
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                resp_url = response.url
                if "sportsbook-nash" in resp_url and "sportscontent" in resp_url:
                    body = await response.text()
                    data = json.loads(body)
                    if "events" in data and "markets" in data and "selections" in data:
                        captured.append(data)
                        got_data.set()
            except Exception:
                pass

        self._page.on("response", on_response)
        try:
            await self._page.goto(url, timeout=15000, wait_until="domcontentloaded")
            # Wait for first API response
            try:
                await asyncio.wait_for(got_data.wait(), timeout=4.0)
            except asyncio.TimeoutError:
                pass  # Move on — response may not arrive for empty pages

            # For MMA/boxing, scroll page to trigger lazy-loaded fight cards
            is_combat = "mma" in sport_key or "boxing" in sport_key
            if is_combat and captured:
                # Give time for additional responses after first one
                await asyncio.sleep(1.5)
                # Scroll down to trigger lazy loading of more fights
                try:
                    await self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(2.0)
                    # Scroll a couple more times for pagination
                    for _ in range(3):
                        await self._page.evaluate("window.scrollBy(0, 800)")
                        await asyncio.sleep(0.8)
                except Exception:
                    pass
            elif captured:
                # For other sports, brief delay to catch trailing responses
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.warning("DraftKings: Navigation to %s failed: %s", url, e)
            self._page.remove_listener("response", on_response)
            return []

        self._page.remove_listener("response", on_response)

        if not captured:
            return []

        all_events = []  # type: List[OddsEvent]
        for data in captured:
            events = self._parse_nash_response(
                data, sport_key,
                is_player_props=is_player_props,
                is_team_props=is_team_props,
                prop_category=prop_category,
            )
            all_events.extend(events)

        return all_events

    # ── Parsing ───────────────────────────────────────────────────────

    def _parse_nash_response(
        self,
        data: dict,
        sport_key: str,
        is_player_props: bool = False,
        is_team_props: bool = False,
        prop_category: str = "",
    ) -> List[OddsEvent]:
        """Parse a sportsbook-nash API response."""
        raw_events = data.get("events", [])
        raw_markets = data.get("markets", [])
        raw_selections = data.get("selections", [])

        logger.info(
            "DraftKings nash: %d events, %d markets, %d selections for %s",
            len(raw_events), len(raw_markets), len(raw_selections), sport_key,
        )

        # Debug: log sample IDs and types for MMA
        if ("mma" in sport_key or "boxing" in sport_key) and raw_markets and raw_selections:
            m0 = raw_markets[0]
            s0 = raw_selections[0]
            logger.info(
                "DraftKings MMA debug: market[0] id=%r (type=%s) eventId=%r (type=%s), "
                "selection[0] marketId=%r (type=%s) displayOdds=%r label=%r",
                m0.get("id"), type(m0.get("id")).__name__,
                m0.get("eventId"), type(m0.get("eventId")).__name__,
                s0.get("marketId"), type(s0.get("marketId")).__name__,
                s0.get("displayOdds"), s0.get("label"),
            )

        if not raw_events:
            return []

        # Index markets by eventId
        markets_by_event = {}  # type: Dict[str, List[dict]]
        for m in raw_markets:
            eid = m.get("eventId")
            if eid:
                markets_by_event.setdefault(eid, []).append(m)

        # Index selections by marketId
        sels_by_market = {}  # type: Dict[str, List[dict]]
        for s in raw_selections:
            mid = s.get("marketId")
            if mid:
                sels_by_market.setdefault(mid, []).append(s)

        sport_title = get_sport_title(sport_key)
        events = []  # type: List[OddsEvent]

        for ev in raw_events:
            event_id = ev.get("id")
            event_name = ev.get("name", "")
            commence_time = ev.get("startEventDate", "")
            participants = ev.get("participants", [])

            # Extract team names from participants
            home_team = ""
            away_team = ""
            for p in participants:
                role = (p.get("venueRole") or "").lower()
                name = p.get("name", "")
                if role == "home":
                    home_team = name
                elif role == "away":
                    away_team = name

            # Fallback: parse from event name
            if not home_team or not away_team:
                if " @ " in event_name:
                    parts = event_name.split(" @ ", 1)
                    away_team = away_team or parts[0].strip()
                    home_team = home_team or parts[1].strip()
                elif " vs " in event_name.lower():
                    parts = re.split(
                        r"\s+vs\.?\s+", event_name, maxsplit=1, flags=re.IGNORECASE
                    )
                    away_team = away_team or parts[0].strip()
                    home_team = home_team or (
                        parts[1].strip() if len(parts) > 1 else ""
                    )

            if not home_team or not away_team:
                if "mma" in sport_key or "boxing" in sport_key:
                    logger.warning(
                        "DraftKings MMA: Skipping event '%s' - no teams (participants: %s)",
                        event_name,
                        [(p.get("name", ""), p.get("venueRole", "")) for p in participants],
                    )
                continue

            home_team = resolve_team_name(home_team)
            away_team = resolve_team_name(away_team)

            # Build markets for this event
            event_markets = markets_by_event.get(event_id, [])
            dk_markets = []  # type: List[Market]

            if ("mma" in sport_key or "boxing" in sport_key):
                mkt_names = [(m.get("name", ""), self._classify_market_name((m.get("name") or "").strip())) for m in event_markets[:10]]
                logger.info(
                    "DraftKings MMA event '%s vs %s': id=%r, %d markets found (markets_by_event keys=%s)",
                    away_team, home_team, event_id, len(event_markets),
                    list(markets_by_event.keys())[:5],
                )
                if event_markets:
                    logger.info("  market names: %s", mkt_names)
                    # Debug: check selections for first market
                    m0 = event_markets[0]
                    m0_id = m0.get("id")
                    m0_sels = sels_by_market.get(m0_id, [])
                    logger.info("  market[0] id=%r has %d selections, sels_by_market keys=%s",
                                m0_id, len(m0_sels), list(sels_by_market.keys())[:5])

            for mkt in event_markets:
                market_id = mkt.get("id")
                market_name = (mkt.get("name") or "").strip()
                sels = sels_by_market.get(market_id, [])
                if not sels:
                    continue

                if is_player_props:
                    parsed = self._parse_player_prop_market(
                        market_name, sels, prop_category
                    )
                    if parsed:
                        dk_markets.append(parsed)
                elif is_team_props:
                    parsed = self._parse_team_prop_market(
                        market_name, sels, home_team, away_team
                    )
                    if parsed:
                        dk_markets.append(parsed)
                else:
                    mkt_key = self._classify_market_name(market_name)
                    if not mkt_key:
                        continue
                    parsed = self._parse_selections(
                        mkt_key, sels, home_team, away_team,
                        sport_key=sport_key,
                    )
                    if parsed:
                        dk_markets.append(parsed)

            if not dk_markets:
                if "mma" in sport_key or "boxing" in sport_key:
                    logger.warning(
                        "DraftKings MMA: No classified markets for '%s vs %s' (%d raw markets)",
                        away_team, home_team, len(event_markets),
                    )
                continue

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
                        key="draftkings",
                        title="DraftKings",
                        markets=dk_markets,
                        event_url="https://sportsbook.draftkings.com",
                    )
                ],
            ))

        return events

    def _classify_market_name(self, name: str) -> Optional[str]:
        """Classify a DraftKings market name into a canonical market key."""
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

        if "moneyline" in lower or "money line" in lower:
            return "h2h" + suffix
        elif "spread" in lower or "puck line" in lower or "run line" in lower:
            return "spreads" + suffix
        elif "total" in lower or "over/under" in lower:
            return "totals" + suffix
        elif "fight winner" in lower or "bout winner" in lower:
            return "h2h"
        elif "fight total" in lower or "total rounds" in lower:
            return "totals"
        elif "go the distance" in lower or "goes the distance" in lower:
            return "fight_to_go_distance"

        return None

    def _parse_selections(
        self,
        market_key: str,
        selections: List[dict],
        home_team: str,
        away_team: str,
        sport_key: str = "",
    ) -> Optional[Market]:
        """Parse selections into a Market for game lines."""
        outcomes = []  # type: List[Outcome]

        for sel in selections:
            display_odds = sel.get("displayOdds", {})
            american_str = display_odds.get("american", "")
            price = _parse_dk_american_odds(american_str)
            if price is None:
                if "mma" in sport_key or "boxing" in sport_key:
                    logger.warning(
                        "DraftKings MMA: _parse_dk_american_odds failed for %r (hex=%s)",
                        american_str,
                        american_str.encode('unicode_escape').decode() if american_str else "empty",
                    )
                continue

            outcome_type = (sel.get("outcomeType") or "").lower()
            label = sel.get("label", "")
            points = sel.get("points")

            if market_key.startswith("totals"):
                if outcome_type == "over":
                    name = "Over"
                elif outcome_type == "under":
                    name = "Under"
                else:
                    name = label
            elif market_key == "fight_to_go_distance":
                label_lower = label.lower()
                if "yes" in label_lower:
                    name = "Yes"
                elif "no" in label_lower:
                    name = "No"
                else:
                    name = label
            else:
                if outcome_type == "home":
                    name = home_team
                elif outcome_type == "away":
                    name = away_team
                elif outcome_type == "draw":
                    name = "Draw"
                else:
                    name = label

            # Extract rotation number
            rot_num = None
            participants = sel.get("participants", [])
            if participants:
                rot_str = (
                    participants[0]
                    .get("metadata", {})
                    .get("retailRotNumber")
                )
                if rot_str:
                    try:
                        rot_num = int(rot_str)
                    except (ValueError, TypeError):
                        pass

            outcomes.append(Outcome(
                name=name,
                price=price,
                point=points,
                rotation_number=rot_num,
            ))

        if len(outcomes) < 2:
            return None
        return Market(key=market_key, outcomes=outcomes)

    def _parse_player_prop_market(
        self,
        market_name: str,
        selections: List[dict],
        prop_category: str,
    ) -> Optional[Market]:
        """Parse a player prop market.

        DraftKings uses two formats:
          1. O/U format: selections with outcomeType=Over/Under + points
          2. Tiered milestones: "18+", "20+", "25+" with individual odds

        For tiered milestones, we pick the tier closest to -110 (even odds)
        and convert to an O/U market: Over = that tier's odds,
        point = the milestone value - 0.5 (since "25+" means Over 24.5).
        """
        prop_type_map = {
            "player-points": "player_points",
            "player-rebounds": "player_rebounds",
            "player-assists": "player_assists",
            "player-threes": "player_threes",
            "player-combos": "player_combos",
            "player-defense": "player_defense",
        }
        prop_key = prop_type_map.get(prop_category, "player_prop")

        # Extract player name from market name
        lower = market_name.lower()
        player_name = market_name
        for suffix in [" points", " rebounds", " assists", " threes",
                       " steals", " blocks", " turnovers", " combos",
                       " pts + reb + ast", " pts + reb", " pts + ast",
                       " reb + ast", " defense", " o/u"]:
            if lower.endswith(suffix):
                player_name = market_name[:len(market_name) - len(suffix)].strip()
                break

        # Try O/U format first
        overs = [s for s in selections if (s.get("outcomeType") or "").lower() == "over"]
        unders = [s for s in selections if (s.get("outcomeType") or "").lower() == "under"]

        if overs and unders:
            over_sel = overs[0]
            under_sel = unders[0]
            over_price = _parse_dk_american_odds(
                over_sel.get("displayOdds", {}).get("american", "")
            )
            under_price = _parse_dk_american_odds(
                under_sel.get("displayOdds", {}).get("american", "")
            )
            point = over_sel.get("points")

            if over_price is not None and under_price is not None and point is not None:
                safe_name = player_name.lower().replace(" ", "_").replace(".", "")
                market_key = prop_key + "_" + safe_name
                return Market(
                    key=market_key,
                    outcomes=[
                        Outcome(name=player_name + " Over", price=over_price, point=point),
                        Outcome(name=player_name + " Under", price=under_price, point=point),
                    ],
                )

        # Tiered milestone format: "18+", "20+", "25+" etc.
        # Pick the tier closest to -110 odds as the "main line"
        tiers = []  # type: List[Tuple[float, int]]
        for sel in selections:
            label = sel.get("label", "")
            match = re.match(r"^(\d+)\+$", label.strip())
            if not match:
                continue
            threshold = int(match.group(1))
            price = _parse_dk_american_odds(
                sel.get("displayOdds", {}).get("american", "")
            )
            if price is not None:
                tiers.append((threshold, price))

        if len(tiers) >= 2:
            # Find the tier closest to -110
            tiers.sort(key=lambda t: abs(t[1] - (-110)))
            main_threshold, main_price = tiers[0]
            # Convert "25+" to Over 24.5
            point = main_threshold - 0.5
            safe_name = player_name.lower().replace(" ", "_").replace(".", "")
            market_key = prop_key + "_" + safe_name
            return Market(
                key=market_key,
                outcomes=[
                    Outcome(name=player_name + " Over", price=main_price, point=point),
                ],
            )

        return None

    def _parse_team_prop_market(
        self,
        market_name: str,
        selections: List[dict],
        home_team: str,
        away_team: str,
    ) -> Optional[Market]:
        """Parse a team prop market (Alternate Total Points, Team Totals)."""
        lower = market_name.lower()
        if "total" not in lower:
            return None

        # Detect period suffix from market name
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

        # Determine which team from the market name
        home_lower = home_team.lower()
        away_lower = away_team.lower()
        # Check if any word from the team name appears in market name
        home_words = home_lower.split()
        away_words = away_lower.split()

        is_home = any(w in lower for w in home_words if len(w) > 2)
        is_away = any(w in lower for w in away_words if len(w) > 2)

        if is_home and not is_away:
            team = home_team
            side = "home"
        elif is_away and not is_home:
            team = away_team
            side = "away"
        else:
            return None

        # Find Over/Under pair closest to even odds (main line)
        overs = [s for s in selections if (s.get("outcomeType") or "").lower() == "over"]
        unders = [s for s in selections if (s.get("outcomeType") or "").lower() == "under"]

        if not overs or not unders:
            return None

        # Pick the Over/Under closest to even odds (trueOdds near 2.0)
        best_over = min(overs, key=lambda o: abs(o.get("trueOdds", 0) - 2.0))
        over_point = best_over.get("points")

        # Find matching Under at same point
        best_under = None
        for u in unders:
            if u.get("points") == over_point:
                best_under = u
                break
        if not best_under:
            best_under = min(unders, key=lambda u: abs(u.get("trueOdds", 0) - 2.0))

        over_price = _parse_dk_american_odds(
            best_over.get("displayOdds", {}).get("american", "")
        )
        under_price = _parse_dk_american_odds(
            best_under.get("displayOdds", {}).get("american", "")
        )

        if over_price is None or under_price is None:
            return None

        return Market(
            key="team_total_" + side + suffix,
            outcomes=[
                Outcome(name=team + " Over", price=over_price, point=over_point),
                Outcome(
                    name=team + " Under",
                    price=under_price,
                    point=best_under.get("points"),
                ),
            ],
        )

    # ── Event Merging ─────────────────────────────────────────────────

    def _merge_events(
        self,
        existing: List[OddsEvent],
        new_events: List[OddsEvent],
    ) -> List[OddsEvent]:
        """Merge new events into existing list, combining DK markets."""
        by_id = {}  # type: Dict[str, OddsEvent]
        for ev in existing:
            by_id[ev.id] = ev

        for ev in new_events:
            if ev.id in by_id:
                old_ev = by_id[ev.id]
                old_dk = None
                for bm in old_ev.bookmakers:
                    if bm.key == "draftkings":
                        old_dk = bm
                        break
                if old_dk and ev.bookmakers:
                    new_dk = ev.bookmakers[0]
                    existing_keys = {m.key for m in old_dk.markets}
                    for m in new_dk.markets:
                        if m.key not in existing_keys:
                            old_dk.markets.append(m)
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
