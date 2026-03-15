"""
Hard Rock Bet sportsbook scraper.
Uses Hard Rock's public GraphQL API at api.hardrocksportsbook.com.
No authentication required.  Uses Playwright to obtain Cloudflare
clearance cookies, then passes them to httpx for all subsequent requests.
"""

import asyncio
import logging
import math
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome, PlayerProp
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    decimal_to_american,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://api.hardrocksportsbook.com/java-graphql/graphql"
SITE_URL = "https://www.hardrocksportsbook.com"

# Channel = state-specific locale for the sportsbook.
# Florida has the broadest offering (1,488 events, 22 sports).
CHANNEL = "FLORIDA_ONLINE"
SEGMENT = "fl"

# Cloudflare cookie refresh interval (~25 minutes, cookies last ~30 min)
_CF_COOKIE_TTL = 1500

# ─── Competition IDs per sport ─────────────────────────────────
# These are discovered from the event_tree query at startup.
# If not yet discovered, the scraper will auto-discover.
# Format: sport_key → list of competition IDs
_DEFAULT_COMP_IDS: Dict[str, List[str]] = {}

# ─── Market type codes per sport ────────────────────────────────
# Format: {SPORT}:{PERIOD}:{TYPE}
# FTOT = full game (inc. OT), FHOT = first half, F1QOT-F4QOT = quarters
# FT = full time (reg only), F1POT-F3POT = periods (hockey)
_MARKET_TYPES: Dict[str, List[str]] = {
    "basketball_nba": [
        # Full game
        "BASKETBALL:FTOT:ML", "BASKETBALL:FTOT:SPRD", "BASKETBALL:FTOT:OU",
        "BASKETBALL:FTOT:A:OU", "BASKETBALL:FTOT:B:OU",
        # Period markets (period detected from market name)
        "BASKETBALL:P:DNB", "BASKETBALL:P:OU",
        # Period team totals
        "BASKETBALL:P:A:OU", "BASKETBALL:P:B:OU",
        "BASKETBALL:FT:A:OU", "BASKETBALL:FT:B:OU",
        # Alternate period types (available closer to game time)
        "BASKETBALL:FHOT:ML", "BASKETBALL:FHOT:SPRD", "BASKETBALL:FHOT:OU",
        "BASKETBALL:F1QOT:ML", "BASKETBALL:F1QOT:SPRD", "BASKETBALL:F1QOT:OU",
        "BASKETBALL:F2QOT:ML", "BASKETBALL:F2QOT:SPRD", "BASKETBALL:F2QOT:OU",
        "BASKETBALL:F3QOT:ML", "BASKETBALL:F3QOT:SPRD", "BASKETBALL:F3QOT:OU",
        "BASKETBALL:F4QOT:ML", "BASKETBALL:F4QOT:SPRD", "BASKETBALL:F4QOT:OU",
    ],
    "basketball_ncaab": [
        "BASKETBALL:FTOT:ML", "BASKETBALL:FTOT:SPRD", "BASKETBALL:FTOT:OU",
        "BASKETBALL:FTOT:A:OU", "BASKETBALL:FTOT:B:OU",
        "BASKETBALL:P:DNB", "BASKETBALL:P:OU",
        "BASKETBALL:P:A:OU", "BASKETBALL:P:B:OU",
        "BASKETBALL:FHOT:ML", "BASKETBALL:FHOT:SPRD", "BASKETBALL:FHOT:OU",
    ],
    "americanfootball_nfl": [
        "AMERICAN_FOOTBALL:FTOT:ML", "AMERICAN_FOOTBALL:FTOT:SPRD", "AMERICAN_FOOTBALL:FTOT:OU",
        "AMERICAN_FOOTBALL:P:DNB", "AMERICAN_FOOTBALL:P:OU",
        "AMERICAN_FOOTBALL:P:A:OU", "AMERICAN_FOOTBALL:P:B:OU",
        "AMERICAN_FOOTBALL:FHOT:ML", "AMERICAN_FOOTBALL:FHOT:SPRD", "AMERICAN_FOOTBALL:FHOT:OU",
        "AMERICAN_FOOTBALL:F1QOT:ML", "AMERICAN_FOOTBALL:F1QOT:SPRD", "AMERICAN_FOOTBALL:F1QOT:OU",
        "AMERICAN_FOOTBALL:F2QOT:ML", "AMERICAN_FOOTBALL:F2QOT:SPRD", "AMERICAN_FOOTBALL:F2QOT:OU",
        "AMERICAN_FOOTBALL:F3QOT:ML", "AMERICAN_FOOTBALL:F3QOT:SPRD", "AMERICAN_FOOTBALL:F3QOT:OU",
        "AMERICAN_FOOTBALL:F4QOT:ML", "AMERICAN_FOOTBALL:F4QOT:SPRD", "AMERICAN_FOOTBALL:F4QOT:OU",
    ],
    "americanfootball_ncaaf": [
        "AMERICAN_FOOTBALL:FTOT:ML", "AMERICAN_FOOTBALL:FTOT:SPRD", "AMERICAN_FOOTBALL:FTOT:OU",
        "AMERICAN_FOOTBALL:P:DNB", "AMERICAN_FOOTBALL:P:OU",
        "AMERICAN_FOOTBALL:FHOT:ML", "AMERICAN_FOOTBALL:FHOT:SPRD", "AMERICAN_FOOTBALL:FHOT:OU",
    ],
    "icehockey_nhl": [
        "ICE_HOCKEY:FTOT:ML", "ICE_HOCKEY:FTOT:SPRD", "ICE_HOCKEY:FTOT:OU",
        "ICE_HOCKEY:FTOT:A:OU", "ICE_HOCKEY:FTOT:B:OU",
        "ICE_HOCKEY:P:AXB", "ICE_HOCKEY:P:DNB", "ICE_HOCKEY:P:OU", "ICE_HOCKEY:P:SPRD",
        "ICE_HOCKEY:P:A:OU", "ICE_HOCKEY:P:B:OU",
        "ICE_HOCKEY:F1POT:ML", "ICE_HOCKEY:F1POT:SPRD", "ICE_HOCKEY:F1POT:OU",
        "ICE_HOCKEY:F2POT:ML", "ICE_HOCKEY:F2POT:SPRD", "ICE_HOCKEY:F2POT:OU",
        "ICE_HOCKEY:F3POT:ML", "ICE_HOCKEY:F3POT:SPRD", "ICE_HOCKEY:F3POT:OU",
    ],
    "baseball_mlb": [
        "BASEBALL:FTEI:ML", "BASEBALL:FTEI:SPRD", "BASEBALL:FTEI:OU",
        "BASEBALL:FT:ML", "BASEBALL:FT:SPRD", "BASEBALL:FT:OU",
        "BASEBALL:FT:AHCP", "BASEBALL:FT:A:OU", "BASEBALL:FT:B:OU",
        "BASEBALL:P:DNB", "BASEBALL:P:OU",
        "BASEBALL:P:A:OU", "BASEBALL:P:B:OU",
        "BASEBALL:FHOT:ML", "BASEBALL:FHOT:SPRD", "BASEBALL:FHOT:OU",
        "BASEBALL:FIOT:ML", "BASEBALL:FIOT:SPRD", "BASEBALL:FIOT:OU",
    ],
    "mma_mixed_martial_arts": [],   # Empty = fetch all market types (catches GTD, etc.)
    "boxing_boxing": [],            # Empty = fetch all market types

    "soccer_epl": [
        "SOCCER:FT:AXB", "SOCCER:FT:AHCP", "SOCCER:FT:OU",
        "SOCCER:FT:A:OU", "SOCCER:FT:B:OU",
        "SOCCER:FT:DNB", "SOCCER:FT:BTTS", "SOCCER:FT:DC",
        "SOCCER:P:AXB", "SOCCER:P:DNB", "SOCCER:P:OU",
        "SOCCER:P:A:OU", "SOCCER:P:B:OU",
        "SOCCER:FH:AXB", "SOCCER:FH:OU", "SOCCER:SH:AXB", "SOCCER:SH:OU",
    ],
    "soccer_spain_la_liga": [
        "SOCCER:FT:AXB", "SOCCER:FT:AHCP", "SOCCER:FT:OU",
        "SOCCER:FT:A:OU", "SOCCER:FT:B:OU",
        "SOCCER:FT:DNB", "SOCCER:FT:BTTS", "SOCCER:FT:DC",
        "SOCCER:P:AXB", "SOCCER:P:DNB", "SOCCER:P:OU",
        "SOCCER:P:A:OU", "SOCCER:P:B:OU",
        "SOCCER:FH:AXB", "SOCCER:FH:OU", "SOCCER:SH:AXB", "SOCCER:SH:OU",
    ],
    "soccer_germany_bundesliga": [
        "SOCCER:FT:AXB", "SOCCER:FT:AHCP", "SOCCER:FT:OU",
        "SOCCER:FT:A:OU", "SOCCER:FT:B:OU",
        "SOCCER:FT:DNB", "SOCCER:FT:BTTS", "SOCCER:FT:DC",
        "SOCCER:P:AXB", "SOCCER:P:DNB", "SOCCER:P:OU",
        "SOCCER:P:A:OU", "SOCCER:P:B:OU",
        "SOCCER:FH:AXB", "SOCCER:FH:OU", "SOCCER:SH:AXB", "SOCCER:SH:OU",
    ],
    "soccer_italy_serie_a": [
        "SOCCER:FT:AXB", "SOCCER:FT:AHCP", "SOCCER:FT:OU",
        "SOCCER:FT:A:OU", "SOCCER:FT:B:OU",
        "SOCCER:FT:DNB", "SOCCER:FT:BTTS", "SOCCER:FT:DC",
        "SOCCER:P:AXB", "SOCCER:P:DNB", "SOCCER:P:OU",
        "SOCCER:P:A:OU", "SOCCER:P:B:OU",
        "SOCCER:FH:AXB", "SOCCER:FH:OU", "SOCCER:SH:AXB", "SOCCER:SH:OU",
    ],
    "soccer_france_ligue_one": [
        "SOCCER:FT:AXB", "SOCCER:FT:AHCP", "SOCCER:FT:OU",
        "SOCCER:FT:A:OU", "SOCCER:FT:B:OU",
        "SOCCER:FT:DNB", "SOCCER:FT:BTTS", "SOCCER:FT:DC",
        "SOCCER:P:AXB", "SOCCER:P:DNB", "SOCCER:P:OU",
        "SOCCER:P:A:OU", "SOCCER:P:B:OU",
        "SOCCER:FH:AXB", "SOCCER:FH:OU", "SOCCER:SH:AXB", "SOCCER:SH:OU",
    ],
    "soccer_usa_mls": [
        "SOCCER:FT:AXB", "SOCCER:FT:AHCP", "SOCCER:FT:OU",
        "SOCCER:FT:A:OU", "SOCCER:FT:B:OU",
        "SOCCER:FT:DNB", "SOCCER:FT:BTTS", "SOCCER:FT:DC",
        "SOCCER:P:AXB", "SOCCER:P:DNB", "SOCCER:P:OU",
        "SOCCER:P:A:OU", "SOCCER:P:B:OU",
        "SOCCER:FH:AXB", "SOCCER:FH:OU", "SOCCER:SH:AXB", "SOCCER:SH:OU",
    ],
    "tennis_atp": [
        "TENNIS:FT:ML", "TENNIS:FT:OU",
        "TENNIS:F1SOT:ML",
    ],
    "tennis_wta": [
        "TENNIS:FT:ML", "TENNIS:FT:OU",
        "TENNIS:F1SOT:ML",
    ],
}

# Map sport_key → Hard Rock sport code for event_tree lookup
_SPORT_CODE_MAP: Dict[str, str] = {
    "basketball_nba": "BASKETBALL",
    "basketball_ncaab": "BASKETBALL",
    "americanfootball_nfl": "AMERICAN_FOOTBALL",
    "americanfootball_ncaaf": "AMERICAN_FOOTBALL",
    "icehockey_nhl": "ICE_HOCKEY",
    "baseball_mlb": "BASEBALL",
    "mma_mixed_martial_arts": "MMA",
    "boxing_boxing": "BOXING",
    "soccer_epl": "SOCCER",
    "soccer_spain_la_liga": "SOCCER",
    "soccer_germany_bundesliga": "SOCCER",
    "soccer_italy_serie_a": "SOCCER",
    "soccer_france_ligue_one": "SOCCER",
    "soccer_usa_mls": "SOCCER",
    "tennis_atp": "TENNIS",
    "tennis_wta": "TENNIS",
}

# Map sport_key → expected competition name patterns for matching
_COMP_NAME_MAP: Dict[str, List[str]] = {
    "basketball_nba": ["NBA"],
    "basketball_ncaab": ["NCAAB", "NCAA"],
    "americanfootball_nfl": ["NFL"],
    "americanfootball_ncaaf": ["NCAAF", "NCAA Football", "College Football"],
    "icehockey_nhl": ["NHL"],
    "baseball_mlb": ["MLB"],
    "mma_mixed_martial_arts": ["UFC"],
    "boxing_boxing": ["Upcoming Fights", "Boxing"],
    "soccer_epl": ["England Premier League", "EPL", "Premier League"],
    "soccer_spain_la_liga": ["Spain La Liga", "La Liga"],
    "soccer_germany_bundesliga": ["Germany Bundesliga", "Bundesliga"],
    "soccer_italy_serie_a": ["Italy Serie A", "Serie A"],
    "soccer_france_ligue_one": ["France Ligue 1", "Ligue 1"],
    "soccer_usa_mls": ["MLS"],
    "tennis_atp": ["ATP"],
    "tennis_wta": ["WTA"],
}

# ─── Market type code → (market_key, period_suffix) mapping ────
# Parsed at module load from the type code format: {SPORT}:{PERIOD}:{TYPE}
_PERIOD_MAP = {
    "FTOT": "",       # Full game (inc. OT) - basketball, hockey
    "FT": "",         # Full time (regulation only) - soccer, mma, tennis
    "FTEI": "",       # Full time extra innings - baseball
    "FHOT": "_h1",    # First half (OT included)
    "FH": "_h1",      # First half - soccer
    "SHOT": "_h2",    # Second half
    "SH": "_h2",      # Second half - soccer
    "F1QOT": "_q1",   # 1st quarter
    "F2QOT": "_q2",   # 2nd quarter
    "F3QOT": "_q3",   # 3rd quarter
    "F4QOT": "_q4",   # 4th quarter
    "F1POT": "_p1",   # 1st period (hockey)
    "F2POT": "_p2",   # 2nd period
    "F3POT": "_p3",   # 3rd period
    "F1SOT": "_s1",   # 1st set (tennis)
    "F2SOT": "_s2",   # 2nd set
    "F3SOT": "_s3",   # 3rd set
    "FIOT": "_f5",    # First innings OT (baseball first 5 innings)
}

_TYPE_MAP = {
    "ML": "h2h",
    "AXB": "h2h_3way", # 3-way result (A x B = home/draw/away) → 3-way moneyline
    "DNB": "draw_no_bet",  # Draw No Bet → own market key
    "AHCP": "spreads", # Asian Handicap → spreads
    "SPRD": "spreads",
    "OU": "totals",
    "BTTS": "btts",    # Both Teams to Score (Yes/No)
    "DC": "double_chance",  # Double Chance (Home/Draw, Away/Draw, Home/Away)
}

# ─── Period detection from market NAME for P: type codes ──────
# When the type code contains ":P:", the period is in the market name.
_PERIOD_NAME_PATTERNS = [
    (re.compile(r"1st\s+quarter", re.I), "_q1"),
    (re.compile(r"2nd\s+quarter", re.I), "_q2"),
    (re.compile(r"3rd\s+quarter", re.I), "_q3"),
    (re.compile(r"4th\s+quarter", re.I), "_q4"),
    (re.compile(r"1st\s+half", re.I), "_h1"),
    (re.compile(r"2nd\s+half", re.I), "_h2"),
    (re.compile(r"1st\s+period", re.I), "_p1"),
    (re.compile(r"2nd\s+period", re.I), "_p2"),
    (re.compile(r"3rd\s+period", re.I), "_p3"),
    (re.compile(r"1st\s+set", re.I), "_s1"),
    (re.compile(r"2nd\s+set", re.I), "_s2"),
    (re.compile(r"3rd\s+set", re.I), "_s3"),
    (re.compile(r"first\s+5\s+innings", re.I), "_f5"),
    (re.compile(r"1st\s+5\s+innings", re.I), "_f5"),
]


def _detect_period_from_name(market_name: str) -> str:
    """Detect period suffix from market name for P: type markets."""
    for pattern, suffix in _PERIOD_NAME_PATTERNS:
        if pattern.search(market_name):
            return suffix
    return ""

# GraphQL fragments
_EVENT_TREE_QUERY = """
query betSync(
    $channel: String
    $segment: String
    $region: String
    $language: String
    $nonTradingFilters: [NodeFilterType]
) {
    betSync(channel: $channel, cmsSegment: $segment, region: $region, language: $language) {
        sports(filters: $nonTradingFilters) {
            code
            categories(filters: $nonTradingFilters) {
                competitions(filters: $nonTradingFilters) {
                    id
                    name
                    numEvents
                }
            }
        }
    }
}
"""

_EVENTS_QUERY = """
query betSync(
    $filters: [Filter]
    $segment: String
    $region: String
    $language: String
    $channel: String
    $marketTypes: [String]
    $sort: Sort
    $slice: Interval
) {
    betSync(cmsSegment: $segment, region: $region, language: $language, channel: $channel) {
        events(filters: $filters, sort: $sort, slice: $slice) {
            data {
                id
                name
                eventTime
                sport
                inplay
                compId
                compName
                rotationCodeA
                rotationCodeB
                participants {
                    id
                    name
                }
                markets(keyMarkets: false, marketTypes: $marketTypes) {
                    id
                    name
                    type
                    line
                    spread
                    suspended
                    period
                    overUnder
                    playerProp
                    player
                    team
                    selection {
                        id
                        name
                        odds
                        type
                        suspended
                        rotationCode
                    }
                }
            }
            count
        }
    }
}
"""

_PLAYER_PROPS_QUERY = """
query betSync(
    $filters: [Filter]
    $segment: String
    $region: String
    $language: String
    $channel: String
    $marketTypes: [String]
) {
    betSync(cmsSegment: $segment, region: $region, language: $language, channel: $channel) {
        events(filters: $filters) {
            data {
                id
                name
                markets(keyMarkets: false) {
                    id
                    name
                    type
                    line
                    suspended
                    playerProp
                    player
                    playerId
                    team
                    overUnder
                    selection {
                        id
                        name
                        odds
                        type
                        suspended
                    }
                }
            }
            count
        }
    }
}
"""


def _parse_market_type_code(
    type_code: str, market_name: str = ""
) -> Optional[Tuple[str, str, bool]]:
    """
    Parse a market type code like 'BASKETBALL:FTOT:ML' into (base_key, suffix, is_team_total).
    Returns ('h2h', '', False) for full game moneyline.
    Returns ('totals', '_q1', False) for Q1 total.
    Returns ('totals', '_h1', True) for 1st half team total.
    Returns None if not a recognized market type.

    For P: type codes (e.g., 'BASKETBALL:P:DNB'), the period is detected
    from the market_name.  P:A:OU / P:B:OU are team totals.
    """
    parts = type_code.split(":")
    if len(parts) < 3:
        return None

    market_type = parts[-1]  # e.g., ML, SPRD, OU, AXB, DNB, AHCP
    base = _TYPE_MAP.get(market_type)
    if base is None:
        return None

    is_team_total = False

    # Check for P: period-in-name type codes
    # Patterns: SPORT:P:TYPE, SPORT:P:A:TYPE, SPORT:P:B:TYPE
    if ":P:" in type_code:
        suffix = _detect_period_from_name(market_name)
        # A: and B: indicate team totals
        if len(parts) >= 4 and parts[-2] in ("A", "B"):
            is_team_total = True
        return (base, suffix, is_team_total)

    # Check for FT:A:OU, FT:B:OU (full game team totals)
    if len(parts) >= 4 and parts[-2] in ("A", "B"):
        period_code = parts[-3]
        suffix = _PERIOD_MAP.get(period_code, "")
        is_team_total = True
        return (base, suffix, is_team_total)

    # Standard: SPORT:PERIOD:TYPE
    period_code = parts[-2]
    suffix = _PERIOD_MAP.get(period_code)
    if suffix is None:
        return None

    return (base, suffix, is_team_total)


def _decimal_to_american_odds(decimal_odds: float) -> int:
    """Convert decimal odds to American odds."""
    return decimal_to_american(decimal_odds)


def _extract_line_from_selection_name(name: str) -> Optional[float]:
    """
    Extract the numeric line from a selection name.
    E.g. "Over 228.5" → 228.5, "Grizzlies +3.5" → 3.5, "Warriors -3.5" → -3.5
    """
    m = re.search(r"([+-]?\d+\.?\d*)\s*$", name)
    if m:
        return float(m.group(1))
    return None


class HardRockBetSource(DataSource):
    """Fetches odds from Hard Rock Bet via their public GraphQL API.

    Tries direct HTTP access first (no Cloudflare bypass needed).
    Falls back to Playwright-based Cloudflare cookie acquisition if direct fails.
    """

    _HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"{SITE_URL}/",
        "Origin": SITE_URL,
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
    }

    def __init__(self):
        self._cf_cookies: Dict[str, str] = {}
        self._cf_cookies_expires: float = 0
        self._client: Optional[httpx.AsyncClient] = None
        self._init_done = False
        self._cf_lock = asyncio.Lock()
        self._direct_mode = True  # Try direct HTTP first (no Playwright)
        self._direct_failed_count = 0
        # Discovered competition IDs: sport_key → [comp_id, ...]
        self._comp_ids: Dict[str, List[str]] = {}
        self._tree_fetched = False
        # Cache for event IDs: canonical_event_id → hr_event_id
        self._event_ids: Dict[str, str] = {}

    def _create_client(self) -> httpx.AsyncClient:
        """Create an httpx client with current CF cookies (or none for direct mode)."""
        return httpx.AsyncClient(
            timeout=20.0,
            headers=self._HEADERS,
            cookies=self._cf_cookies if self._cf_cookies else {},
            follow_redirects=True,
        )

    async def _ensure_client(self) -> bool:
        """Ensure we have an httpx client ready. In direct mode, no CF cookies needed."""
        if self._direct_mode:
            if self._client is None:
                self._client = self._create_client()
                self._init_done = True
                logger.info("HardRock: using direct HTTP mode (no Cloudflare bypass)")
            return True
        # Fallback: use CF cookie mode
        return await self._ensure_cf_cookies()

    async def _ensure_cf_cookies(self) -> bool:
        """Ensure we have valid Cloudflare cookies, refreshing if needed."""
        if self._cf_cookies and time.time() < self._cf_cookies_expires:
            return True
        async with self._cf_lock:
            # Double-check after acquiring lock (another coroutine may have refreshed)
            if self._cf_cookies and time.time() < self._cf_cookies_expires:
                return True
            return await self._get_cloudflare_cookies()

    async def _get_cloudflare_cookies(self) -> bool:
        """Get Cloudflare clearance cookies — try cloudscraper first, then Playwright."""
        # --- Attempt 1: cloudscraper (lightweight, no browser needed) ---
        try:
            import cloudscraper
            scraper = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "darwin", "desktop": True}
            )
            resp = scraper.get(SITE_URL + "/", timeout=20)
            if resp.status_code == 200:
                self._cf_cookies = dict(scraper.cookies)
                self._cf_cookies_expires = time.time() + _CF_COOKIE_TTL
                cf_names = list(self._cf_cookies.keys())
                logger.info("HardRock: cloudscraper got %d cookies: %s", len(self._cf_cookies), cf_names[:10])
                # Also hit the API domain to pick up its cookies
                try:
                    resp2 = scraper.get("https://api.hardrocksportsbook.com/java-graphql/graphql?type=event_tree", timeout=15)
                    extra = dict(scraper.cookies)
                    self._cf_cookies.update(extra)
                except Exception:
                    pass
                if self._client:
                    await self._client.aclose()
                self._client = self._create_client()
                self._init_done = True
                return bool(self._cf_cookies)
            else:
                logger.info("HardRock: cloudscraper returned %d, trying Playwright", resp.status_code)
        except ImportError:
            logger.info("HardRock: cloudscraper not installed, trying Playwright")
        except Exception as e:
            logger.info("HardRock: cloudscraper failed (%s), trying Playwright", e)

        # --- Attempt 2: Playwright CF bypass ---
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("HardRock: playwright not installed, cannot bypass Cloudflare")
            return False

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                    ],
                )
                context = await browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                    ),
                )
                page = await context.new_page()

                logger.info("HardRock: launching browser for Cloudflare clearance...")

                # Visit the main site first to establish cookies
                await page.goto(SITE_URL + "/", wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(8000)

                # Also visit the API domain to get cookies for it
                try:
                    await page.goto(
                        "https://api.hardrocksportsbook.com/java-graphql/graphql?type=event_tree",
                        wait_until="networkidle",
                        timeout=15000,
                    )
                    await page.wait_for_timeout(5000)
                except Exception:
                    pass  # API may return error page, that's fine — we just want cookies

                # Extract ALL cookies from the browser context
                cookies = await context.cookies()
                self._cf_cookies = {}
                for c in cookies:
                    domain = c.get("domain", "")
                    # Collect cookies from both the site and API domains
                    if "hardrock" in domain:
                        self._cf_cookies[c["name"]] = c["value"]

                self._cf_cookies_expires = time.time() + _CF_COOKIE_TTL

                await browser.close()

            cf_names = list(self._cf_cookies.keys())
            logger.info(
                "HardRock: got %d cookies: %s",
                len(self._cf_cookies),
                cf_names[:10],
            )

            # Create/recreate httpx client with new cookies
            if self._client:
                await self._client.aclose()
            self._client = self._create_client()
            self._init_done = True
            return bool(self._cf_cookies)

        except Exception as e:
            logger.warning("HardRock: Cloudflare bypass failed: %s", e)
            return False

    async def _fetch_event_tree(self) -> None:
        """Fetch the sport tree to discover competition IDs."""
        if self._tree_fetched:
            return

        # Ensure client is ready (direct or CF mode)
        if not await self._ensure_client():
            logger.warning("HardRock: cannot fetch event tree without client")
            return

        for attempt in range(3):
            try:
                resp = await self._client.post(
                    f"{GRAPHQL_URL}?type=event_tree",
                    json={
                        "query": _EVENT_TREE_QUERY,
                        "variables": {
                            "channel": CHANNEL,
                            "segment": SEGMENT,
                            "region": "us",
                            "language": "enus",
                            "nonTradingFilters": ["DISPLAYED"],
                        },
                    },
                )
                if resp.status_code == 403:
                    if self._direct_mode and attempt == 0:
                        logger.info("HardRock: event_tree direct mode got 403, switching to Playwright CF...")
                        self._direct_mode = False
                        self._cf_cookies_expires = 0
                        if await self._ensure_cf_cookies():
                            continue
                        return
                    elif attempt < 2:
                        logger.info("HardRock: event_tree got 403, refreshing CF cookies...")
                        self._cf_cookies_expires = 0
                        if await self._ensure_cf_cookies():
                            continue
                    return
                resp.raise_for_status()
                data = resp.json()
                break
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 403 and attempt == 0:
                    logger.info("HardRock: event_tree got 403, refreshing CF cookies...")
                    self._cf_cookies_expires = 0
                    if await self._ensure_cf_cookies():
                        continue
                logger.warning(f"HardRock: event_tree fetch failed: {e}")
                return
            except Exception as e:
                logger.warning(f"HardRock: event_tree fetch failed: {e}")
                return
        else:
            return

        sports = data.get("data", {}).get("betSync", {}).get("sports", [])
        for sport in sports:
            sport_code = sport.get("code", "")
            for cat in sport.get("categories", []):
                for comp in cat.get("competitions", []):
                    comp_name = comp.get("name", "")
                    comp_id = comp.get("id", "")
                    num_events = comp.get("numEvents", 0)
                    if not comp_id or num_events == 0:
                        continue

                    # Match competition to sport_key
                    for sport_key, patterns in _COMP_NAME_MAP.items():
                        if _SPORT_CODE_MAP.get(sport_key) != sport_code:
                            continue
                        for pattern in patterns:
                            if pattern.lower() in comp_name.lower() or comp_name.lower() in pattern.lower():
                                if sport_key not in self._comp_ids:
                                    self._comp_ids[sport_key] = []
                                if comp_id not in self._comp_ids[sport_key]:
                                    self._comp_ids[sport_key].append(comp_id)
                                    logger.info(f"HardRock: {sport_key} → comp '{comp_name}' (id={comp_id}, {num_events} events)")
                                break

        # Tennis: match all ATP/WTA tournaments
        for sport in sports:
            if sport.get("code") != "TENNIS":
                continue
            for cat in sport.get("categories", []):
                for comp in cat.get("competitions", []):
                    comp_name = comp.get("name", "")
                    comp_id = comp.get("id", "")
                    num_events = comp.get("numEvents", 0)
                    if not comp_id or num_events == 0:
                        continue
                    name_lower = comp_name.lower()
                    if "atp" in name_lower or "challenger" in name_lower:
                        if "tennis_atp" not in self._comp_ids:
                            self._comp_ids["tennis_atp"] = []
                        if comp_id not in self._comp_ids["tennis_atp"]:
                            self._comp_ids["tennis_atp"].append(comp_id)
                    elif "wta" in name_lower:
                        if "tennis_wta" not in self._comp_ids:
                            self._comp_ids["tennis_wta"] = []
                        if comp_id not in self._comp_ids["tennis_wta"]:
                            self._comp_ids["tennis_wta"].append(comp_id)

        self._tree_fetched = True
        logger.info(f"HardRock: discovered {sum(len(v) for v in self._comp_ids.values())} competitions across {len(self._comp_ids)} sports")

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        if bookmakers and "hardrock" not in bookmakers:
            return [], {"x-requests-remaining": "unlimited"}

        # Ensure client is ready (direct mode or CF cookie mode)
        if not await self._ensure_client():
            logger.warning("HardRock: skipping %s — no client available", sport_key)
            return [], {"x-requests-remaining": "unlimited"}

        # Discover competitions if not yet done
        await self._fetch_event_tree()

        comp_ids = self._comp_ids.get(sport_key, [])
        if not comp_ids:
            return [], {"x-requests-remaining": "unlimited"}

        market_types = _MARKET_TYPES.get(sport_key, [])

        try:
            # Fetch events for all competitions in this sport
            all_events = []

            # Batch competitions together (API supports multiple compId values)
            batch_size = 10
            for i in range(0, len(comp_ids), batch_size):
                batch = comp_ids[i : i + batch_size]
                events = await self._fetch_competition_events(batch, market_types)
                all_events.extend(events)

            # Parse events
            parsed = []
            sport_title = get_sport_title(sport_key)
            for event_data in all_events:
                event = self._parse_event(event_data, sport_key, sport_title)
                if event:
                    parsed.append(event)

            logger.info(f"HardRock: {len(parsed)} events for {sport_key}")
            return parsed, {"x-requests-remaining": "unlimited"}

        except Exception as e:
            logger.warning(f"HardRock failed for {sport_key}: {e}")
            return [], {"x-requests-remaining": "unlimited"}

    async def _fetch_competition_events(
        self, comp_ids: List[str], market_types: List[str]
    ) -> List[dict]:
        """Fetch events for a list of competition IDs."""
        comp_filter = comp_ids[0] if len(comp_ids) == 1 else comp_ids

        variables = {
            "channel": CHANNEL,
            "segment": SEGMENT,
            "region": "us",
            "language": "enus",
            "filters": [
                {"field": "compId", "values": comp_filter},
                {"field": "outright", "value": "false"},
            ],
            "sort": {"field": "compEventWeightingV2", "descending": True},
            "slice": {"from": 0, "to": 200},
        }

        if market_types:
            variables["marketTypes"] = market_types

        for attempt in range(3):
            try:
                resp = await self._client.post(
                    GRAPHQL_URL,
                    json={"query": _EVENTS_QUERY, "variables": variables},
                )
                if resp.status_code == 403:
                    if self._direct_mode and attempt == 0:
                        # Direct mode failed — switch to Playwright CF mode
                        logger.info("HardRock: direct mode got 403, switching to Playwright CF mode...")
                        self._direct_mode = False
                        self._direct_failed_count += 1
                        self._cf_cookies_expires = 0
                        if await self._ensure_cf_cookies():
                            continue
                        return []
                    elif attempt < 2:
                        logger.info("HardRock: got 403, refreshing CF cookies...")
                        self._cf_cookies_expires = 0
                        if await self._ensure_cf_cookies():
                            continue
                    return []
                resp.raise_for_status()
                data = resp.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 403:
                    if self._direct_mode and attempt == 0:
                        logger.info("HardRock: direct mode got 403, switching to Playwright CF mode...")
                        self._direct_mode = False
                        self._cf_cookies_expires = 0
                        if await self._ensure_cf_cookies():
                            continue
                    elif attempt < 2:
                        logger.info("HardRock: got 403, refreshing CF cookies...")
                        self._cf_cookies_expires = 0
                        if await self._ensure_cf_cookies():
                            continue
                logger.warning(f"HardRock: events fetch failed for comps {comp_ids}: {e}")
                return []
            except Exception as e:
                logger.warning(f"HardRock: events fetch failed for comps {comp_ids}: {e}")
                return []

            if "errors" in data:
                logger.warning(f"HardRock GraphQL errors: {data['errors'][:2]}")
                return []

            events = data.get("data", {}).get("betSync", {}).get("events", {}).get("data", [])
            return events

        return []

    def _parse_event(
        self, event_data: dict, sport_key: str, sport_title: str
    ) -> Optional[OddsEvent]:
        """Parse a single event from the GraphQL response."""
        name = event_data.get("name", "")
        participants = event_data.get("participants", [])

        if len(participants) < 2:
            # Try parsing from name: "Team A vs. Team B" or "Team A vs Team B"
            m = re.match(r"(.+?)\s+vs\.?\s+(.+)", name, re.IGNORECASE)
            if m:
                away_name = resolve_team_name(m.group(1).strip())
                home_name = resolve_team_name(m.group(2).strip())
            else:
                return None
        else:
            # First participant = away (visitor), second = home
            away_name = resolve_team_name(participants[0].get("name", ""))
            home_name = resolve_team_name(participants[1].get("name", ""))

        if not home_name or not away_name:
            return None

        # Convert event time from epoch milliseconds to ISO 8601
        event_time_ms = event_data.get("eventTime")
        if event_time_ms:
            dt = datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc)
            commence_time = dt.isoformat()
        else:
            return None

        # Get rotation codes from event level
        rot_a = event_data.get("rotationCodeA")
        rot_b = event_data.get("rotationCodeB")

        # Parse markets
        raw_markets = event_data.get("markets", [])
        hr_markets = self._parse_markets(raw_markets, home_name, away_name)

        if not hr_markets:
            return None

        # Build canonical ID
        cid = canonical_event_id(sport_key, home_name, away_name, commence_time)

        # Cache event ID for player props
        self._event_ids[cid] = event_data.get("id", "")

        # Apply rotation codes to first outcomes
        for mkt in hr_markets:
            for outcome in mkt.outcomes:
                o_name = outcome.name.lower()
                if rot_a and (o_name == away_name.lower() or "away" in o_name):
                    outcome.rotation_number = rot_a
                elif rot_b and (o_name == home_name.lower() or "home" in o_name):
                    outcome.rotation_number = rot_b

        event_url = f"https://app.hardrock.bet/sports/{event_data.get('sport', '').lower()}"

        return OddsEvent(
            id=cid,
            sport_key=sport_key,
            sport_title=sport_title,
            commence_time=commence_time,
            home_team=home_name,
            away_team=away_name,
            bookmakers=[
                Bookmaker(
                    key="hardrock",
                    title="Hard Rock Bet",
                    markets=hr_markets,
                    event_url=event_url,
                )
            ],
        )

    def _parse_markets(
        self, raw_markets: list, home_name: str, away_name: str
    ) -> List[Market]:
        """Parse all markets for an event, picking the main line for each type."""
        # Group markets by (market_key, is_team_total, team_side)
        # team_side: "A" (away), "B" (home), or "" (not team-specific)
        GroupKey = Tuple[str, bool, str]
        grouped: Dict[GroupKey, List[dict]] = {}

        gtd_market = None  # type: Optional[dict]
        for mkt in raw_markets:
            if mkt.get("suspended"):
                continue

            type_code = mkt.get("type", "")
            mkt_name = mkt.get("name", "")

            # MMA/Boxing: detect GTD by name before type code parsing
            mkt_name_lower = mkt_name.lower()
            if ("go the distance" in mkt_name_lower or "goes the distance" in mkt_name_lower) and not gtd_market:
                gtd_market = mkt
                continue

            parsed = _parse_market_type_code(type_code, mkt_name)
            if not parsed:
                continue

            base, suffix, is_team_total = parsed
            market_key = base + suffix

            # Determine team side for team totals
            parts = type_code.split(":")
            team_side = ""
            if is_team_total:
                for p in parts:
                    if p in ("A", "B"):
                        team_side = p
                        break

            key = (market_key, is_team_total, team_side)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(mkt)

        # For each market key, pick the "main" line
        result = []
        for (market_key, is_team_total, team_side), mkts in grouped.items():
            base = market_key.split("_")[0] if "_" in market_key else market_key
            # Handle compound base keys like h2h_3way, draw_no_bet, double_chance
            if market_key.startswith("h2h_3way"):
                base = "h2h_3way"
            elif market_key.startswith("draw_no_bet"):
                base = "draw_no_bet"
            elif market_key.startswith("double_chance"):
                base = "double_chance"
            elif market_key.startswith("btts"):
                base = "btts"

            if is_team_total and base == "totals":
                # Team totals: pick main line, label with team name
                best = self._pick_main_line(mkts)
                if best:
                    side_label = "away" if team_side == "A" else "home"
                    team_label = away_name if team_side == "A" else home_name
                    parsed_outcomes = self._parse_team_total(best, team_label)
                    if parsed_outcomes:
                        suffix = market_key.replace("totals", "")
                        tt_key = f"team_total_{side_label}" + suffix
                        result.append(Market(key=tt_key, outcomes=parsed_outcomes))
            elif base == "h2h":
                # 2-way Moneyline: should only be one, take the first
                parsed_outcomes = self._parse_moneyline(mkts[0], home_name, away_name)
                if parsed_outcomes:
                    result.append(Market(key=market_key, outcomes=parsed_outcomes))
            elif base == "h2h_3way":
                # 3-way moneyline (home/draw/away): parse all 3 selections
                parsed_outcomes = self._parse_moneyline(mkts[0], home_name, away_name)
                if parsed_outcomes:
                    result.append(Market(key=market_key, outcomes=parsed_outcomes))
            elif base == "draw_no_bet":
                # Draw No Bet: 2-way (home/away, no draw)
                parsed_outcomes = self._parse_moneyline(mkts[0], home_name, away_name)
                if parsed_outcomes:
                    result.append(Market(key=market_key, outcomes=parsed_outcomes))
            elif base == "btts":
                # Both Teams to Score: Yes/No
                parsed_outcomes = self._parse_yes_no(mkts[0])
                if parsed_outcomes:
                    result.append(Market(key=market_key, outcomes=parsed_outcomes))
            elif base == "double_chance":
                # Double Chance: 3 outcomes (Home/Draw, Away/Draw, Home/Away)
                parsed_outcomes = self._parse_moneyline(mkts[0], home_name, away_name)
                if parsed_outcomes:
                    result.append(Market(key=market_key, outcomes=parsed_outcomes))
            elif base == "spreads":
                # Spread: pick the line closest to -110/-110 (1.909 decimal)
                best = self._pick_main_line(mkts)
                if best:
                    parsed_outcomes = self._parse_spread(best, home_name, away_name)
                    if parsed_outcomes:
                        result.append(Market(key=market_key, outcomes=parsed_outcomes))
            elif base == "totals":
                # Totals: pick the line closest to -110/-110
                best = self._pick_main_line(mkts)
                if best:
                    parsed_outcomes = self._parse_total(best)
                    if parsed_outcomes:
                        result.append(Market(key=market_key, outcomes=parsed_outcomes))

        # MMA/Boxing: add GTD market if detected by name
        if gtd_market:
            gtd_outcomes = self._parse_yes_no(gtd_market)
            if gtd_outcomes:
                result.append(Market(key="fight_to_go_distance", outcomes=gtd_outcomes))

        return result

    def _pick_main_line(self, markets: List[dict]) -> Optional[dict]:
        """Pick the market with odds closest to -110/-110 (1.909 decimal)."""
        target = 1.909  # -110 in decimal
        best = None
        best_score = float("inf")

        for mkt in markets:
            sels = mkt.get("selection", [])
            if len(sels) < 2:
                continue

            # Average deviation from target odds
            total_dev = 0
            valid = True
            for sel in sels:
                odds_str = sel.get("odds")
                if not odds_str:
                    valid = False
                    break
                try:
                    odds = float(odds_str)
                except (ValueError, TypeError):
                    valid = False
                    break
                total_dev += abs(odds - target)

            if not valid:
                continue

            score = total_dev / len(sels)
            if score < best_score:
                best_score = score
                best = mkt

        return best

    def _parse_moneyline(
        self, mkt: dict, home_name: str, away_name: str
    ) -> List[Outcome]:
        """Parse a moneyline market."""
        sels = mkt.get("selection", [])
        result = []
        for sel in sels:
            if sel.get("suspended"):
                continue
            odds_str = sel.get("odds")
            if not odds_str:
                continue
            try:
                decimal_odds = float(odds_str)
            except (ValueError, TypeError):
                continue

            american = _decimal_to_american_odds(decimal_odds)
            sel_type = sel.get("type", "")
            name = resolve_team_name(sel.get("name", ""))

            # Use type to determine home/away if name doesn't match
            if sel_type == "A":
                name = name or away_name
            elif sel_type == "B":
                name = name or home_name

            rot = sel.get("rotationCode")
            result.append(Outcome(
                name=name,
                price=american,
                rotation_number=rot,
            ))

        return result if len(result) >= 2 else []

    def _parse_spread(
        self, mkt: dict, home_name: str, away_name: str
    ) -> List[Outcome]:
        """Parse a spread market."""
        sels = mkt.get("selection", [])
        result = []
        for sel in sels:
            if sel.get("suspended"):
                continue
            odds_str = sel.get("odds")
            if not odds_str:
                continue
            try:
                decimal_odds = float(odds_str)
            except (ValueError, TypeError):
                continue

            american = _decimal_to_american_odds(decimal_odds)
            sel_name = sel.get("name", "")
            sel_type = sel.get("type", "")

            # Extract point spread from selection name
            point = _extract_line_from_selection_name(sel_name)

            # Determine team name
            name = resolve_team_name(sel_name.rsplit(" ", 1)[0].strip()) if point is not None else resolve_team_name(sel_name)

            # Use type to assign if needed: AH = away handicap, BH = home handicap
            if sel_type == "AH":
                name = name or away_name
            elif sel_type == "BH":
                name = name or home_name

            rot = sel.get("rotationCode")
            result.append(Outcome(
                name=name,
                price=american,
                point=point,
                rotation_number=rot,
            ))

        return result if len(result) >= 2 else []

    def _parse_total(self, mkt: dict) -> List[Outcome]:
        """Parse a totals (over/under) market."""
        sels = mkt.get("selection", [])
        result = []
        for sel in sels:
            if sel.get("suspended"):
                continue
            odds_str = sel.get("odds")
            if not odds_str:
                continue
            try:
                decimal_odds = float(odds_str)
            except (ValueError, TypeError):
                continue

            american = _decimal_to_american_odds(decimal_odds)
            sel_name = sel.get("name", "")
            sel_type = sel.get("type", "")

            # Determine Over/Under from type or name
            if sel_type == "Over" or "over" in sel_name.lower():
                name = "Over"
            elif sel_type == "Under" or "under" in sel_name.lower():
                name = "Under"
            else:
                name = sel_name

            # Extract total line from selection name
            point = _extract_line_from_selection_name(sel_name)

            result.append(Outcome(
                name=name,
                price=american,
                point=point,
            ))

        return result if len(result) >= 2 else []

    def _parse_yes_no(self, mkt: dict) -> List[Outcome]:
        """Parse a Yes/No market (e.g., BTTS)."""
        sels = mkt.get("selection", [])
        result = []
        for sel in sels:
            if sel.get("suspended"):
                continue
            odds_str = sel.get("odds")
            if not odds_str:
                continue
            try:
                decimal_odds = float(odds_str)
            except (ValueError, TypeError):
                continue
            american = _decimal_to_american_odds(decimal_odds)
            sel_name = sel.get("name", "")
            sel_type = sel.get("type", "")
            if sel_type == "Yes" or "yes" in sel_name.lower():
                name = "Yes"
            elif sel_type == "No" or "no" in sel_name.lower():
                name = "No"
            else:
                name = sel_name
            result.append(Outcome(name=name, price=american))
        return result if len(result) >= 2 else []

    def _parse_team_total(self, mkt: dict, team_name: str) -> List[Outcome]:
        """Parse a team totals (over/under) market."""
        sels = mkt.get("selection", [])
        result = []
        for sel in sels:
            if sel.get("suspended"):
                continue
            odds_str = sel.get("odds")
            if not odds_str:
                continue
            try:
                decimal_odds = float(odds_str)
            except (ValueError, TypeError):
                continue

            american = _decimal_to_american_odds(decimal_odds)
            sel_name = sel.get("name", "")
            sel_type = sel.get("type", "")

            if sel_type == "Over" or "over" in sel_name.lower():
                name = "Over"
            elif sel_type == "Under" or "under" in sel_name.lower():
                name = "Under"
            else:
                name = sel_name

            point = _extract_line_from_selection_name(sel_name)

            result.append(Outcome(
                name=name,
                price=american,
                point=point,
            ))

        return result if len(result) >= 2 else []

    async def get_player_props(self, sport_key: str, event_id: str) -> List[PlayerProp]:
        """Fetch player props for a specific event."""
        hr_event_id = self._event_ids.get(event_id)
        if not hr_event_id:
            return []

        try:
            resp = await self._client.post(
                GRAPHQL_URL,
                json={
                    "query": _PLAYER_PROPS_QUERY,
                    "variables": {
                        "channel": CHANNEL,
                        "segment": SEGMENT,
                        "region": "us",
                        "language": "enus",
                        "filters": [
                            {"field": "eventId", "values": hr_event_id},
                        ],
                    },
                },
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"HardRock: player props failed for {event_id}: {e}")
            return []

        events = data.get("data", {}).get("betSync", {}).get("events", {}).get("data", [])
        if not events:
            return []

        props: List[PlayerProp] = []
        for event in events:
            for mkt in event.get("markets", []):
                if not mkt.get("playerProp"):
                    continue
                if mkt.get("suspended"):
                    continue

                player = mkt.get("player", "")
                if not player:
                    continue

                # Determine stat type from market name/type
                mkt_name = mkt.get("name", "").lower()
                stat_type = self._classify_player_prop(mkt_name)
                if not stat_type:
                    continue

                for sel in mkt.get("selection", []):
                    if sel.get("suspended"):
                        continue
                    odds_str = sel.get("odds")
                    if not odds_str:
                        continue
                    try:
                        decimal_odds = float(odds_str)
                    except (ValueError, TypeError):
                        continue

                    american = _decimal_to_american_odds(decimal_odds)
                    sel_type = sel.get("type", "")
                    sel_name = sel.get("name", "")

                    if sel_type == "Over" or "over" in sel_name.lower():
                        description = "Over"
                    elif sel_type == "Under" or "under" in sel_name.lower():
                        description = "Under"
                    else:
                        continue

                    line = _extract_line_from_selection_name(sel_name)
                    if line is None:
                        line_val = mkt.get("line")
                        if line_val is not None:
                            line = float(line_val)

                    if line is None:
                        continue

                    props.append(PlayerProp(
                        player_name=player,
                        stat_type=stat_type,
                        line=line,
                        price=american,
                        description=description,
                        bookmaker_key="hardrock",
                        bookmaker_title="Hard Rock Bet",
                    ))

        return props

    @staticmethod
    def _classify_player_prop(market_name: str) -> Optional[str]:
        """Classify a player prop market name into a stat type."""
        name = market_name.lower()
        if "point" in name and "rebound" in name and "assist" in name:
            return "pts_reb_ast"
        if "point" in name and "rebound" in name:
            return "pts_reb"
        if "point" in name and "assist" in name:
            return "pts_ast"
        if "rebound" in name and "assist" in name:
            return "reb_ast"
        if "point" in name or "pts" in name:
            return "points"
        if "rebound" in name or "reb" in name:
            return "rebounds"
        if "assist" in name or "ast" in name:
            return "assists"
        if "three" in name or "3-point" in name or "3pt" in name:
            return "threes"
        if "steal" in name:
            return "steals"
        if "block" in name:
            return "blocks"
        if "strikeout" in name:
            return "strikeouts"
        if "hit" in name and "pitch" not in name:
            return "hits"
        if "total base" in name:
            return "total_bases"
        if "rbi" in name or "runs batted" in name:
            return "rbis"
        if "run" in name:
            return "runs"
        if "goal" in name:
            return "goals"
        if "shot" in name:
            return "shots_on_goal"
        if "ace" in name:
            return "aces"
        return None

    async def close(self) -> None:
        await self._client.aclose()
