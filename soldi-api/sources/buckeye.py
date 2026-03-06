"""
Buckeye sportsbook scraper.
Uses the DGS/Visor platform API at demotest.me.

The site is behind Cloudflare bot protection. We use Playwright to
launch a headless browser once at startup to obtain the cf_clearance
cookie, then authenticate and use httpx for all subsequent API calls.

Auth: POST authenticateCustomer (returns JWT code for Bearer auth).
Odds: POST Get_LeagueLines2 per sport/league.

DGS field conventions (confirmed via real API responses):
- Team1 = away, Team2 = home (standard DGS ordering)
- Spread = the spread number (negative = favorite's handicap)
- FavoredTeamID = full name of the favored team
- SpreadAdj1/SpreadAdj2 = spread JUICE in American format
- TtlPtsAdj1/TtlPtsAdj2 = totals JUICE in American format
- MoneyLine1/MoneyLine2 = moneyline in American format
- Response wrapped in {"Lines": [...]}
"""

import asyncio
import logging
import math
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome, PlayerProp
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

SITE_URL = "https://demotest.me"
API_BASE = SITE_URL + "/cloud/api"

# OddsScreen sport_key -> (DGS sportType, sportSubType)
BUCKEYE_SPORT_MAP = {
    "basketball_nba": ("BASKETBALL", "NBA"),
    "americanfootball_nfl": ("FOOTBALL", "NFL"),
    "icehockey_nhl": ("HOCKEY", "NHL"),
    "baseball_mlb": ("BASEBALL", "MLB"),
    "basketball_ncaab": ("BASKETBALL", "NCAA"),
    "americanfootball_ncaaf": ("FOOTBALL", "College"),
    "soccer_epl": ("SOCCER", "ENG PREM"),
    "soccer_spain_la_liga": ("SOCCER", "SPA LA LIGA"),
    "soccer_germany_bundesliga": ("SOCCER", "GER BUNDE"),
    "soccer_italy_serie_a": ("SOCCER", "ITA SER A"),
    "soccer_uefa_champs_league": ("SOCCER", "UEFA CH LEA"),
    "soccer_france_ligue_one": ("SOCCER", "FRA LIGUE 1"),
    "soccer_fifa_world_cup": ("SOCCER", "WORLD CUP"),
    "soccer_australia_aleague": ("SOCCER", "AUST A LEAGUE"),
    # Tennis
    "tennis_atp": ("TENNIS", "ATP Matchups"),
    "tennis_wta": ("TENNIS", "WTA Matchups"),
    # Boxing
    "boxing_boxing": ("BOXING", "BOXING"),
    # MMA / UFC — DGS sport type TBD; these are best-guess values.
    # If MMA becomes available, the subtypes rotate per event card
    # (e.g. "UFC 325", "UFC Fight Night"), so we fetch ALL subtypes.
    "mma_mixed_martial_arts": ("MARTIAL ARTS", "UFC"),
}

# sport_key -> list of (period_name, market_key_suffix) for multi-period fetching.
# "Game" period (suffix "") is always fetched first and is not listed here.
BUCKEYE_PERIOD_MAP = {
    # Basketball: halves
    "basketball_nba": [("1st Half", "_h1"), ("2nd Half", "_h2")],
    "basketball_ncaab": [("1st Half", "_h1"), ("2nd Half", "_h2")],
    # Football: halves
    "americanfootball_nfl": [("1st Half", "_h1"), ("2nd Half", "_h2")],
    "americanfootball_ncaaf": [("1st Half", "_h1"), ("2nd Half", "_h2")],
    # Hockey: periods
    "icehockey_nhl": [("1st Period", "_p1"), ("2nd Period", "_p2"), ("3rd Period", "_p3")],
    # Baseball: first 5 innings
    "baseball_mlb": [("1st 5 Innings", "_f5")],
    # Soccer: halves
    "soccer_epl": [("1st Half", "_h1"), ("2nd Half", "_h2")],
    "soccer_spain_la_liga": [("1st Half", "_h1"), ("2nd Half", "_h2")],
    "soccer_germany_bundesliga": [("1st Half", "_h1"), ("2nd Half", "_h2")],
    "soccer_italy_serie_a": [("1st Half", "_h1"), ("2nd Half", "_h2")],
    "soccer_uefa_champs_league": [("1st Half", "_h1"), ("2nd Half", "_h2")],
    "soccer_france_ligue_one": [("1st Half", "_h1"), ("2nd Half", "_h2")],
    "soccer_fifa_world_cup": [("1st Half", "_h1"), ("2nd Half", "_h2")],
    "soccer_australia_aleague": [("1st Half", "_h1"), ("2nd Half", "_h2")],
    # Tennis / Boxing / MMA: no period markets (full match only)
}  # type: Dict[str, List[Tuple[str, str]]]

# Live period names to probe — DGS uses these for in-play odds.
# Multiple names are tried because different DGS installations use
# different labels. "Prime Live" and "Commercial Live" are the most
# common on Buckeye/Visor-based books.
BUCKEYE_LIVE_PERIODS = [
    "Prime Live",
    "Commercial Live",
    "Ultra Live",
    "Live",
    "In-Game",
]  # type: List[str]

# DGS live betting uses sportType="LIVE" and sportSubType="{LEAGUE} LIVE".
# This is a separate section from the regular pre-game odds.
# Discovered by capturing the actual API calls from the Buckeye website's
# "Commercial Live" → "NBA - Live Betting" navigation.
BUCKEYE_LIVE_MAP = {
    "basketball_nba": "NBA LIVE",
    "basketball_ncaab": "NCAA LIVE",
    "americanfootball_nfl": "NFL LIVE",
    "icehockey_nhl": "NHL LIVE",
    "baseball_mlb": "MLB LIVE",
    "soccer_epl": "ENG PREM LIVE",
    "soccer_spain_la_liga": "SPA LA LIGA LIVE",
    "soccer_germany_bundesliga": "GER BUNDE LIVE",
    "soccer_italy_serie_a": "ITA SER A LIVE",
    "soccer_uefa_champs_league": "UEFA CH LEA LIVE",
    "soccer_france_ligue_one": "FRA LIGUE 1 LIVE",
    "soccer_fifa_world_cup": "WORLD CUP LIVE",
    "soccer_australia_aleague": "AUST A LEAGUE LIVE",
    "tennis_atp": "ATP Matchups LIVE",
    "tennis_wta": "WTA Matchups LIVE",
    "boxing_boxing": "BOXING LIVE",
    "mma_mixed_martial_arts": "UFC LIVE",
}  # type: Dict[str, str]

# MMA subtypes rotate per event card (UFC 325, UFC Fight Night, etc.).
# We try multiple common patterns when fetching MMA.
_MMA_SUBTYPES = [
    "UFC", "UFC 325", "UFC Fight Night", "UFC FN",
    "PFL", "Bellator", "MMA",
]

# sport_key -> DGS sportSubType for player props.
# DGS exposes props as a separate "league" using the same Get_LeagueLines2 endpoint.
# In the props response:
#   Team1ID = player name,  Team2ID = stat type (Points, Rebounds, etc.)
#   TotalPoints = O/U line,  TtlPtsAdj1 = Over juice,  TtlPtsAdj2 = Under juice
#   CorrelationID links back to the main game (format: "{Team1RotNum}-g")
BUCKEYE_PROPS_MAP = {
    "basketball_nba": "NBAPlayerProps",
    "basketball_ncaab": "NCAAPlayerProps",
    "americanfootball_nfl": "NFLPlayerProps",
    "americanfootball_ncaaf": "NCAAFPlayerProps",
    "icehockey_nhl": "NHLPlayerProps",
    "baseball_mlb": "MLBPlayerProps",
}  # type: Dict[str, str]

# DGS stat type label (Team2ID) → canonical stat_type
_DGS_STAT_MAP = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "pts+rebs+asts": "pts_reb_ast",
    "pts + rebs + asts": "pts_reb_ast",
    "pts+reb+ast": "pts_reb_ast",
    "3pt shots made": "threes",
    "3-pt shots made": "threes",
    "three pointers": "threes",
    "threes": "threes",
    "steals": "steals",
    "blocks": "blocks",
    "pts+rebs": "pts_reb",
    "pts+reb": "pts_reb",
    "pts + rebs": "pts_reb",
    "pts+asts": "pts_ast",
    "pts+ast": "pts_ast",
    "pts + asts": "pts_ast",
    "rebs+asts": "reb_ast",
    "reb+ast": "reb_ast",
    "rebs + asts": "reb_ast",
    "steals+blocks": "stl_blk",
    "stl+blk": "stl_blk",
    "steals + blocks": "stl_blk",
    "goals": "goals",
    "shots on goal": "shots_on_goal",
    "strikeouts": "strikeouts",
    "hits": "hits",
    "total bases": "total_bases",
    "rbi": "rbis",
    "runs": "runs",
    "passing yards": "pass_yards",
    "rushing yards": "rush_yards",
    "receiving yards": "rec_yards",
    "receptions": "receptions",
    "touchdowns": "touchdowns",
    "pass completions": "pass_completions",
    "interceptions": "interceptions",
}  # type: Dict[str, str]

# Keywords in team names that indicate a futures/championship market
_FUTURES_KEYWORDS = frozenset([
    "wins", "champion", "coin toss", "futures", "conference",
    "division", "mvp", "award", "super bowl", "world series",
    "stanley cup", "nba finals", "pennant", "cy young", "heisman",
])


class BuckeyeSource(DataSource):
    """Fetches odds from the Buckeye (DGS/Visor) sportsbook platform.

    Uses Playwright to obtain Cloudflare clearance cookies, then
    authenticates via the DGS API to get a JWT token. Subsequent
    API calls use httpx with those cookies and the JWT.
    """

    def __init__(self, username: str = "xl37", password: str = "test"):
        self._username = username.upper()
        self._password = password.upper()
        self._jwt_code = None  # type: Optional[str]
        self._jwt_expires = 0.0  # type: float
        self._customer_id = None  # type: Optional[str]
        self._cf_cookies = {}  # type: Dict[str, str]
        self._cf_cookies_expires = 0.0  # type: float
        self._client = None  # type: Optional[httpx.AsyncClient]
        self._init_done = False
        self._auth_lock = asyncio.Lock()  # Prevents concurrent auth attempts

    def _create_client(self) -> httpx.AsyncClient:
        """Create httpx client with current cookies."""
        return httpx.AsyncClient(
            timeout=20.0,
            cookies=self._cf_cookies,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": SITE_URL,
                "Referer": SITE_URL + "/",
            },
        )

    async def _get_cloudflare_cookies(self) -> bool:
        """Use Playwright to get Cloudflare clearance cookies."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("Buckeye: playwright not installed, cannot bypass Cloudflare")
            return False

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=False,
                    args=["--disable-blink-features=AutomationControlled"],
                )
                page = await browser.new_page()

                logger.info("Buckeye: launching browser for Cloudflare clearance...")
                await page.goto(SITE_URL + "/", wait_until="load", timeout=30000)
                await page.wait_for_timeout(3000)

                # Authenticate from within the page context
                auth_result = await page.evaluate("""
                    async ([username, password]) => {
                        try {
                            const resp = await fetch('/cloud/api/System/authenticateCustomer', {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                                    'X-Requested-With': 'XMLHttpRequest',
                                    'Authorization': 'Bearer ',
                                },
                                body: new URLSearchParams({
                                    customerID: username, password: password,
                                    state: 'true', response_type: 'code',
                                    client_id: username,
                                    domain: window.location.host,
                                    redirect_uri: window.location.host,
                                    operation: 'authenticateCustomer', RRO: '1',
                                }).toString(),
                                credentials: 'same-origin',
                            });
                            if (!resp.ok) return {error: 'HTTP ' + resp.status};
                            return await resp.json();
                        } catch(e) { return {error: e.toString()}; }
                    }
                """, [self._username, self._password])

                if "error" in auth_result:
                    logger.warning("Buckeye: browser auth failed: %s", auth_result["error"])
                    await browser.close()
                    return False

                # Extract JWT and account info
                self._jwt_code = auth_result.get("code", "")
                account_info = auth_result.get("accountInfo", {})
                self._customer_id = (account_info.get("customerID") or "").strip()
                self._jwt_expires = time.time() + 1200  # 20 min

                # Extract Cloudflare cookies
                cookies = await page.context.cookies()
                self._cf_cookies = {
                    c["name"]: c["value"]
                    for c in cookies
                    if "demotest.me" in c.get("domain", "")
                }
                # cf_clearance lasts ~30 min typically
                self._cf_cookies_expires = time.time() + 1500

                await browser.close()

            logger.info(
                "Buckeye: authenticated as %s, got %d cookies",
                self._customer_id, len(self._cf_cookies),
            )

            # Create/recreate httpx client with new cookies
            if self._client:
                await self._client.aclose()
            self._client = self._create_client()
            self._client.headers["Authorization"] = "Bearer %s" % self._jwt_code
            self._init_done = True
            return True

        except Exception as e:
            logger.warning("Buckeye: Cloudflare bypass failed: %s", e)
            return False

    async def _ensure_auth(self) -> bool:
        """Ensure we have valid cookies and JWT.

        Uses an asyncio lock so that when many concurrent prop requests
        arrive (e.g. 11 NBA games at once), only ONE request performs the
        auth refresh; the rest wait and then see already-valid credentials.
        """
        async with self._auth_lock:
            now = time.time()

            # Need new CF cookies (or first time)
            if not self._init_done or now >= self._cf_cookies_expires:
                return await self._get_cloudflare_cookies()

            # Need new JWT (CF cookies still valid)
            if not self._jwt_code or now >= self._jwt_expires:
                return await self._refresh_jwt()

            return True

    async def _refresh_jwt(self) -> bool:
        """Re-authenticate to get a new JWT using existing CF cookies."""
        if not self._client:
            return await self._get_cloudflare_cookies()

        try:
            resp = await self._client.post(
                API_BASE + "/System/authenticateCustomer",
                data={
                    "customerID": self._username,
                    "password": self._password,
                    "state": "true",
                    "response_type": "code",
                    "client_id": self._username,
                    "domain": "demotest.me",
                    "redirect_uri": "demotest.me",
                    "operation": "authenticateCustomer",
                    "RRO": "1",
                },
                headers={"Authorization": "Bearer "},
            )

            if resp.status_code == 401:
                # CF cookies expired, need full browser refresh
                return await self._get_cloudflare_cookies()

            resp.raise_for_status()
            data = resp.json()
            self._jwt_code = data.get("code", "")
            self._customer_id = (data.get("accountInfo", {}).get("customerID") or "").strip()
            self._jwt_expires = time.time() + 1200
            self._client.headers["Authorization"] = "Bearer %s" % self._jwt_code
            logger.info("Buckeye: JWT refreshed for %s", self._customer_id)
            return True

        except Exception as e:
            logger.warning("Buckeye: JWT refresh failed: %s", e)
            return await self._get_cloudflare_cookies()

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        headers = {"x-requests-remaining": "unlimited"}

        if bookmakers and "buckeye" not in bookmakers:
            return [], headers

        sport_info = BUCKEYE_SPORT_MAP.get(sport_key)
        if not sport_info:
            return [], headers

        if not await self._ensure_auth():
            return [], headers

        sport_type, sport_sub_type = sport_info

        try:
            # --- For MMA, try multiple subtypes (UFC event names rotate) ---
            if sport_key == "mma_mixed_martial_arts":
                return await self._fetch_mma(sport_type, markets)

            # --- Fetch Game period (always) ---
            resp = await self._fetch_period(sport_type, sport_sub_type, "Game")
            if resp is None:
                return [], headers

            resp.raise_for_status()
            data = resp.json()

            lines = data.get("Lines", [])
            if lines:
                logger.info("Buckeye: %d Game lines for %s", len(lines), sport_key)

            events = self._parse_lines(lines, sport_key, markets, period_suffix="")
            logger.info("Buckeye: %d Game events for %s", len(events), sport_key)

            # Build lookup: event_id -> event for merging (used by periods & live)
            event_map = {}  # type: Dict[str, OddsEvent]
            for ev in events:
                event_map[ev.id] = ev

            # --- Fetch additional periods and merge into game events ---
            extra_periods = BUCKEYE_PERIOD_MAP.get(sport_key, [])
            if extra_periods and events:
                for period_name, period_suffix in extra_periods:
                    try:
                        p_resp = await self._fetch_period(
                            sport_type, sport_sub_type, period_name
                        )
                        if p_resp is None:
                            continue
                        p_resp.raise_for_status()
                        p_data = p_resp.json()
                        p_lines = p_data.get("Lines", [])
                        if not p_lines:
                            continue

                        logger.info(
                            "Buckeye: %d %s lines for %s",
                            len(p_lines), period_name, sport_key,
                        )

                        p_events = self._parse_lines(
                            p_lines, sport_key, markets,
                            period_suffix=period_suffix,
                        )

                        # Merge period markets into matching game events
                        for p_ev in p_events:
                            game_ev = event_map.get(p_ev.id)
                            if game_ev is None:
                                # Period event has no matching game event — add standalone
                                events.append(p_ev)
                                event_map[p_ev.id] = p_ev
                            else:
                                # Append period markets to the existing bookmaker
                                if game_ev.bookmakers and p_ev.bookmakers:
                                    game_ev.bookmakers[0].markets.extend(
                                        p_ev.bookmakers[0].markets
                                    )

                    except Exception as e:
                        logger.debug(
                            "Buckeye: %s fetch failed for %s: %s",
                            period_name, sport_key, e,
                        )
                        continue

            # --- Fetch live/in-play odds ---
            # DGS live betting uses sportType="LIVE" with
            # sportSubType="{LEAGUE} LIVE" (e.g., "NBA LIVE").
            # This is separate from the regular pre-game endpoint.
            live_sub = BUCKEYE_LIVE_MAP.get(sport_key)
            live_found = 0
            if live_sub:
                try:
                    l_resp = await self._fetch_live(live_sub)
                    if l_resp is not None and l_resp.status_code == 200:
                        l_data = l_resp.json()
                        l_lines = l_data.get("Lines", [])
                        if l_lines:
                            logger.info(
                                "Buckeye: %d live lines for %s (subType=%s)",
                                len(l_lines), sport_key, live_sub,
                            )
                            l_events = self._parse_lines(
                                l_lines, sport_key, markets, period_suffix="",
                            )

                            if not l_events and l_lines:
                                # Lines exist but no odds parsed — likely
                                # "Available During Game Breaks" (odds appear
                                # only during commercial timeouts on DGS live).
                                logger.info(
                                    "Buckeye: %d live games in-progress for %s "
                                    "(odds available during game breaks only)",
                                    len(l_lines), sport_key,
                                )

                            for l_ev in l_events:
                                existing = event_map.get(l_ev.id)
                                if existing is None:
                                    # Live-only event (started after pregame snapshot)
                                    events.append(l_ev)
                                    event_map[l_ev.id] = l_ev
                                else:
                                    # Live odds override pregame base markets
                                    if existing.bookmakers and l_ev.bookmakers:
                                        live_keys = {
                                            m.key for m in l_ev.bookmakers[0].markets
                                        }
                                        existing.bookmakers[0].markets = [
                                            m for m in existing.bookmakers[0].markets
                                            if m.key not in live_keys
                                        ] + l_ev.bookmakers[0].markets

                            live_found += len(l_events)
                        else:
                            logger.info("Buckeye: 0 live lines for %s", sport_key)
                    else:
                        status = l_resp.status_code if l_resp else "None"
                        logger.info("Buckeye: live fetch returned %s for %s", status, sport_key)

                except Exception as e:
                    logger.debug(
                        "Buckeye: live fetch failed for %s: %s",
                        sport_key, e,
                    )

            if live_found:
                logger.info(
                    "Buckeye: %d live events merged for %s", live_found, sport_key,
                )

            logger.info("Buckeye: %d total events for %s", len(events), sport_key)
            return events, headers

        except Exception as e:
            logger.warning("Buckeye: failed for %s: %s", sport_key, e)
            return [], headers

    async def _fetch_mma(
        self,
        sport_type: str,
        markets: Optional[List[str]] = None,
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        """Fetch MMA events by trying multiple subtypes.

        UFC event names rotate (UFC 325 → UFC 326, UFC Fight Night, etc.)
        so we probe several common subtypes and combine results.
        """
        headers = {"x-requests-remaining": "unlimited"}
        all_lines = []  # type: List[dict]

        # Try multiple known MMA sport types and subtypes
        mma_sport_types = [sport_type, "MMA", "MARTIAL ARTS", "UFC", "FIGHTING"]
        seen_sport_types = set()  # type: set

        for st in mma_sport_types:
            if st in seen_sport_types:
                continue
            seen_sport_types.add(st)

            for sub in _MMA_SUBTYPES:
                try:
                    resp = await self._fetch_period(st, sub, "Game")
                    if resp is None:
                        continue
                    if resp.status_code != 200:
                        continue
                    data = resp.json()
                    lines = data.get("Lines", [])
                    if lines:
                        logger.info("Buckeye MMA: %d lines from %s/%s", len(lines), st, sub)
                        all_lines.extend(lines)
                except Exception:
                    continue

        if not all_lines:
            logger.debug("Buckeye MMA: no events found across all subtypes")
            return [], headers

        # Deduplicate by GameNum
        seen_games = set()  # type: set
        unique_lines = []
        for line in all_lines:
            game_num = line.get("GameNum")
            if game_num and game_num not in seen_games:
                seen_games.add(game_num)
                unique_lines.append(line)

        events = self._parse_lines(unique_lines, "mma_mixed_martial_arts", markets, period_suffix="")
        logger.info("Buckeye MMA: %d unique events", len(events))
        return events, headers

    async def _fetch_period(
        self,
        sport_type: str,
        sport_sub_type: str,
        period: str,
    ) -> Optional[httpx.Response]:
        """Fetch lines for a single period. Handles 401 retry. Returns None on failure."""
        resp = await self._client.post(
            API_BASE + "/Lines/Get_LeagueLines2",
            data={
                "customerID": self._customer_id or self._username,
                "operation": "Get_LeagueLines2",
                "sportType": sport_type,
                "sportSubType": sport_sub_type,
                "period": period,
                "RRO": "0",
            },
        )

        if resp.status_code == 401:
            self._jwt_code = None
            if await self._ensure_auth():
                resp = await self._client.post(
                    API_BASE + "/Lines/Get_LeagueLines2",
                    data={
                        "customerID": self._customer_id or self._username,
                        "operation": "Get_LeagueLines2",
                        "sportType": sport_type,
                        "sportSubType": sport_sub_type,
                        "period": period,
                        "RRO": "0",
                    },
                )
            else:
                return None

        return resp

    async def _fetch_live(
        self,
        live_sub_type: str,
    ) -> Optional[httpx.Response]:
        """Fetch live/in-play lines using the DGS live section.

        The DGS live betting section uses sportType="LIVE" with
        sportSubType="{LEAGUE} LIVE" (e.g., "NBA LIVE"), period="Game",
        and RRO="1". This was discovered by capturing the actual API
        calls from the Buckeye website's live betting UI.
        """
        data = {
            "customerID": self._customer_id or self._username,
            "operation": "Get_LeagueLines2",
            "sportType": "LIVE",
            "sportSubType": live_sub_type,
            "period": "Game",
            "RRO": "1",
            "wagerType": "Straight",
            "periodNumber": "0",
            "periods": "0",
        }

        resp = await self._client.post(
            API_BASE + "/Lines/Get_LeagueLines2",
            data=data,
        )

        if resp.status_code == 401:
            self._jwt_code = None
            if await self._ensure_auth():
                resp = await self._client.post(
                    API_BASE + "/Lines/Get_LeagueLines2",
                    data=data,
                )
            else:
                return None

        return resp

    def _parse_lines(
        self,
        lines: list,
        sport_key: str,
        markets: Optional[List[str]] = None,
        period_suffix: str = "",
    ) -> List[OddsEvent]:
        """Parse DGS lines into OddsEvent objects.

        Args:
            lines: Raw DGS line dicts from the API.
            sport_key: Canonical sport key (e.g., "basketball_nba").
            markets: Optional list of base market types to include.
            period_suffix: Suffix appended to market keys (e.g., "_h1", "_p2").
                           Empty string for full-game markets.
        """
        events = []  # type: List[OddsEvent]
        sport_title = get_sport_title(sport_key)

        # Always parse all available market types (h2h, spreads, totals).
        # The `markets` parameter is ignored because the background refresh loop
        # only requests ["h2h"] but we want to cache all markets for downstream use.
        want = {"h2h", "spreads", "totals"}

        for game in lines:
            try:
                # DGS: Team1 = away (listed first), Team2 = home (listed second)
                team1_id = (game.get("Team1ID") or "").strip()
                team2_id = (game.get("Team2ID") or "").strip()
                # Live lines append "(Live)" to team names — strip it
                team1_id = re.sub(r"\s*\(Live\)\s*$", "", team1_id, flags=re.IGNORECASE)
                team2_id = re.sub(r"\s*\(Live\)\s*$", "", team2_id, flags=re.IGNORECASE)
                if not team1_id or not team2_id:
                    logger.debug(
                        "Buckeye: skipping line — empty team: T1=%r T2=%r keys=%s",
                        team1_id, team2_id, list(game.keys())[:10],
                    )
                    continue

                # Skip futures/championship markets
                combined = (team1_id + " " + team2_id).lower()
                if any(kw in combined for kw in _FUTURES_KEYWORDS):
                    continue

                # GameDateTime may be absent — fall back to PeriodWagerCutoff or ScheduleDate
                game_dt = (
                    game.get("GameDateTime")
                    or game.get("PeriodWagerCutoff")
                    or game.get("ScheduleDate")
                    or ""
                )
                if not game_dt:
                    logger.debug(
                        "Buckeye: skipping line — no date: %s vs %s, keys=%s",
                        team1_id, team2_id, list(game.keys())[:10],
                    )
                    continue

                commence_time = self._parse_datetime(game_dt)
                away_team = resolve_team_name(team1_id)
                home_team = resolve_team_name(team2_id)
                cid = canonical_event_id(sport_key, home_team, away_team, commence_time)

                # Extract rotation numbers
                away_rot = self._safe_int(game.get("Team1RotNum"))
                home_rot = self._safe_int(game.get("Team2RotNum"))

                bk_markets = []  # type: List[Market]

                if "h2h" in want:
                    ml = self._parse_moneyline(game, away_team, home_team, sport_key=sport_key)
                    if ml:
                        # Attach rotation numbers to moneyline outcomes
                        for o in ml:
                            if o.name == away_team and away_rot is not None:
                                o.rotation_number = away_rot
                            elif o.name == home_team and home_rot is not None:
                                o.rotation_number = home_rot
                        bk_markets.append(Market(
                            key="h2h%s" % period_suffix, outcomes=ml,
                        ))

                if "spreads" in want:
                    sp = self._parse_spread(game, away_team, home_team)
                    if sp:
                        # Attach rotation numbers to spread outcomes
                        for o in sp:
                            if o.name == away_team and away_rot is not None:
                                o.rotation_number = away_rot
                            elif o.name == home_team and home_rot is not None:
                                o.rotation_number = home_rot
                        bk_markets.append(Market(
                            key="spreads%s" % period_suffix, outcomes=sp,
                        ))

                if "totals" in want:
                    tot = self._parse_totals(game)
                    if tot:
                        # For totals, attach away rot to Over, home rot to Under
                        for o in tot:
                            if o.name == "Over" and away_rot is not None:
                                o.rotation_number = away_rot
                            elif o.name == "Under" and home_rot is not None:
                                o.rotation_number = home_rot
                        bk_markets.append(Market(
                            key="totals%s" % period_suffix, outcomes=tot,
                        ))

                if not bk_markets:
                    continue

                events.append(OddsEvent(
                    id=cid,
                    sport_key=sport_key,
                    sport_title=sport_title,
                    commence_time=commence_time,
                    home_team=home_team,
                    away_team=away_team,
                    bookmakers=[
                        Bookmaker(
                            key="buckeye",
                            title="Buckeye",
                            markets=bk_markets,
                            event_url=SITE_URL,
                        )
                    ],
                ))

            except Exception as e:
                logger.warning("Buckeye: skipping game [%s vs %s]: %s", team1_id, team2_id, e)
                continue

        return events

    @staticmethod
    def _parse_datetime(dt_str: str) -> str:
        """Parse DGS datetime string to ISO 8601.

        DGS returns game times in US/Eastern (ET).  We convert to
        UTC so that canonical_event_id's date bucketing and the
        frontend's time display are both correct.
        """
        # DGS format: "2026-02-23 16:10:01.000"
        dt_str = dt_str.strip()
        if "T" in dt_str and ("+" in dt_str[10:] or dt_str.endswith("Z")):
            return dt_str  # already has tz info
        # Remove milliseconds
        if "." in dt_str:
            dt_str = dt_str.split(".")[0]
        # Remove trailing "T" artefacts
        if "T" in dt_str:
            dt_str = dt_str.replace("T", " ")
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %I:%M:%S %p",
            "%m/%d/%Y %H:%M:%S",
        ):
            try:
                dt = datetime.strptime(dt_str, fmt)
                # DGS times are US/Eastern — attach ET offset (EST = -5, EDT = -4)
                # Use -5 as a stable approximation (correct for most of the season)
                from datetime import timedelta
                et = timezone(timedelta(hours=-5))
                dt = dt.replace(tzinfo=et)
                return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            except ValueError:
                continue
        return dt_str

    @staticmethod
    def _safe_int(val):
        # type: (...) -> Optional[int]
        if val is None or val == "":
            return None
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_float(val):
        # type: (...) -> Optional[float]
        if val is None or val == "":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    # Standard deviation of game margin by sport (for spread→ML conversion)
    _SPORT_SIGMA = {
        "basketball_nba": 12.0,
        "basketball_ncaab": 11.0,
        "americanfootball_nfl": 13.5,
        "americanfootball_ncaaf": 16.0,
        "baseball_mlb": 1.5,
        "icehockey_nhl": 1.5,
    }

    @staticmethod
    def _prob_to_american(prob):
        # type: (float) -> int
        """Convert win probability (0-1) to American odds with standard vig."""
        if prob <= 0.01:
            return 5000
        if prob >= 0.99:
            return -5000
        if prob >= 0.5:
            return -round(100 * prob / (1 - prob))
        else:
            return round(100 * (1 - prob) / prob)

    def _derive_moneyline_from_spread(
        self, game: dict, sport_key: str, away: str, home: str
    ) -> List[Outcome]:
        """Derive approximate moneyline from spread using normal distribution.

        When the DGS account only provides spread data, we can convert the
        point spread to an implied moneyline using the relationship:
            win_probability = Φ(spread / σ)
        where σ is the sport-specific standard deviation of game margin
        and Φ is the normal CDF.
        """
        spread_val = self._safe_float(game.get("Spread"))
        if spread_val is None:
            return []
        # Pick-em (spread=0) → equal odds with vig
        if spread_val == 0:
            return [
                Outcome(name=away, price=-110),
                Outcome(name=home, price=-110),
            ]

        sigma = self._SPORT_SIGMA.get(sport_key, 12.0)
        margin = abs(spread_val)
        # Normal CDF: probability the favorite wins
        fav_prob = 0.5 * (1.0 + math.erf(margin / (sigma * math.sqrt(2))))
        dog_prob = 1.0 - fav_prob

        # Add standard vig (~4.5% total overround, split evenly)
        fav_prob_vig = min(0.99, fav_prob * 1.023)
        dog_prob_vig = min(0.99, dog_prob * 1.023)

        fav_odds = self._prob_to_american(fav_prob_vig)
        dog_odds = self._prob_to_american(dog_prob_vig)

        # Determine which team is favored
        favored = (game.get("FavoredTeamID") or "").strip()
        team1_id = (game.get("Team1ID") or "").strip()

        if favored == team1_id:
            # Away is favored
            return [
                Outcome(name=away, price=fav_odds),
                Outcome(name=home, price=dog_odds),
            ]
        else:
            # Home is favored (default)
            return [
                Outcome(name=away, price=dog_odds),
                Outcome(name=home, price=fav_odds),
            ]

    def _parse_moneyline(
        self, game: dict, away: str, home: str, sport_key: str = ""
    ) -> List[Outcome]:
        """Parse moneyline. Team1=away, Team2=home. Includes Draw for soccer.
        Falls back to deriving ML from spread if ML fields are absent."""
        ml1 = self._safe_int(game.get("MoneyLine1"))
        ml2 = self._safe_int(game.get("MoneyLine2"))
        if ml1 is None or ml2 is None:
            # ML not provided — derive from spread if available
            if sport_key:
                return self._derive_moneyline_from_spread(game, sport_key, away, home)
            return []
        if ml1 == 0 and ml2 == 0:
            return []
        outcomes = [
            Outcome(name=away, price=ml1),
            Outcome(name=home, price=ml2),
        ]
        # Include Draw outcome for soccer
        ml_draw = self._safe_int(game.get("MoneyLineDraw"))
        if ml_draw is not None and ml_draw != 0:
            outcomes.append(Outcome(name="Draw", price=ml_draw))
        return outcomes

    def _parse_spread(
        self, game: dict, away: str, home: str
    ) -> List[Outcome]:
        """Parse spread from DGS game data.

        Confirmed field mapping:
        - Spread = the spread number (favorite's spread, e.g., -3.5)
        - FavoredTeamID = full name of the favored team
        - SpreadAdj1 = Team1 (away) spread JUICE in American format
        - SpreadAdj2 = Team2 (home) spread JUICE in American format
        """
        spread_val = self._safe_float(game.get("Spread"))
        if spread_val is None:
            return []

        juice1 = self._safe_int(game.get("SpreadAdj1"))
        juice2 = self._safe_int(game.get("SpreadAdj2"))
        if juice1 is None or juice2 is None:
            return []

        # Determine which team gets the negative spread
        favored = (game.get("FavoredTeamID") or "").strip()
        team1_id = (game.get("Team1ID") or "").strip()
        team2_id = (game.get("Team2ID") or "").strip()

        if favored == team2_id:
            # Team2 (home) is favored: gets negative spread
            team1_point = abs(spread_val)
            team2_point = -abs(spread_val)
        elif favored == team1_id:
            # Team1 (away) is favored: gets negative spread
            team1_point = -abs(spread_val)
            team2_point = abs(spread_val)
        else:
            # Can't determine — use spread as-is for team1
            team1_point = float(spread_val)
            team2_point = float(-spread_val)

        return [
            Outcome(name=away, price=juice1, point=team1_point),
            Outcome(name=home, price=juice2, point=team2_point),
        ]

    def _parse_totals(self, game: dict) -> List[Outcome]:
        """Parse totals. TtlPtsAdj1=over juice, TtlPtsAdj2=under juice."""
        total_points = self._safe_float(game.get("TotalPoints"))
        if total_points is None or total_points <= 0:
            return []

        over_juice = self._safe_int(game.get("TtlPtsAdj1")) or -110
        under_juice = self._safe_int(game.get("TtlPtsAdj2")) or -110

        return [
            Outcome(name="Over", price=over_juice, point=total_points),
            Outcome(name="Under", price=under_juice, point=total_points),
        ]

    # ------------------------------------------------------------------
    # Player props
    # ------------------------------------------------------------------

    async def get_player_props(
        self, sport_key: str, event_id: str
    ) -> List[PlayerProp]:
        """Fetch player props from Buckeye's DGS platform.

        DGS exposes player props as a separate "league" (sportSubType)
        accessible via the same Get_LeagueLines2 endpoint used for game
        lines.  For example, NBA player props use sportSubType="NBAPlayerProps".

        In the props response each "line" represents one player + stat O/U:
          Team1ID  = player name  (e.g. "Tyrese Maxey")
          Team2ID  = stat label   (e.g. "Points", "Pts+Rebs+Asts")
          TotalPoints = O/U line  (e.g. 29.5)
          TtlPtsAdj1  = Over juice  (e.g. -120)
          TtlPtsAdj2  = Under juice (e.g. -110)
          CorrelationID links back to the game via "{Team1RotNum}-g".
        """
        props_sub_type = BUCKEYE_PROPS_MAP.get(sport_key)
        if not props_sub_type:
            return []

        sport_info = BUCKEYE_SPORT_MAP.get(sport_key)
        if not sport_info:
            return []

        if not await self._ensure_auth():
            return []

        sport_type = sport_info[0]
        game_sub_type = sport_info[1]

        try:
            # Step 1: Fetch regular game lines to build CorrelationID → event_id map
            game_resp = await self._fetch_period(sport_type, game_sub_type, "Game")
            if game_resp is None or game_resp.status_code != 200:
                return []

            game_data = game_resp.json()
            game_lines = game_data.get("Lines", [])

            # Build correlation map: "501-g" → canonical_event_id
            corr_to_event = {}  # type: Dict[str, str]
            for game in game_lines:
                team1_id = (game.get("Team1ID") or "").strip()
                team2_id = (game.get("Team2ID") or "").strip()
                if not team1_id or not team2_id:
                    continue
                game_dt = (
                    game.get("GameDateTime")
                    or game.get("PeriodWagerCutoff")
                    or game.get("ScheduleDate")
                    or ""
                )
                if not game_dt:
                    continue
                ct = self._parse_datetime(game_dt)
                away = resolve_team_name(team1_id)
                home = resolve_team_name(team2_id)
                cid = canonical_event_id(sport_key, home, away, ct)
                corr_id = (game.get("CorrelationID") or "").strip()
                if corr_id:
                    corr_to_event[corr_id] = cid

            # Find which CorrelationID corresponds to our target event
            target_corr = None
            for corr_id, cid in corr_to_event.items():
                if cid == event_id:
                    target_corr = corr_id
                    break

            if not target_corr:
                logger.debug("Buckeye props: no matching CorrelationID for %s", event_id)
                return []

            # Step 2: Fetch player props lines
            props_resp = await self._fetch_period(sport_type, props_sub_type, "Game")
            if props_resp is None or props_resp.status_code != 200:
                logger.debug("Buckeye props: failed to fetch %s lines", props_sub_type)
                return []

            props_data = props_resp.json()
            prop_lines = props_data.get("Lines", [])
            if not prop_lines:
                logger.debug("Buckeye props: 0 lines from %s", props_sub_type)
                return []

            # Step 3: Filter props for our target event and parse
            props = []  # type: List[PlayerProp]
            for line in prop_lines:
                corr_id = (line.get("CorrelationID") or "").strip()
                if corr_id != target_corr:
                    continue

                player = (line.get("Team1ID") or "").strip()
                stat_label = (line.get("Team2ID") or "").strip()
                if not player or not stat_label:
                    continue

                # Map DGS stat label to canonical stat_type
                stat_type = _DGS_STAT_MAP.get(stat_label.lower(), "other")

                total_pts = self._safe_float(line.get("TotalPoints"))
                if not total_pts or total_pts <= 0:
                    continue

                over_juice = self._safe_int(line.get("TtlPtsAdj1"))
                under_juice = self._safe_int(line.get("TtlPtsAdj2"))

                if over_juice is not None:
                    props.append(PlayerProp(
                        player_name=player,
                        stat_type=stat_type,
                        line=total_pts,
                        price=over_juice,
                        description="Over",
                        bookmaker_key="buckeye",
                        bookmaker_title="Buckeye",
                        event_url=SITE_URL,
                    ))

                if under_juice is not None:
                    props.append(PlayerProp(
                        player_name=player,
                        stat_type=stat_type,
                        line=total_pts,
                        price=under_juice,
                        description="Under",
                        bookmaker_key="buckeye",
                        bookmaker_title="Buckeye",
                        event_url=SITE_URL,
                    ))

            logger.info(
                "Buckeye: %d player props for %s (from %d total %s lines)",
                len(props), event_id, len(prop_lines), props_sub_type,
            )
            return props

        except Exception as e:
            logger.warning("Buckeye props failed for %s: %s", event_id, e)
            return []

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
