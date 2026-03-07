"""Bookmaker.eu — HTTP-based scraper (direct API calls via httpx).

Logs in to be.bookmaker.eu via an HTTP session, then calls the
GetSchedule API directly for each league (NBA, NCAAB, MLB, NHL, UFC)
to fetch both pregame and live games with moneyline/spread/total odds.

No Playwright or browser required.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome, ScoreData
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

SITE_URL = "https://be.bookmaker.eu"
_CACHE_TTL = 90  # seconds — prefetch loop keeps cache warm every ~45s

# Map Bookmaker sport-ID (idspt) to OddsScreen sport_key.
_SPORT_ID_MAP = {
    "NBA": "basketball_nba",
    "CBB": "basketball_ncaab",
    "MLB": "baseball_mlb",
    "NHL": "icehockey_nhl",
    "NFL": "americanfootball_nfl",
}

# Map Bookmaker league IDs to sport keys.
# Used for GetSchedule API calls and for resolving MU (multi-use) sport IDs.
# Sidebar idleague format: "{idspt}_{leagueId}" e.g. "NBA_3", "MU_206"
_LEAGUE_CONFIG = {
    3: "basketball_nba",           # NBA Game Lines
    4: "basketball_ncaab",         # NCAA Game Lines
    5: "baseball_mlb",             # MLB Game Lines
    7: "icehockey_nhl",            # NHL Game Lines
    206: "mma_mixed_martial_arts", # UFC
}

# For MU (multi-use) sport ID, map league ID to sport key
_MU_LEAGUE_MAP = {
    206: "mma_mixed_martial_arts",    # UFC
    18027: "boxing_boxing",           # Boxing
    12064: "boxing_boxing",           # Boxing (Other)
}

_SUPPORTED_SPORTS = set(_SPORT_ID_MAP.values()) | set(_LEAGUE_CONFIG.values()) | set(_MU_LEAGUE_MAP.values())

_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": SITE_URL,
    "Referer": f"{SITE_URL}/en/sports/",
}


class BookmakerSource(DataSource):
    """Scrape odds from Bookmaker.eu using direct HTTP API calls."""

    def __init__(self, username: str = "", password: str = ""):
        self._username = username
        self._password = password
        self._client: Optional[httpx.AsyncClient] = None
        self._logged_in = False
        self._lock = asyncio.Lock()
        # Per-sport cache: {sport_key: (events, timestamp)}
        self._cache: Dict[str, Tuple[List[OddsEvent], float]] = {}
        self._prefetch_task = None

    # ── Prefetch ────────────────────────────────────────────────────────────

    def start_prefetch(self):
        """Kick off background cache warming (call after event loop is up)."""
        if not self._username or not self._password:
            return
        self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    async def _prefetch_all(self):
        """Continuous warm-up: login and capture the full schedule in a loop."""
        await asyncio.sleep(8)  # let other sources init first
        logger.info("Bookmaker: Starting continuous background prefetch")
        cycle = 0
        while True:
            cycle += 1
            try:
                async with self._lock:
                    await self._ensure_session()
                    all_events = await self._fetch_all_odds()
                    now = time.time()
                    for sport_key, events in all_events.items():
                        self._cache[sport_key] = (events, now)
                        logger.info(
                            "Bookmaker prefetch: %d events for %s",
                            len(events),
                            sport_key,
                        )
                    # Mark empty sports as fresh too (so we don't re-fetch immediately)
                    for sk in _SUPPORTED_SPORTS:
                        if sk not in self._cache:
                            self._cache[sk] = ([], now)
                logger.info("Bookmaker: Prefetch cycle #%d complete", cycle)
            except Exception as exc:
                logger.warning("Bookmaker prefetch cycle #%d failed: %s", cycle, exc)
            await asyncio.sleep(45)  # Bookmaker fetches all sports at once; keep cache warm

    # ── HTTP session lifecycle ────────────────────────────────────────────

    async def _ensure_session(self):
        """Create httpx client and log in if needed (idempotent)."""
        if self._client is not None and self._logged_in:
            return
        if not self._username or not self._password:
            return

        # Create a fresh client with cookies
        if self._client:
            try:
                await self._client.aclose()
            except Exception:
                pass

        self._client = httpx.AsyncClient(
            headers=_HTTP_HEADERS,
            timeout=httpx.Timeout(30.0, connect=15.0),
            follow_redirects=True,
        )

        ok = await self._login()
        if not ok:
            logger.error("Bookmaker HTTP login failed")
            self._logged_in = False
        else:
            self._logged_in = True

    async def _login(self) -> bool:
        """Log in via HTTP POST to get session cookies."""
        try:
            # First, visit the site to get initial cookies/CSRF tokens
            resp = await self._client.get(
                f"{SITE_URL}/en/sports/",
            )
            logger.info("Bookmaker: Initial page load status %d", resp.status_code)

            # Try various login endpoints used by Bookmaker.eu
            login_endpoints = [
                f"{SITE_URL}/api/auth/login",
                f"{SITE_URL}/api/v1/auth/login",
                f"{SITE_URL}/gateway/UserProxy.aspx/Login",
                f"{SITE_URL}/api/account/login",
            ]

            login_payloads = [
                # JSON format
                {"username": self._username, "password": self._password},
                {"account": self._username, "password": self._password},
                {"login": self._username, "password": self._password},
                # Wrapped format (common in .NET backends)
                {"o": {"UserName": self._username, "Password": self._password}},
            ]

            for endpoint in login_endpoints:
                for payload in login_payloads:
                    try:
                        resp = await self._client.post(
                            endpoint,
                            json=payload,
                        )
                        if resp.status_code == 200:
                            try:
                                data = resp.json()
                                # Check for success indicators
                                if data.get("success") or data.get("token") or data.get("sessionId"):
                                    logger.info("Bookmaker: Login success via %s", endpoint)
                                    return True
                            except Exception:
                                pass
                        elif resp.status_code in (401, 403):
                            continue
                    except Exception:
                        continue

            # Even if explicit login failed, the session cookies from visiting
            # the site may be sufficient for the GetSchedule API (which is
            # sometimes accessible without authentication on Bookmaker.eu)
            logger.warning("Bookmaker: No login endpoint worked — trying API with session cookies")
            self._logged_in = True
            return True

        except Exception as exc:
            logger.error("Bookmaker login error: %s", exc)
            return False

    # ── get_odds ────────────────────────────────────────────────────────────

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        headers = {"x-requests-remaining": "unlimited"}

        if bookmakers and "bookmaker" not in bookmakers:
            return [], headers
        if not self._username or not self._password:
            return [], headers
        if sport_key not in _SUPPORTED_SPORTS:
            return [], headers

        # Always serve from cache — prefetch loop keeps it warm.
        # Never fall through to API in the composite context.
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _CACHE_TTL:
            return cached[0], headers
        # Serve stale data if prefetch hasn't refreshed yet
        if cached:
            return cached[0], headers
        return [], headers

    # ── Fetch + capture ─────────────────────────────────────────────────────

    async def _fetch_all_odds(self) -> Dict[str, List[OddsEvent]]:
        """Call GetSchedule API for each league, parse all results."""
        all_events = {}  # type: Dict[str, List[OddsEvent]]

        # Build comma-separated league IDs for a single batch call
        league_ids = ",".join(str(lid) for lid in _LEAGUE_CONFIG.keys())

        schedule_data = await self._call_get_schedule(league_ids)
        if schedule_data:
            events = self._parse_schedule(schedule_data)
            for sk, evts in events.items():
                all_events.setdefault(sk, []).extend(evts)

        if not all_events:
            # Fallback: try individual league calls
            logger.info("Bookmaker: batch call returned no data, trying per-league")
            for league_id, sport_key in _LEAGUE_CONFIG.items():
                try:
                    data = await self._call_get_schedule(str(league_id))
                    if data:
                        events = self._parse_schedule(data)
                        for sk, evts in events.items():
                            all_events.setdefault(sk, []).extend(evts)
                except Exception as exc:
                    logger.debug("Bookmaker league %d failed: %s", league_id, exc)

        if not all_events:
            # Session may have expired — force re-login on next cycle
            logger.warning("Bookmaker: no events returned — will re-login next cycle")
            self._logged_in = False

        return all_events

    async def _call_get_schedule(self, league_ids: str) -> Optional[dict]:
        """Call GetSchedule API via direct HTTP POST.

        Args:
            league_ids: Comma-separated league IDs (e.g. "3,4,5,7,206")

        Returns:
            Parsed JSON dict or None on failure.
        """
        if not self._client:
            return None

        payload = {
            "o": {
                "BORequestData": {
                    "BOParameters": {
                        "BORt": {},
                        "LeaguesIdList": league_ids,
                        "LanguageId": "0",
                        "LineStyle": "E",
                        "ScheduleType": "american",
                        "LinkDeriv": "true",
                    }
                }
            }
        }

        try:
            resp = await self._client.post(
                f"{SITE_URL}/gateway/BetslipProxy.aspx/GetSchedule",
                json=payload,
            )

            if resp.status_code != 200:
                logger.warning(
                    "Bookmaker GetSchedule HTTP %d for leagues %s",
                    resp.status_code, league_ids,
                )
                return None

            text = resp.text
            if len(text) < 50:
                logger.warning(
                    "Bookmaker GetSchedule: empty response (%d bytes) for leagues %s",
                    len(text), league_ids,
                )
                return None

            data = resp.json()
            if not isinstance(data, dict):
                return None

            if data.get("status") == "error":
                logger.warning(
                    "Bookmaker GetSchedule API error for leagues %s: %s",
                    league_ids, data.get("error_message", "unknown"),
                )
                return None

            logger.info(
                "Bookmaker GetSchedule OK for leagues %s (%d bytes)",
                league_ids, len(text),
            )
            return data
        except Exception as exc:
            logger.warning("Bookmaker GetSchedule call failed: %s", exc)
            return None

    # ── Parse schedule ──────────────────────────────────────────────────────

    def _parse_schedule(self, data: dict) -> Dict[str, List[OddsEvent]]:
        """Parse GetSchedule response into {sport_key: [OddsEvent]}.

        Response structure:
          Schedule.Data.Leagues.League -> list of league objects
        Each league has: Description, IdLeague, IdSport, dateGroup -> game[]
        """
        result = {}  # type: Dict[str, List[OddsEvent]]

        try:
            leagues_wrapper = (
                data.get("Schedule", {}).get("Data", {}).get("Leagues", {})
            )
            league_list = leagues_wrapper.get("League", [])
        except (AttributeError, TypeError):
            logger.warning("Bookmaker: unexpected schedule format")
            return result

        if not isinstance(league_list, list):
            league_list = [league_list] if league_list else []

        for league in league_list:
                desc = league.get("Description", "")
                idspt = league.get("IdSport", "")
                league_id = self._safe_int(league.get("IdLeague"))

                # Resolve sport key: direct map first, then MU league map
                sport_key = _SPORT_ID_MAP.get(idspt)
                if not sport_key and idspt == "MU" and league_id is not None:
                    sport_key = _MU_LEAGUE_MAP.get(league_id)

                # Also try _LEAGUE_CONFIG for any league ID we know about
                if not sport_key and league_id is not None:
                    sport_key = _LEAGUE_CONFIG.get(league_id)

                if not sport_key:
                    continue

                # Accept all leagues for the sport — "GAME LINE" filter was too
                # restrictive and was dropping pre-game odds. Log the description
                # for debugging but don't skip.
                desc_upper = desc.upper()
                if "PROP" in desc_upper or "FUTURES" in desc_upper or "SPECIAL" in desc_upper:
                    # Skip props/futures/specials — they aren't standard game lines
                    continue

                sport_title = get_sport_title(sport_key)

                date_groups = league.get("dateGroup", [])
                if not isinstance(date_groups, list):
                    date_groups = [date_groups]

                for dg in date_groups:
                    games = dg.get("game", [])
                    if not isinstance(games, list):
                        games = [games]

                    for game in games:
                        event = self._parse_game(game, sport_key, sport_title)
                        if event:
                            result.setdefault(sport_key, []).append(event)

        # Deduplicate by event ID
        for sk in list(result.keys()):
            seen = set()  # type: set
            deduped = []
            for ev in result[sk]:
                if ev.id not in seen:
                    seen.add(ev.id)
                    deduped.append(ev)
            result[sk] = deduped

        return result

    def _parse_game(
        self, game: dict, sport_key: str, sport_title: str
    ) -> Optional[OddsEvent]:
        """Parse a single Bookmaker game dict into an OddsEvent."""
        try:
            home_team = (game.get("htm") or "").strip()
            away_team = (game.get("vtm") or "").strip()

            if not home_team or not away_team:
                return None

            # Skip hidden / fully closed games
            if game.get("HideGame") or game.get("MarketsClosed"):
                return None

            # Parse game time
            gmdt = game.get("gmdt", "")  # "20260224"
            gmtm = game.get("gmtm", "")  # "16:13:00"
            commence_time = self._parse_game_time(gmdt, gmtm)
            if not commence_time:
                return None

            # Find primary line (index == "0")
            derivatives = game.get("Derivatives") or {}
            lines = derivatives.get("line", [])
            if not isinstance(lines, list):
                lines = [lines] if lines else []

            primary_line = None
            alt_lines = []  # type: List[dict]
            for line in lines:
                if str(line.get("index", "")) == "0":
                    primary_line = line
                else:
                    alt_lines.append(line)

            if not primary_line:
                return None

            # Build markets
            markets_list = []  # type: List[Market]

            home_resolved = resolve_team_name(home_team)
            away_resolved = resolve_team_name(away_team)

            ml = self._parse_moneyline(primary_line, home_resolved, away_resolved)
            if ml:
                markets_list.append(ml)

            sp = self._parse_spread(primary_line, home_resolved, away_resolved)
            if sp:
                markets_list.append(sp)

            # If spreads not in primary line, check alternate lines
            if not sp and alt_lines:
                for alt_line in alt_lines:
                    sp = self._parse_spread(alt_line, home_resolved, away_resolved)
                    if sp:
                        markets_list.append(sp)
                        break

            tot = self._parse_total(primary_line)
            if tot:
                markets_list.append(tot)

            # Parse period/half lines from alt_lines
            # Bookmaker uses index values: 1=1H, 2=2H, 3=Q1, 4=Q2, 5=Q3, 6=Q4
            _PERIOD_INDEX_MAP = {
                "1": "_h1", "2": "_h2",
                "3": "_q1", "4": "_q2", "5": "_q3", "6": "_q4",
            }
            for alt_line in alt_lines:
                idx = str(alt_line.get("index", ""))
                suffix = _PERIOD_INDEX_MAP.get(idx)
                if not suffix:
                    continue

                alt_ml = self._parse_moneyline(alt_line, home_resolved, away_resolved)
                if alt_ml:
                    alt_ml.key = "h2h" + suffix
                    markets_list.append(alt_ml)

                alt_sp = self._parse_spread(alt_line, home_resolved, away_resolved)
                if alt_sp:
                    alt_sp.key = "spreads" + suffix
                    markets_list.append(alt_sp)

                alt_tot = self._parse_total(alt_line)
                if alt_tot:
                    alt_tot.key = "totals" + suffix
                    markets_list.append(alt_tot)

            if not markets_list:
                return None

            event_id = canonical_event_id(
                sport_key, home_team, away_team, commence_time
            )

            bookmaker_obj = Bookmaker(
                key="bookmaker",
                title="Bookmaker.eu",
                last_update=datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                markets=markets_list,
                event_url="https://be.bookmaker.eu",
            )

            # Build ScoreData for live events
            score_data = None
            is_live = game.get("LiveGame", False)
            if is_live:
                home_score = (game.get("hpt") or "").strip() or None
                away_score = (game.get("vpt") or "").strip() or None
                period_desc = game.get("gpd", "")
                period_num = self._safe_int(game.get("gp"))
                score_data = ScoreData(
                    home_score=home_score,
                    away_score=away_score,
                    status="in",
                    detail=period_desc if period_desc and period_desc != "Game" else None,
                    period=period_num if period_num and period_num > 0 else None,
                )

            return OddsEvent(
                id=event_id,
                sport_key=sport_key,
                sport_title=sport_title,
                commence_time=commence_time,
                home_team=home_resolved,
                away_team=away_resolved,
                bookmakers=[bookmaker_obj],
                score_data=score_data,
            )
        except Exception as exc:
            logger.debug("Bookmaker parse game error: %s", exc)
            return None

    # ── Market parsers ──────────────────────────────────────────────────────

    def _parse_moneyline(
        self, line: dict, home: str, away: str
    ) -> Optional[Market]:
        # s_ml flag may be 0 for pre-game events even when odds exist.
        # Check for actual odds values regardless of the flag.
        home_odds = self._safe_int(line.get("hoddst"))
        away_odds = self._safe_int(line.get("voddst"))

        if home_odds is None or away_odds is None:
            return None
        if home_odds == 0 and away_odds == 0:
            return None

        outcomes = [
            Outcome(name=home, price=home_odds),
            Outcome(name=away, price=away_odds),
        ]

        # Soccer draw
        draw_odds = self._safe_int(line.get("drawoddst"))
        if draw_odds is not None and draw_odds != 0:
            outcomes.append(Outcome(name="Draw", price=draw_odds))

        return Market(key="h2h", outcomes=outcomes)

    def _parse_spread(
        self, line: dict, home: str, away: str, force: bool = False,
    ) -> Optional[Market]:
        # Always attempt to parse spreads — s_sp flag may be 0 for pre-game
        # even when spread data exists. Only skip if no data at all.

        home_spread = self._safe_float(line.get("hsprdt"))
        home_spread_odds = self._safe_int(line.get("hsprdoddst"))
        away_spread = self._safe_float(line.get("vsprdt"))
        away_spread_odds = self._safe_int(line.get("vsprdoddst"))

        if home_spread is None or home_spread_odds is None:
            return None
        if away_spread is None or away_spread_odds is None:
            return None
        if home_spread_odds == 0 and away_spread_odds == 0:
            return None

        outcomes = [
            Outcome(name=home, price=home_spread_odds, point=home_spread),
            Outcome(name=away, price=away_spread_odds, point=away_spread),
        ]
        return Market(key="spreads", outcomes=outcomes)

    def _parse_total(self, line: dict) -> Optional[Market]:
        # Always attempt to parse totals — s_tot flag may be 0 for pre-game
        # even when total data exists. Only skip if no data at all.

        total = self._safe_float(line.get("ovt"))
        over_odds = self._safe_int(line.get("ovoddst"))
        under_odds = self._safe_int(line.get("unoddst"))

        if total is None or over_odds is None or under_odds is None:
            return None
        if over_odds == 0 and under_odds == 0:
            return None

        outcomes = [
            Outcome(name="Over", price=over_odds, point=total),
            Outcome(name="Under", price=under_odds, point=total),
        ]
        return Market(key="totals", outcomes=outcomes)

    # ── Time parsing ────────────────────────────────────────────────────────

    @staticmethod
    def _parse_game_time(gmdt: str, gmtm: str) -> Optional[str]:
        """Convert Bookmaker date + time to ISO 8601 UTC.

        Bookmaker times are in US Eastern.
        gmdt: "20260224"  gmtm: "16:13:00"
        → "2026-02-24T21:13:00Z"
        """
        if not gmdt or len(gmdt) < 8:
            return None
        try:
            year = int(gmdt[:4])
            month = int(gmdt[4:6])
            day = int(gmdt[6:8])

            parts = gmtm.split(":") if gmtm else ["0", "0", "0"]
            hour = int(parts[0]) if len(parts) > 0 else 0
            minute = int(parts[1]) if len(parts) > 1 else 0
            second = int(parts[2]) if len(parts) > 2 else 0

            # Eastern time (UTC-5 standard — close enough for date bucketing)
            et_offset = timezone(timedelta(hours=-5))
            dt = datetime(year, month, day, hour, minute, second, tzinfo=et_offset)

            utc_dt = dt.astimezone(timezone.utc)
            return utc_dt.isoformat().replace("+00:00", "Z")
        except Exception:
            return f"{gmdt[:4]}-{gmdt[4:6]}-{gmdt[6:8]}T00:00:00Z"

    # ── Utility ─────────────────────────────────────────────────────────────

    @staticmethod
    def _safe_int(val) -> Optional[int]:
        if val is None or val == "":
            return None
        try:
            return int(float(str(val)))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        if val is None or val == "":
            return None
        try:
            return float(str(val))
        except (ValueError, TypeError):
            return None

    # ── Cleanup ──────────────────────────────────────────────────────────────

    async def close(self):
        """Shut down the HTTP client."""
        if self._client:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None
        self._logged_in = False
