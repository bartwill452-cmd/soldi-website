"""Bookmaker.eu — Playwright-based scraper (direct API calls).

Logs in to be.bookmaker.eu via headless Chromium, then calls the
GetSchedule API directly for each league (NBA, NCAAB, MLB, NHL, UFC)
to fetch both pregame and live games with moneyline/spread/total odds.

Follows the Bet105Source pattern: browser lifecycle, async lock, 90s
TTL cache, background prefetch, and stale-on-error fallback.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

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


class BookmakerSource(DataSource):
    """Scrape odds from Bookmaker.eu using Playwright passive capture."""

    def __init__(self, username: str = "", password: str = ""):
        self._username = username
        self._password = password
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None
        self._logged_in = False
        self._lock = asyncio.Lock()
        # Per-sport cache: {sport_key: (events, timestamp)}
        self._cache: Dict[str, Tuple[List[OddsEvent], float]] = {}
        self._prefetch_task = None

    # ── Prefetch ────────────────────────────────────────────────────────────

    def start_prefetch(self):
        """Kick off background cache warming (call after event loop is up)."""
        if not self._username or not self._password:
            logger.warning("Bookmaker: missing credentials (user=%s, pass=%s) — prefetch disabled",
                           bool(self._username), bool(self._password))
            return
        logger.info("Bookmaker: credentials present, starting prefetch")
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
                    await self._ensure_browser()
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

    # ── Browser lifecycle ───────────────────────────────────────────────────

    async def _ensure_browser(self):
        """Launch Playwright headless Chromium and log in (idempotent)."""
        if self._page is not None:
            return
        if not self._username or not self._password:
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
                    "--disable-gpu",
                ],
            )
            self._context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
            self._page = await self._context.new_page()
            ok = await self._login()
            if not ok:
                logger.error("Bookmaker login failed — closing browser")
                await self._close_browser()
        except Exception as exc:
            logger.error("Bookmaker browser init failed: %s", exc)
            await self._close_browser()
            raise

    async def _login(self) -> bool:
        """Navigate to site and fill/submit the login form."""
        try:
            await self._page.goto(
                f"{SITE_URL}/en/sports/",
                timeout=30000,
                wait_until="domcontentloaded",
            )
            await asyncio.sleep(3)
        except Exception as exc:
            logger.warning("Bookmaker navigation: %s", exc)

        # ── Fill username ──
        username_filled = False
        for sel in [
            'input[name="account"]',
            'input[name="username"]',
            'input[name="login"]',
            'input[type="text"]:first-of-type',
            'input[placeholder*="user" i]',
            'input[placeholder*="account" i]',
            'input[id*="user" i]',
            'input[id*="login" i]',
            'input[id*="account" i]',
        ]:
            try:
                el = await self._page.wait_for_selector(sel, timeout=2000)
                if el:
                    await el.fill(self._username)
                    username_filled = True
                    logger.info("Bookmaker username filled via: %s", sel)
                    break
            except Exception:
                pass

        # ── Fill password ──
        password_filled = False
        for sel in [
            'input[type="password"]',
            'input[name="password"]',
            'input[placeholder*="password" i]',
            'input[id*="password" i]',
        ]:
            try:
                el = await self._page.wait_for_selector(sel, timeout=2000)
                if el:
                    await el.fill(self._password)
                    password_filled = True
                    logger.info("Bookmaker password filled via: %s", sel)
                    break
            except Exception:
                pass

        if not username_filled or not password_filled:
            logger.error(
                "Bookmaker login form incomplete: user=%s, pass=%s",
                username_filled,
                password_filled,
            )
            return False

        # ── Submit ──
        submitted = False
        for sel in [
            'button[type="submit"]',
            'button:has-text("Log In")',
            'button:has-text("Login")',
            'button:has-text("Sign In")',
            'button:has-text("Submit")',
            'input[type="submit"]',
            'button:has-text("Enter")',
            ".login-btn",
            "#login-btn",
        ]:
            try:
                el = await self._page.wait_for_selector(sel, timeout=2000)
                if el:
                    await el.click()
                    submitted = True
                    logger.info("Bookmaker submit via: %s", sel)
                    break
            except Exception:
                pass

        if not submitted:
            await self._page.keyboard.press("Enter")
            logger.info("Bookmaker submit via Enter key")

        await asyncio.sleep(5)

        # ── Verify ──
        html = await self._page.content()
        html_lower = html.lower()
        if any(kw in html_lower for kw in ("logout", "account", "balance")):
            self._logged_in = True
            logger.info("Bookmaker login successful (URL: %s)", self._page.url)
            return True

        # Ambiguous — assume it worked (session cookie may be set)
        logger.warning("Bookmaker login status unclear (URL: %s)", self._page.url)
        self._logged_in = True
        return True

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
        # Never fall through to Playwright navigation in the composite context.
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
        # Ensure we're on the sports page (needed for cookies/session)
        current = self._page.url or ""
        if "/en/sports" not in current:
            await self._page.goto(
                f"{SITE_URL}/en/sports/",
                timeout=30000,
                wait_until="domcontentloaded",
            )
            await asyncio.sleep(5)

        # Wait for SPA to fully initialize (session cookies, Angular, etc.)
        await asyncio.sleep(3)

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
            # Check if we're actually logged out (look for login form specifically)
            logged_out = await self._page.evaluate("""
                () => {
                    const loginBtn = document.querySelector('button[type="submit"]');
                    const pwdInput = document.querySelector('input[type="password"]');
                    return !!(loginBtn && pwdInput);
                }
            """)
            if logged_out:
                logger.info("Bookmaker: session expired, re-logging in")
                await self._close_browser()
                raise RuntimeError("Bookmaker session expired")
            else:
                logger.warning("Bookmaker: no events returned but appears logged in")

        return all_events

    async def _call_get_schedule(self, league_ids: str) -> Optional[dict]:
        """Call GetSchedule API via page.evaluate(fetch(...)).

        Args:
            league_ids: Comma-separated league IDs (e.g. "3,4,5,7,206")

        Returns:
            Parsed JSON dict or None on failure.
        """
        try:
            result = await self._page.evaluate(
                """
                async (leagueIds) => {
                    try {
                        const resp = await fetch('/gateway/BetslipProxy.aspx/GetSchedule', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({
                                o: {
                                    BORequestData: {
                                        BOParameters: {
                                            BORt: {},
                                            LeaguesIdList: leagueIds,
                                            LanguageId: "0",
                                            LineStyle: "E",
                                            ScheduleType: "american",
                                            LinkDeriv: "true"
                                        }
                                    }
                                }
                            }),
                        });
                        const text = await resp.text();
                        return {
                            status: resp.status,
                            size: text.length,
                            data: text.length > 50 ? JSON.parse(text) : null,
                            error: resp.status !== 200 ? text.substring(0, 200) : null
                        };
                    } catch(e) {
                        return {status: 0, size: 0, data: null, error: e.message};
                    }
                }
                """,
                league_ids,
            )
            if not result or not isinstance(result, dict):
                logger.warning("Bookmaker GetSchedule: no result for leagues %s", league_ids)
                return None

            status = result.get("status", 0)
            size = result.get("size", 0)
            data = result.get("data")
            error = result.get("error")

            if status != 200 or error:
                logger.warning(
                    "Bookmaker GetSchedule HTTP %d for leagues %s: %s",
                    status, league_ids, error or "unknown",
                )
                return None

            if not data or not isinstance(data, dict):
                logger.warning(
                    "Bookmaker GetSchedule: empty/invalid data (%d bytes) for leagues %s",
                    size, league_ids,
                )
                return None

            if data.get("status") == "error":
                logger.warning(
                    "Bookmaker GetSchedule API error for leagues %s: %s",
                    league_ids, data.get("error_message", "unknown"),
                )
                return None

            logger.info(
                "Bookmaker GetSchedule OK for leagues %s (%d bytes)",
                league_ids, size,
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

            # Team totals — home and away
            home_tt = self._parse_team_total(primary_line, "home")
            if home_tt:
                markets_list.append(Market(key="team_total_home", outcomes=home_tt))
            away_tt = self._parse_team_total(primary_line, "away")
            if away_tt:
                markets_list.append(Market(key="team_total_away", outcomes=away_tt))

            # Parse period/half lines from alt_lines
            # Bookmaker uses index values: 1=1H, 2=2H, 3=Q1, 4=Q2, 5=Q3, 6=Q4,
            # 7=P1, 8=P2, 9=P3, 10=F5, 11=I1, 12=F7
            _PERIOD_INDEX_MAP = {
                "1": "_h1", "2": "_h2",
                "3": "_q1", "4": "_q2", "5": "_q3", "6": "_q4",
                "7": "_p1", "8": "_p2", "9": "_p3",
                "10": "_f5", "11": "_i1", "12": "_f7",
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

                # Period team totals
                alt_home_tt = self._parse_team_total(alt_line, "home")
                if alt_home_tt:
                    markets_list.append(Market(key="team_total_home" + suffix, outcomes=alt_home_tt))
                alt_away_tt = self._parse_team_total(alt_line, "away")
                if alt_away_tt:
                    markets_list.append(Market(key="team_total_away" + suffix, outcomes=alt_away_tt))

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

    def _parse_team_total(self, line: dict, side: str) -> Optional[List[Outcome]]:
        """Parse home or away team total from a Bookmaker line dict.

        Bookmaker uses hovt/hunoddst for home team total over/under,
        and vovt/vunoddst for away (visitor) team total.
        """
        if side == "home":
            total = self._safe_float(line.get("hovt"))
            over_odds = self._safe_int(line.get("hovoddst"))
            under_odds = self._safe_int(line.get("hunoddst"))
        else:
            total = self._safe_float(line.get("vovt"))
            over_odds = self._safe_int(line.get("vovoddst"))
            under_odds = self._safe_int(line.get("vunoddst"))

        if total is None or over_odds is None or under_odds is None:
            return None
        if over_odds == 0 and under_odds == 0:
            return None

        return [
            Outcome(name="Over", price=over_odds, point=total),
            Outcome(name="Under", price=under_odds, point=total),
        ]

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

    # ── Browser cleanup ─────────────────────────────────────────────────────

    async def _close_browser(self):
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
        self._logged_in = False

    async def close(self):
        """Shut down the Playwright browser."""
        await self._close_browser()
