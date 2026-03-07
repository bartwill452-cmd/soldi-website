"""
BetOnline sportsbook scraper.

Uses direct HTTP requests (httpx) to BetOnline's offering API.
No browser required — the API subdomain (api-offering.betonline.ag)
does not enforce Cloudflare challenges.

Architecture:
  1. POST to offering-by-league endpoint with sport/league params
  2. Parse the JSON responses with the same offering format as before
  3. Background prefetch loop keeps cache warm for all supported sports
"""

import asyncio
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
    "mma_mixed_martial_arts": ("martial-arts", "ufc"),
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

# Sports that support period markets (1st half / 1st quarter)
_PERIOD_SPORTS = frozenset([
    "basketball_nba", "basketball_ncaab",
    "americanfootball_nfl", "americanfootball_ncaaf",
    "baseball_mlb",
])

# Default headers for API requests
_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.betonline.ag",
    "Referer": "https://www.betonline.ag/",
}

# Number of retries for failed HTTP requests
_MAX_RETRIES = 2


class BetOnlineSource(DataSource):
    """Fetches odds from BetOnline via direct HTTP API calls.

    Uses httpx to POST to the offering API endpoint. No browser needed —
    the API subdomain does not enforce Cloudflare challenges.
    """

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None
        self._lock = asyncio.Lock()
        # Cache: sport_key → (events, timestamp)
        self._cache: Dict[str, Tuple[List[OddsEvent], float]] = {}
        # Props cache: "props:{sport_key}:{event_id}" → (props, timestamp)
        self._props_cache: Dict[str, Tuple[List[PlayerProp], float]] = {}
        self._prefetch_task = None  # type: ignore
        # Track consecutive zero-event cycles for health detection
        self._consecutive_zero_cycles: int = 0

    async def _ensure_client(self) -> None:
        """Create the httpx async client if not already created."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers=_DEFAULT_HEADERS,
                timeout=httpx.Timeout(30.0, connect=10.0),
                follow_redirects=True,
            )
            logger.info("BetOnline: HTTP client created")

    def start_prefetch(self) -> None:
        """Start background prefetch of all supported sports (call after event loop is running)."""
        self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    async def _prefetch_all(self) -> None:
        """Background task: continuously warm up cache for all supported sports."""
        await asyncio.sleep(8)  # Stagger startup
        logger.info("BetOnline: Starting continuous background prefetch")
        cycle = 0
        while True:
            cycle += 1
            cycle_total_events = 0
            try:
                await self._ensure_client()

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

            # Track health: if we get 0 events for 2+ consecutive cycles,
            # recreate the HTTP client in case of stale connections
            if cycle_total_events == 0:
                self._consecutive_zero_cycles += 1
                if self._consecutive_zero_cycles >= 2:
                    logger.warning(
                        "BetOnline: %d consecutive zero-event cycles — recreating HTTP client",
                        self._consecutive_zero_cycles,
                    )
                    await self._close_client()
                    self._consecutive_zero_cycles = 0
                    await asyncio.sleep(10)
            else:
                self._consecutive_zero_cycles = 0

            logger.info(
                "BetOnline: Prefetch cycle #%d complete (%d total events)",
                cycle, cycle_total_events,
            )
            await asyncio.sleep(15)  # Keep cache warm — 17 sports × 0.5s ≈ 9s + 15s pause

    # ------------------------------------------------------------------
    # Core API fetch method (direct HTTP)
    # ------------------------------------------------------------------

    async def _fetch_sport_api(
        self, sport_key: str, sport: str, league: str
    ) -> List[OddsEvent]:
        """Fetch odds for one sport via direct HTTP POST to the API.

        For basketball/football, also fetches 1st-half and 1st-quarter
        lines via the Period parameter.
        """
        # Fetch full-game lines
        full_game_data = await self._api_call(sport, league)
        if full_game_data is None:
            return []

        events = self._parse_offering(full_game_data, sport_key)

        # Fetch period markets for applicable sports
        if sport_key in _PERIOD_SPORTS and events:
            # 1st Half (Period=1)
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

        return events

    async def _api_call(
        self, sport: str, league: str, period: Optional[int] = None
    ) -> Optional[dict]:
        """Make a single API call via HTTP POST.

        Returns the parsed JSON or None on failure. Retries up to _MAX_RETRIES
        times on transient failures.
        """
        await self._ensure_client()
        if self._client is None:
            return None

        payload: dict = {
            "Sport": sport,
            "League": league,
            "ScheduleText": None,
            "filterTime": 0,
        }
        if period is not None:
            payload["Period"] = period

        extra_headers = {
            "Content-Type": "application/json",
            "gsetting": "bolsassite",
            "utc-offset": "300",
        }

        last_error = None
        for attempt in range(_MAX_RETRIES + 1):
            try:
                resp = await self._client.post(
                    API_URL,
                    json=payload,
                    headers=extra_headers,
                )

                if resp.status_code == 403:
                    logger.warning(
                        "BetOnline: API returned 403 for %s/%s (attempt %d)",
                        sport, league, attempt + 1,
                    )
                    last_error = "403"
                    if attempt < _MAX_RETRIES:
                        await asyncio.sleep(1 * (attempt + 1))
                    continue

                if resp.status_code != 200:
                    logger.debug(
                        "BetOnline: API returned %d for %s/%s",
                        resp.status_code, sport, league,
                    )
                    last_error = str(resp.status_code)
                    if attempt < _MAX_RETRIES:
                        await asyncio.sleep(1 * (attempt + 1))
                    continue

                result = resp.json()
                if not isinstance(result, dict):
                    return None

                return result

            except (httpx.TimeoutException, httpx.TransportError) as e:
                last_error = str(e)
                logger.debug(
                    "BetOnline: API call error for %s/%s (attempt %d): %s",
                    sport, league, attempt + 1, e,
                )
                if attempt < _MAX_RETRIES:
                    await asyncio.sleep(1 * (attempt + 1))
                continue
            except Exception as e:
                logger.warning("BetOnline: API call error for %s/%s: %s", sport, league, e)
                return None

        if last_error:
            logger.warning(
                "BetOnline: API call failed after %d attempts for %s/%s: %s",
                _MAX_RETRIES + 1, sport, league, last_error,
            )
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
    # Player props (via direct HTTP)
    # ------------------------------------------------------------------

    async def get_player_props(
        self, sport_key: str, event_id: str
    ) -> List[PlayerProp]:
        """Fetch player props from BetOnline's prop offering API.

        Uses the same offering-by-league endpoint but looks for prop-style
        game descriptions in the response.
        """
        cache_key = "props:%s:%s" % (sport_key, event_id)
        cached = self._props_cache.get(cache_key)
        if cached and (time.time() - cached[1]) < _CACHE_TTL:
            return cached[0]

        url_path = BETONLINE_SPORT_URLS.get(sport_key)
        if not url_path:
            return []

        api_params = BETONLINE_API_PARAMS.get(sport_key)
        if not api_params:
            return []

        sport, league = api_params
        sport_url_base = "%s/%s" % (SITE_URL, url_path)

        try:
            await self._ensure_client()
            data = await self._api_call(sport, league)
            if not data:
                return []

            props = self._parse_props_from_offering(data, sport_url_base)

            self._props_cache[cache_key] = (props, time.time())
            logger.info(
                "BetOnline: %d props for %s / %s",
                len(props), sport_key, event_id,
            )
            return props

        except Exception as e:
            logger.warning("BetOnline: Props failed for %s: %s", sport_key, e)
            return []

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
    # HTTP client lifecycle
    # ------------------------------------------------------------------

    async def _close_client(self) -> None:
        """Close and reset the HTTP client for recovery."""
        logger.info("BetOnline: Closing HTTP client for restart")
        try:
            if self._client:
                await self._client.aclose()
        except Exception as e:
            logger.debug("BetOnline: Error closing HTTP client: %s", e)
        finally:
            self._client = None

    async def close(self) -> None:
        """Shut down the HTTP client."""
        await self._close_client()
