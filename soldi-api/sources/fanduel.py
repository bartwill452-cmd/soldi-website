"""
FanDuel sportsbook scraper.
Uses FanDuel's public-facing sbapi to fetch live odds.
"""

import asyncio
import logging
import re
from typing import Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome, PlayerProp
from sources.base import DataSource
from market_keys import classify_market_type, detect_period_suffix, classify_base_market
from sources.sport_mapping import (
    FANDUEL_EVENT_TYPE_IDS,
    FANDUEL_PAGE_IDS,
    canonical_event_id,
    decimal_to_american,
    get_sport_title,
    resolve_team_name,
)

# FanDuel market type → (stat_type, threshold)
# Pattern: TO_SCORE_25+_POINTS → ("points", 25)
FANDUEL_PROP_PATTERNS = [
    (re.compile(r"TO_SCORE_(\d+)\+_POINTS$"), "points"),
    (re.compile(r"TO_RECORD_(\d+)\+_REBOUNDS$"), "rebounds"),
    (re.compile(r"TO_RECORD_(\d+)\+_ASSISTS$"), "assists"),
    (re.compile(r"(\d+)\+_MADE_THREES$"), "threes"),
    (re.compile(r"TO_RECORD_(\d+)\+_PTS_\+_REB_\+_AST$"), "pts_reb_ast"),
    (re.compile(r"TO_RECORD_(\d+)\+_PTS_\+_REB$"), "pts_reb"),
    (re.compile(r"TO_RECORD_(\d+)\+_PTS_\+_AST$"), "pts_ast"),
    (re.compile(r"TO_RECORD_(\d+)\+_REB_\+_AST$"), "reb_ast"),
    (re.compile(r"TO_RECORD_(\d+)\+_STEALS$"), "steals"),
    (re.compile(r"TO_RECORD_(\d+)\+_BLOCKS$"), "blocks"),
]

logger = logging.getLogger(__name__)

BASE_URL = "https://sbapi.ky.sportsbook.fanduel.com/api/content-managed-page"
API_KEY = "FhMFpcPWXMeyZxOx"

# Sport key → FanDuel URL path segment for event deep-links
FANDUEL_SPORT_SLUGS = {
    "basketball_nba": "basketball/nba",
    "americanfootball_nfl": "football/nfl",
    "icehockey_nhl": "hockey/nhl",
    "baseball_mlb": "baseball/mlb",
    "basketball_ncaab": "basketball/college-basketball",
    "americanfootball_ncaaf": "football/college-football",
    "mma_mixed_martial_arts": "mma/ufc",
}


class FanDuelSource(DataSource):
    """Fetches odds directly from FanDuel's sportsbook API."""

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=10.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
            },
        )
        # Cache: sport_key → (raw_data, event_id_to_canonical_id, event_id_to_url)
        self._last_data: Dict[str, tuple] = {}
        # Lazy semaphore — created on first use in the current event loop
        self._sem_obj: Optional[asyncio.Semaphore] = None
        self._sem_loop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def _api_sem(self) -> asyncio.Semaphore:
        """Lazily create or re-create the API semaphore for the current event loop.

        asyncio.Semaphore is bound to the event loop that was running when it
        was created.  If the loop changes (e.g. uvicorn restarts or the
        background refresh task runs in a different loop context), we must
        create a fresh semaphore to avoid 'Future attached to a different loop'.
        """
        loop = asyncio.get_running_loop()
        if self._sem_obj is None or self._sem_loop is not loop:
            self._sem_obj = asyncio.Semaphore(20)
            self._sem_loop = loop
        return self._sem_obj

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        # Skip if bookmakers filter excludes FanDuel
        if bookmakers and "fanduel" not in bookmakers:
            return [], {"x-requests-remaining": "unlimited"}

        page_id = FANDUEL_PAGE_IDS.get(sport_key)
        event_type_id = FANDUEL_EVENT_TYPE_IDS.get(sport_key)
        if page_id is None and event_type_id is None:
            return [], {"x-requests-remaining": "unlimited"}

        try:
            params = {
                "_ak": API_KEY,
                "betexRegion": "GBR",
                "capiJurisdiction": "intl",
                "currencyCode": "USD",
                "exchangeLocale": "en_US",
                "language": "en",
                "regionCode": "NAMERICA",
            }
            if page_id:
                params["page"] = "CUSTOM"
                params["customPageId"] = page_id
            else:
                # Sports like MMA use page=SPORT with eventTypeId
                params["page"] = "SPORT"
                params["eventTypeId"] = str(event_type_id)

            # Initial fetch does NOT go through the semaphore — only enrichment
            # does.  The semaphore prevents enrichment from flooding the API when
            # all 5 sports refresh concurrently, but the initial fetch is just
            # one call per sport and must not be blocked by enrichment traffic.
            response = await self._client.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            events, id_map, url_map = self._parse_response(data, sport_key)
            # Cache for player props lookup
            self._last_data[sport_key] = (data, id_map, url_map)
            logger.info(f"FanDuel: {len(events)} events for {sport_key}")

            # Enrichment steps (period markets, team totals) make many per-event
            # API calls.  Allow generous timeout so all sub-markets are captured.
            try:
                await asyncio.wait_for(
                    self._enrich_all(events, id_map, sport_key), timeout=12.0,
                )
            except asyncio.TimeoutError:
                logger.info(f"FanDuel: enrichment timed out (12s) for {sport_key}, returning base markets")

            return events, {"x-requests-remaining": "unlimited"}

        except Exception as e:
            logger.warning(f"FanDuel failed for {sport_key}: {type(e).__name__}: {e!r}")
            return [], {"x-requests-remaining": "unlimited"}

    async def _enrich_all(
        self, events: List[OddsEvent], id_map: Dict[str, str], sport_key: str
    ) -> None:
        """Run all enrichment steps (period markets, team totals, MMA markets)."""
        await self._enrich_period_markets(events, id_map, sport_key)
        await self._enrich_team_totals(events, id_map, sport_key)
        if sport_key in ("mma_mixed_martial_arts", "boxing_boxing"):
            await self._enrich_mma_markets(events, id_map, sport_key)

    # ---- Period market enrichment (1H, Q1, etc.) from event detail pages ----

    # FanDuel event-page tabs that contain period markets we want (sport-specific)
    # Sports with no period tabs (MMA, boxing) use empty lists to skip enrichment.
    _SPORT_PERIOD_TABS = {
        "basketball_nba": ["half", "1st-quarter", "2nd-quarter", "3rd-quarter", "4th-quarter"],
        "basketball_ncaab": ["half"],
        "icehockey_nhl": ["1st-period", "2nd-period", "3rd-period"],
        "baseball_mlb": ["1st-inning", "first-5-innings", "first-7-innings"],
        "americanfootball_nfl": ["half", "1st-quarter", "2nd-quarter", "3rd-quarter", "4th-quarter"],
        "americanfootball_ncaaf": ["half", "1st-quarter"],
        "mma_mixed_martial_arts": [],  # MMA fights have no halves/quarters
        "boxing_boxing": [],  # Boxing has no halves/quarters
    }
    # Fallback for sports not listed
    _PERIOD_TABS = ["half", "1st-quarter", "2nd-quarter", "3rd-quarter", "4th-quarter"]

    async def _enrich_period_markets(
        self, events: List[OddsEvent], id_map: Dict[str, str], sport_key: str
    ) -> None:
        """Fetch 1H/quarter markets from FanDuel event detail pages and merge in-place.

        Makes concurrent per-event API calls with tab=half and tab=1st-quarter
        to retrieve period-specific markets (1H ML/Spread/Total, Q1 ML/Spread/Total).
        """
        if not events:
            return

        # Build reverse map: canonical_id → (fd_event_id, OddsEvent)
        cid_to_fd_id = {}  # type: Dict[str, str]
        for fd_id, cid in id_map.items():
            cid_to_fd_id[cid] = fd_id

        event_by_cid = {}  # type: Dict[str, OddsEvent]
        for ev in events:
            event_by_cid[ev.id] = ev

        period_tabs = self._SPORT_PERIOD_TABS.get(sport_key, self._PERIOD_TABS)
        logger.info(f"FanDuel: starting period enrichment for {sport_key} ({len(events)} events, tabs={period_tabs}, {len(cid_to_fd_id)} mapped)")
        sem = asyncio.Semaphore(8)

        async def fetch_period_for_event(ev: OddsEvent) -> None:
            fd_id = cid_to_fd_id.get(ev.id)
            if not fd_id:
                return
            async with sem:
                for tab in period_tabs:
                    try:
                        period_markets = await self._fetch_event_tab(fd_id, tab)
                        if period_markets:
                            # Get the FanDuel bookmaker from this event
                            fd_bm = None
                            for bm in ev.bookmakers:
                                if bm.key == "fanduel":
                                    fd_bm = bm
                                    break
                            if fd_bm:
                                existing_keys = {m.key for m in fd_bm.markets}
                                for mkt in period_markets:
                                    if mkt.key not in existing_keys:
                                        fd_bm.markets.append(mkt)
                                        existing_keys.add(mkt.key)
                    except Exception as exc:
                        logger.warning(f"FanDuel: period tab={tab} event={fd_id} error: {type(exc).__name__}: {exc}")

        tasks = [fetch_period_for_event(ev) for ev in events]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log any exceptions that occurred during enrichment
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                logger.warning(f"FanDuel: period enrichment error for {sport_key} event {i}: {r}")

        # Count how many period markets were added
        period_count = sum(
            1 for ev in events
            for bm in ev.bookmakers if bm.key == "fanduel"
            for m in bm.markets if "_" in m.key and m.key not in ("h2h", "spreads", "totals")
            and not m.key.startswith("player_")
        )
        if period_count:
            logger.info(f"FanDuel: enriched {period_count} period markets for {sport_key}")

    # Mapping from FanDuel tab slug → market key period suffix
    _TAB_TO_SUFFIX = {
        "half": "_h1",
        "1st-quarter": "_q1",
        "2nd-quarter": "_q2",
        "3rd-quarter": "_q3",
        "4th-quarter": "_q4",
        "1st-period": "_p1",
        "2nd-period": "_p2",
        "3rd-period": "_p3",
        "1st-inning": "_i1",
        "first-5-innings": "_f5",
        "first-7-innings": "_f7",
    }

    async def _fetch_event_tab(self, fd_event_id: str, tab: str) -> List[Market]:
        """Fetch markets for a single FanDuel event detail tab (e.g. 'half', '1st-quarter')."""
        params = {
            "_ak": API_KEY,
            "betexRegion": "GBR",
            "capiJurisdiction": "intl",
            "currencyCode": "USD",
            "exchangeLocale": "en_US",
            "language": "en",
            "eventId": fd_event_id,
            "tab": tab,
        }
        async with self._api_sem:
            resp = await self._client.get(
                "https://sbapi.ky.sportsbook.fanduel.com/api/event-page", params=params
            )
        if resp.status_code != 200:
            logger.info(f"FanDuel: tab={tab} event={fd_event_id} returned HTTP {resp.status_code}")
            return []

        data = resp.json()
        raw_markets = data.get("attachments", {}).get("markets", {})
        if not raw_markets:
            logger.info(f"FanDuel: tab={tab} event={fd_event_id} returned no markets")
            return []

        # Determine the period suffix for this tab (used as fallback when
        # FanDuel returns generic market types like MONEY_LINE instead of
        # period-specific ones like 1ST_HALF_WINNER)
        tab_suffix = self._TAB_TO_SUFFIX.get(tab, "")

        result = []
        seen_keys = set()  # type: set
        for market_id, market_data in raw_markets.items():
            market_type = market_data.get("marketType", "")
            runners = market_data.get("runners", [])

            # Skip alternate/exotic markets — they have many runners with handicap=0
            upper_mt = market_type.upper()
            if "ALTERNATE" in upper_mt or "ALT_TOTAL" in upper_mt:
                continue
            # Skip team totals, margins, doubles, 3-way, etc.
            if any(x in upper_mt for x in ("TEAM_TOTAL", "HOME_TEAM_TOTAL", "AWAY_TEAM_TOTAL",
                                            "MARGIN", "DOUBLE", "3-WAY", "3_WAY",
                                            "MATCH_BETTING_(3-WAY)")):
                continue
            # Skip race-to markets and combo markets
            if any(x in upper_mt for x in ("RACE_TO", "LINE_/_TOTAL", "WINNER_/_")):
                continue
            # Skip ODD/EVEN, SCORE_FIRST, SCORE_LAST markets
            if any(x in upper_mt for x in ("ODD/EVEN", "SCORE_FIRST", "SCORE_LAST")):
                continue

            market_key = classify_market_type(market_type)
            if market_key is None:
                logger.debug(f"FanDuel: tab={tab} unclassified market_type: {market_type}")
                continue

            # If the classified key is a base key (no period suffix) but we're
            # on a period tab, append the tab's period suffix.
            # This handles NBA/NFL where FanDuel returns generic types like
            # MONEY_LINE instead of 1ST_HALF_WINNER on period tabs.
            if market_key in ("h2h", "spreads", "totals") and tab_suffix:
                market_key = market_key + tab_suffix

            # Only keep period markets (skip full-game, props, etc.)
            if "_" not in market_key or market_key.startswith("player_"):
                continue

            # Must be a known period suffix
            base_part = market_key.split("_")[0]
            if base_part not in ("h2h", "spreads", "totals"):
                continue

            if market_key in seen_keys:
                continue
            seen_keys.add(market_key)

            if base_part == "h2h":
                outcomes = self._parse_moneyline(runners, "", "")
                if outcomes:
                    result.append(Market(key=market_key, outcomes=outcomes))
            elif base_part == "spreads":
                outcomes = self._parse_spread(runners)
                if outcomes:
                    result.append(Market(key=market_key, outcomes=outcomes))
            elif base_part == "totals":
                outcomes = self._parse_totals(runners)
                if outcomes:
                    result.append(Market(key=market_key, outcomes=outcomes))

        if result:
            logger.info(f"FanDuel: tab={tab} returned {len(result)} period markets: {[m.key for m in result]}")

        return result

    # ---- Team totals enrichment from event detail pages ----

    # FanDuel tab names to try for team totals (varies by sport)
    _TEAM_TOTAL_TABS = ["team-totals", "team-props"]

    async def _enrich_team_totals(
        self, events: List[OddsEvent], id_map: Dict[str, str], sport_key: str
    ) -> None:
        """Fetch team total markets from FanDuel event detail pages."""
        if not events:
            return

        cid_to_fd_id = {}  # type: Dict[str, str]
        for fd_id, cid in id_map.items():
            cid_to_fd_id[cid] = fd_id

        sem = asyncio.Semaphore(8)

        async def fetch_tt_for_event(ev: OddsEvent) -> None:
            fd_id = cid_to_fd_id.get(ev.id)
            if not fd_id:
                return
            async with sem:
                for tab in self._TEAM_TOTAL_TABS:
                    try:
                        tt_markets = await self._fetch_team_totals(fd_id, tab)
                        if tt_markets:
                            fd_bm = None
                            for bm in ev.bookmakers:
                                if bm.key == "fanduel":
                                    fd_bm = bm
                                    break
                            if fd_bm:
                                existing_keys = {m.key for m in fd_bm.markets}
                                for mkt in tt_markets:
                                    if mkt.key not in existing_keys:
                                        fd_bm.markets.append(mkt)
                                        existing_keys.add(mkt.key)
                            break  # Found team totals in this tab, no need to try others
                    except Exception:
                        pass

        tasks = [fetch_tt_for_event(ev) for ev in events]
        await asyncio.gather(*tasks, return_exceptions=True)

        tt_count = sum(
            1 for ev in events
            for bm in ev.bookmakers if bm.key == "fanduel"
            for m in bm.markets if m.key.startswith("team_total")
        )
        if tt_count:
            logger.info(f"FanDuel: enriched {tt_count} team total markets for {sport_key}")

    async def _fetch_team_totals(self, fd_event_id: str, tab: str) -> List[Market]:
        """Fetch team total markets from a FanDuel event detail tab."""
        params = {
            "_ak": API_KEY,
            "betexRegion": "GBR",
            "capiJurisdiction": "intl",
            "currencyCode": "USD",
            "exchangeLocale": "en_US",
            "language": "en",
            "eventId": fd_event_id,
            "tab": tab,
        }
        async with self._api_sem:
            resp = await self._client.get(
                "https://sbapi.ky.sportsbook.fanduel.com/api/event-page", params=params
            )
        if resp.status_code != 200:
            return []

        data = resp.json()
        raw_markets = data.get("attachments", {}).get("markets", {})
        if not raw_markets:
            return []

        result = []  # type: List[Market]
        seen_keys = set()  # type: set
        for market_id, market_data in raw_markets.items():
            market_type = market_data.get("marketType", "")
            runners = market_data.get("runners", [])
            upper_mt = market_type.upper()

            # Only want team total markets
            if "TEAM_TOTAL" not in upper_mt and "HOME_TEAM_TOTAL" not in upper_mt and "AWAY_TEAM_TOTAL" not in upper_mt:
                continue
            # Skip alternates
            if "ALTERNATE" in upper_mt or "ALT_TOTAL" in upper_mt:
                continue

            market_key = classify_market_type(market_type)
            if market_key is None or not market_key.startswith("team_total"):
                continue

            if market_key in seen_keys:
                continue
            seen_keys.add(market_key)

            outcomes = self._parse_totals(runners)
            if outcomes:
                result.append(Market(key=market_key, outcomes=outcomes))

        return result

    # ---- MMA market enrichment (totals/rounds, fight to go distance) --------

    # FanDuel MMA market types we want from event detail pages
    _MMA_MARKET_TYPES = {
        "TOTAL_ROUNDS": "totals",
        "WILL_THE_FIGHT_GO_THE_DISTANCE?": "fight_to_go_distance",
    }

    async def _enrich_mma_markets(
        self, events: List[OddsEvent], id_map: Dict[str, str], sport_key: str
    ) -> None:
        """Fetch MMA-specific markets (round totals, distance) from event detail pages.

        FanDuel's MMA listing only returns MATCH_BETTING (moneyline).
        The event detail "popular" tab has TOTAL_ROUNDS and WILL_THE_FIGHT_GO_THE_DISTANCE?.
        """
        if not events:
            return

        cid_to_fd_id = {}  # type: Dict[str, str]
        for fd_id, cid in id_map.items():
            cid_to_fd_id[cid] = fd_id

        sem = asyncio.Semaphore(8)

        async def fetch_mma_for_event(ev: OddsEvent) -> None:
            fd_id = cid_to_fd_id.get(ev.id)
            if not fd_id:
                return
            async with sem:
                try:
                    mma_markets = await self._fetch_mma_event_markets(fd_id)
                    if mma_markets:
                        fd_bm = None
                        for bm in ev.bookmakers:
                            if bm.key == "fanduel":
                                fd_bm = bm
                                break
                        if fd_bm:
                            existing_keys = {m.key for m in fd_bm.markets}
                            for mkt in mma_markets:
                                if mkt.key not in existing_keys:
                                    fd_bm.markets.append(mkt)
                                    existing_keys.add(mkt.key)
                except Exception:
                    pass

        tasks = [fetch_mma_for_event(ev) for ev in events]
        await asyncio.gather(*tasks, return_exceptions=True)

        mma_count = sum(
            1 for ev in events
            for bm in ev.bookmakers if bm.key == "fanduel"
            for m in bm.markets if m.key in ("totals", "fight_to_go_distance")
        )
        if mma_count:
            logger.info(f"FanDuel: enriched {mma_count} MMA markets for {sport_key}")

    async def _fetch_mma_event_markets(self, fd_event_id: str) -> List[Market]:
        """Fetch MMA-specific markets from a FanDuel event detail page (popular tab)."""
        params = {
            "_ak": API_KEY,
            "betexRegion": "GBR",
            "capiJurisdiction": "intl",
            "currencyCode": "USD",
            "exchangeLocale": "en_US",
            "language": "en",
            "eventId": fd_event_id,
            "tab": "popular",
        }
        async with self._api_sem:
            resp = await self._client.get(
                "https://sbapi.ky.sportsbook.fanduel.com/api/event-page", params=params
            )
        if resp.status_code != 200:
            return []

        data = resp.json()
        raw_markets = data.get("attachments", {}).get("markets", {})
        if not raw_markets:
            return []

        result = []  # type: List[Market]
        for market_id, market_data in raw_markets.items():
            market_type = market_data.get("marketType", "")
            runners = market_data.get("runners", [])

            market_key = self._MMA_MARKET_TYPES.get(market_type)
            if market_key is None:
                continue

            if market_key == "totals":
                outcomes = self._parse_totals(runners)
                if outcomes:
                    result.append(Market(key="totals", outcomes=outcomes))
            elif market_key == "fight_to_go_distance":
                outcomes = self._parse_yes_no(runners)
                if outcomes:
                    result.append(Market(key="fight_to_go_distance", outcomes=outcomes))

        return result

    def _parse_response(self, data: dict, sport_key: str) -> Tuple[List[OddsEvent], Dict[str, str], Dict[str, str]]:
        events = []
        # Maps: FanDuel eventId → canonical event ID, FanDuel eventId → event URL
        id_map: Dict[str, str] = {}
        url_map: Dict[str, str] = {}
        attachments = data.get("attachments", {})

        raw_events = attachments.get("events", {})
        raw_markets = attachments.get("markets", {})

        # Build a map: eventId -> list of market objects
        event_markets_map = {}  # type: Dict[str, List[dict]]
        for market_id, market_data in raw_markets.items():
            event_id = str(market_data.get("eventId", ""))
            if event_id:
                if event_id not in event_markets_map:
                    event_markets_map[event_id] = []
                event_markets_map[event_id].append(market_data)

        sport_title = get_sport_title(sport_key)

        for event_id, ev in raw_events.items():
            name = ev.get("name", "")
            home_team = ""
            away_team = ""

            # Try competitors array first
            competitors = ev.get("competitors", [])
            if competitors and len(competitors) >= 2:
                for comp in competitors:
                    if comp.get("home", False):
                        home_team = comp.get("name", "")
                    else:
                        away_team = comp.get("name", "")

            # Fall back to parsing the event name
            if not home_team or not away_team:
                if " @ " in name:
                    parts = name.split(" @ ")
                    away_team = parts[0].strip()
                    home_team = parts[1].strip()
                elif " v " in name:
                    parts = name.split(" v ")
                    home_team = parts[0].strip()
                    away_team = parts[1].strip()
                elif " vs " in name.lower():
                    idx = name.lower().index(" vs ")
                    home_team = name[:idx].strip()
                    away_team = name[idx + 4:].strip()

            if not home_team or not away_team:
                continue

            # Resolve team name aliases for clean display
            home_team = resolve_team_name(home_team)
            away_team = resolve_team_name(away_team)

            # Skip women's basketball games mixed into NCAAB/NBA feeds
            if "(W)" in home_team or "(W)" in away_team:
                continue

            commence_time = ev.get("openDate", "")

            # Find markets for this event via the eventId mapping
            fd_markets = []
            market_list = event_markets_map.get(str(event_id), [])

            seen_market_keys = set()  # type: set
            for market_data in market_list:
                market_type = market_data.get("marketType", "")
                runners = market_data.get("runners", [])

                # Use the market key taxonomy to classify this market
                market_key = classify_market_type(market_type)

                if market_key is None:
                    # Fallback: try direct matching for legacy compat
                    if market_type in ("MATCH_BETTING", "MONEY_LINE", "MONEYLINE"):
                        market_key = "h2h"
                    elif market_type in ("HANDICAP", "SPREAD", "MATCH_HANDICAP", "MATCH_HANDICAP_(2-WAY)"):
                        market_key = "spreads"
                    elif market_type in ("TOTAL_POINTS", "TOTAL", "OVER_UNDER", "TOTAL_POINTS_(OVER/UNDER)"):
                        market_key = "totals"

                if market_key is None:
                    continue

                # Skip duplicate market keys for the same event
                if market_key in seen_market_keys:
                    continue
                seen_market_keys.add(market_key)

                # Player props: parse as over/under outcomes
                if market_key.startswith("player_"):
                    outcomes = self._parse_player_prop_market(runners)
                    if outcomes:
                        fd_markets.append(Market(key=market_key, outcomes=outcomes))
                    continue

                # Team totals: parse as totals (Over/Under with point)
                if market_key.startswith("team_total"):
                    outcomes = self._parse_totals(runners)
                    if outcomes:
                        fd_markets.append(Market(key=market_key, outcomes=outcomes))
                    continue

                # Fight to go the distance (Yes/No) — MMA/Boxing
                if market_key == "fight_to_go_distance":
                    outcomes = self._parse_yes_no(runners)
                    if outcomes:
                        fd_markets.append(Market(key=market_key, outcomes=outcomes))
                    continue

                # Determine the base type for parsing
                base_type = market_key.split("_")[0] if "_" in market_key else market_key
                # For keys like "h2h_q1", base is "h2h"; for "spreads_h1", base is "spreads"
                if market_key in ("h2h", "spreads", "totals"):
                    base_type = market_key
                elif market_key.startswith("h2h"):
                    base_type = "h2h"
                elif market_key.startswith("spreads"):
                    base_type = "spreads"
                elif market_key.startswith("totals"):
                    base_type = "totals"

                if base_type == "h2h":
                    outcomes = self._parse_moneyline(runners, home_team, away_team)
                    if outcomes:
                        fd_markets.append(Market(key=market_key, outcomes=outcomes))
                elif base_type == "spreads":
                    outcomes = self._parse_spread(runners)
                    if outcomes:
                        fd_markets.append(Market(key=market_key, outcomes=outcomes))
                elif base_type == "totals":
                    outcomes = self._parse_totals(runners)
                    if outcomes:
                        fd_markets.append(Market(key=market_key, outcomes=outcomes))

            if not fd_markets:
                continue

            # Build event deep-link URL
            event_url = self._build_event_url(sport_key, name, event_id)

            cid = canonical_event_id(sport_key, home_team, away_team, commence_time)
            id_map[str(event_id)] = cid
            if event_url:
                url_map[str(event_id)] = event_url

            events.append(OddsEvent(
                id=cid,
                sport_key=sport_key,
                sport_title=sport_title,
                commence_time=commence_time,
                home_team=home_team,
                away_team=away_team,
                bookmakers=[
                    Bookmaker(key="fanduel", title="FanDuel", markets=fd_markets, event_url=event_url)
                ],
            ))

        return events, id_map, url_map

    # FanDuel event-page tabs for O/U player props
    _PROP_TABS = ["player-points", "player-rebounds", "player-assists", "player-threes", "player-combos"]

    # Map FanDuel market type patterns → stat_type for O/U props
    _OU_MARKET_TYPE_MAP = {
        "TOTAL_POINTS": "points",
        "TOTAL_REBOUNDS": "rebounds",
        "TOTAL_ASSISTS": "assists",
        "TOTAL_MADE_3_POINT_FIELD_GOALS": "threes",
        "TOTAL_POINTS_+_REB_+_AST": "pts_reb_ast",
        "TOTAL_POINTS_+_REBOUNDS": "pts_reb",
        "TOTAL_POINTS_+_ASSISTS": "pts_ast",
        "TOTAL_REBOUNDS_+_ASSISTS": "reb_ast",
        "TOTAL_STEALS": "steals",
        "TOTAL_BLOCKS": "blocks",
        "1ST_QUARTER_POINTS": "points_q1",
    }

    async def get_player_props(self, sport_key: str, event_id: str) -> List[PlayerProp]:
        """Fetch player O/U props from FanDuel event detail tabs."""
        cached = self._last_data.get(sport_key)
        if not cached:
            return []

        data, id_map, url_map = cached

        # Find the FanDuel event ID for this canonical event ID
        fd_event_id = None
        for fid, cid in id_map.items():
            if cid == event_id:
                fd_event_id = fid
                break
        if not fd_event_id:
            return []

        event_url = url_map.get(fd_event_id)

        # Fetch all prop tabs concurrently
        sem = asyncio.Semaphore(3)
        async def fetch_tab(tab):
            async with sem:
                return await self._fetch_prop_tab(fd_event_id, tab)

        tasks = [fetch_tab(tab) for tab in self._PROP_TABS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        props: List[PlayerProp] = []
        seen = set()  # type: set  # Deduplicate: (player, stat_type, line, desc)

        for result in results:
            if isinstance(result, Exception) or result is None:
                continue
            for market_data in result:
                market_type = market_data.get("marketType", "")
                stat_type = self._classify_ou_market(market_type)
                if not stat_type:
                    continue

                runners = market_data.get("runners", [])
                for runner in runners:
                    runner_name = runner.get("runnerName", "")
                    handicap = runner.get("handicap")
                    result_type = runner.get("result", {}).get("type", "")
                    price = self._extract_price(runner)

                    if not runner_name or handicap is None or price is None:
                        continue

                    try:
                        line = float(handicap)
                    except (ValueError, TypeError):
                        continue

                    if line <= 0:
                        continue

                    # Determine Over/Under from result.type or runner name
                    if result_type == "OVER" or "over" in runner_name.lower():
                        desc = "Over"
                    elif result_type == "UNDER" or "under" in runner_name.lower():
                        desc = "Under"
                    else:
                        continue

                    # Extract clean player name (strip " Over" / " Under")
                    player_name = runner_name
                    for suffix in (" Over", " Under"):
                        if player_name.endswith(suffix):
                            player_name = player_name[:-len(suffix)]
                            break

                    key = (player_name, stat_type, line, desc)
                    if key in seen:
                        continue
                    seen.add(key)

                    props.append(PlayerProp(
                        player_name=player_name,
                        stat_type=stat_type,
                        line=line,
                        price=price,
                        description=desc,
                        bookmaker_key="fanduel",
                        bookmaker_title="FanDuel",
                        event_url=event_url,
                    ))

        return props

    async def _fetch_prop_tab(self, event_id: str, tab: str) -> Optional[List[dict]]:
        """Fetch a single FanDuel event-page prop tab and return market dicts."""
        try:
            params = {
                "_ak": API_KEY,
                "betexRegion": "GBR",
                "capiJurisdiction": "intl",
                "currencyCode": "USD",
                "exchangeLocale": "en_US",
                "language": "en",
                "regionCode": "NAMERICA",
                "eventId": event_id,
                "tab": tab,
            }
            response = await self._client.get(
                "https://sbapi.ky.sportsbook.fanduel.com/api/event-page", params=params
            )
            if response.status_code != 200:
                return None
            data = response.json()
            markets = data.get("attachments", {}).get("markets", {})
            return [m for m in markets.values() if "PLAYER" in m.get("marketType", "")]
        except Exception:
            return None

    @staticmethod
    def _classify_ou_market(market_type: str) -> Optional[str]:
        """Classify a FanDuel PLAYER_X_TOTAL_* market type to a stat_type."""
        # Strip the PLAYER_X_ prefix (e.g., PLAYER_A_, PLAYER_B_, etc.)
        stripped = re.sub(r"^PLAYER_[A-Z]_", "", market_type)
        for pattern, stat_type in FanDuelSource._OU_MARKET_TYPE_MAP.items():
            if stripped == pattern:
                return stat_type
        return None

    @staticmethod
    def _match_prop_type(market_type: str) -> Tuple[Optional[str], float]:
        """Match a FanDuel market type to a (stat_type, threshold) pair."""
        for pattern, stat_type in FANDUEL_PROP_PATTERNS:
            m = pattern.search(market_type)
            if m:
                return stat_type, float(m.group(1))
        return None, 0.0

    def _build_event_url(self, sport_key: str, event_name: str, event_id: str) -> Optional[str]:
        """Build a FanDuel event page URL from the event name and ID."""
        sport_slug = FANDUEL_SPORT_SLUGS.get(sport_key)
        if not sport_slug:
            return None
        # Slugify event name: "Cleveland Cavaliers @ Oklahoma City Thunder" → "cleveland-cavaliers-@-oklahoma-city-thunder"
        slug = re.sub(r"[^a-z0-9@]+", "-", event_name.lower()).strip("-")
        return f"https://sportsbook.fanduel.com/{sport_slug}/{slug}-{event_id}"

    def _extract_price(self, runner: dict) -> Optional[int]:
        """Extract American odds price from a FanDuel runner, trying multiple paths."""
        win_price = runner.get("winRunnerOdds", {})

        # Path 1: americanOddsInt (integer)
        american_int = win_price.get("americanDisplayOdds", {}).get("americanOddsInt")
        if american_int is not None:
            try:
                return int(american_int)
            except (ValueError, TypeError):
                pass

        # Path 2: americanOdds (string like "+150" or "-110")
        american_str = win_price.get("americanDisplayOdds", {}).get("americanOdds")
        if american_str is not None:
            try:
                return int(float(str(american_str).replace("+", "")))
            except (ValueError, TypeError):
                pass

        # Path 3: decimal odds conversion
        decimal_odds = win_price.get("decimalDisplayOdds", {}).get("decimalOdds")
        if decimal_odds is not None:
            try:
                return decimal_to_american(float(decimal_odds))
            except (ValueError, TypeError):
                pass

        # Path 4: trueOdds decimal
        true_odds = win_price.get("trueOdds", {}).get("decimalOdds", {}).get("decimalOdds")
        if true_odds is not None:
            try:
                return decimal_to_american(float(true_odds))
            except (ValueError, TypeError):
                pass

        return None

    def _parse_player_prop_market(self, runners: list) -> List[Outcome]:
        """Parse player prop runners into Over/Under outcomes with player name and line."""
        result = []
        for runner in runners:
            name = runner.get("runnerName", "")
            handicap = runner.get("handicap", 0)
            price = self._extract_price(runner)
            if price is None:
                continue
            try:
                point = float(handicap) if handicap else 0.0
            except (ValueError, TypeError):
                point = 0.0
            # Normalize Over/Under names
            lower_name = name.lower()
            if "over" in lower_name:
                display_name = name  # Keep player name context
            elif "under" in lower_name:
                display_name = name
            else:
                display_name = name
            result.append(Outcome(name=display_name, price=price, point=point))
        return result

    def _parse_moneyline(self, runners: list, home: str, away: str) -> List[Outcome]:
        result = []
        for runner in runners:
            name = runner.get("runnerName", "")
            price = self._extract_price(runner)
            if price is not None:
                result.append(Outcome(name=name, price=price))
        return result

    def _parse_spread(self, runners: list) -> List[Outcome]:
        result = []
        for runner in runners:
            name = runner.get("runnerName", "")
            handicap = runner.get("handicap", 0)
            price = self._extract_price(runner)
            if price is None:
                continue

            try:
                point = float(handicap)
            except (ValueError, TypeError):
                point = 0.0

            result.append(Outcome(name=name, price=price, point=point))
        return result

    def _parse_totals(self, runners: list) -> List[Outcome]:
        result = []
        for runner in runners:
            name = runner.get("runnerName", "")
            handicap = runner.get("handicap", 0)
            price = self._extract_price(runner)
            if price is None:
                continue

            try:
                point = float(handicap)
            except (ValueError, TypeError):
                point = 0.0

            # Normalize name
            if "over" in name.lower():
                name = "Over"
            elif "under" in name.lower():
                name = "Under"

            result.append(Outcome(name=name, price=price, point=point))
        return result

    def _parse_yes_no(self, runners: list) -> List[Outcome]:
        """Parse Yes/No runners (fight to go distance, etc.)."""
        result = []
        for runner in runners:
            name = runner.get("runnerName", "")
            price = self._extract_price(runner)
            if price is None:
                continue
            lower = name.lower()
            if "yes" in lower:
                result.append(Outcome(name="Yes", price=price))
            elif "no" in lower:
                result.append(Outcome(name="No", price=price))
        return result

    async def close(self) -> None:
        await self._client.aclose()
