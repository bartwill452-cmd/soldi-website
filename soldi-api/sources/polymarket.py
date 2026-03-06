"""
Polymarket prediction market scraper.
Uses Polymarket's public Gamma API for both game-level and championship futures.

Supports:
1. Pre-game markets (moneyline, spreads, totals) for upcoming games
2. Championship futures ("2026 NBA Champion", etc.)

Game events are fetched via series_id from the /sports endpoint, which
reliably returns real sports events (unlike slug_contains which is broken).
"""

import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome, ScoreData
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    normalize_team_name,
    prob_to_american,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"

# ---------- Sport key -> matching keywords in Polymarket event titles ----------
SPORT_KEYWORDS = {
    "basketball_nba": ["nba"],
    "basketball_ncaab": ["ncaab", "ncaa"],
    "basketball_wnba": ["wnba"],
    "americanfootball_nfl": ["nfl"],
    "americanfootball_ncaaf": ["ncaaf"],
    "icehockey_nhl": ["nhl"],
    "baseball_mlb": ["mlb"],
    "soccer_epl": ["english premier league", "epl", "premier league: "],
    "soccer_spain_la_liga": ["la liga"],
    "soccer_germany_bundesliga": ["bundesliga"],
    "soccer_italy_serie_a": ["serie a"],
    "soccer_france_ligue_one": ["ligue 1"],
    "soccer_uefa_champs_league": ["champions league", "ucl"],
    "mma_mixed_martial_arts": ["ufc", "mma"],
    "boxing_boxing": ["boxing"],
}

# Keywords that should EXCLUDE an event from a sport category
SPORT_EXCLUDE_KEYWORDS = {
    "soccer_epl": ["egyptian", "russia", "russian", "egypt"],
}

# Championship/futures event title patterns per sport
CHAMPIONSHIP_PATTERNS = {
    "basketball_nba": ["nba champion"],
    "icehockey_nhl": ["stanley cup champion", "nhl champion"],
    "baseball_mlb": ["world series champion", "baseball.*champion"],
    "americanfootball_nfl": ["super bowl", "nfl champion"],
    "soccer_epl": ["premier league winner"],
    "soccer_spain_la_liga": ["la liga winner"],
    "soccer_germany_bundesliga": ["bundesliga winner"],
    "soccer_italy_serie_a": ["serie a.*winner"],
    "soccer_france_ligue_one": ["ligue 1 winner"],
    "soccer_uefa_champs_league": ["champions league winner"],
    "basketball_ncaab": ["ncaa tournament winner"],
}

# ---------- sport_key -> Polymarket series_id (from /sports endpoint) ----------
# These are the reliable IDs for fetching game-level events.
# The slug_contains param is broken in the Gamma API.
SPORT_TO_SERIES_ID = {
    "basketball_nba": 10345,
    "basketball_ncaab": 10470,      # cbb series
    "basketball_wnba": 10105,
    "americanfootball_nfl": 10187,
    "americanfootball_ncaaf": 10210, # cfb series
    "icehockey_nhl": 10346,
    "baseball_mlb": 3,
    "soccer_epl": 10188,
    "soccer_spain_la_liga": 10193,   # lal series
    "soccer_germany_bundesliga": 10194,  # bun series
    "soccer_italy_serie_a": 10203,   # sea series
    "soccer_france_ligue_one": 10195,    # fl1 series
    "soccer_usa_mls": 10189,         # mls series
    "soccer_uefa_champs_league": 10204,  # ucl series
    "mma_mixed_martial_arts": 10500, # ufc series
    "tennis_atp": 10365,             # atp series
    "tennis_wta": 10366,             # wta series
}

# ---------- sport_key -> Polymarket tag_slug (fallback for sports without series_id) -
# Used to fetch events when series_id doesn't exist for a sport.
SPORT_TO_TAG_SLUG = {
    "boxing_boxing": "boxing",
}

# Slug prefix -> sport_key (for fallback sport detection)
SLUG_PREFIX_TO_SPORT = {
    "cbb-": "basketball_ncaab",
    "nba-": "basketball_nba",
    "nhl-": "icehockey_nhl",
    "nfl-": "americanfootball_nfl",
    "mlb-": "baseball_mlb",
    "epl-": "soccer_epl",
    "ufc-": "mma_mixed_martial_arts",
}

# Reverse: sport_key -> slug prefix
SPORT_TO_SLUG_PREFIX = {v: k for k, v in SLUG_PREFIX_TO_SPORT.items()}

# Prefixes that indicate the title contains a sport prefix to strip
# Handles "UFC 326:" and "NBA:" style prefixes (optional event number)
TITLE_PREFIXES = re.compile(
    r"^(?:NBA|NFL|NHL|MLB|NCAAB|NCAAF|WNBA|UFC|MMA|EPL|Soccer)\s*\d*\s*:\s*",
    re.IGNORECASE,
)

# Pattern for "Team A vs Team B" (with optional trailing parenthetical, date, or (W))
# Handles UFC titles like "Charles Oliveira vs. Max Holloway (Lightweight, Main Card)"
GAME_PATTERN = re.compile(
    r"^(.+?)\s+vs\.?\s+(.+?)(?:\s*\([^)]*\))?(?:\s+\d{4}-\d{2}-\d{2})?$",
    re.IGNORECASE,
)

# Period values that indicate the game has ended or been cancelled
ENDED_PERIODS = {"VFT", "FT", "CAN", "POST"}


class PolymarketSource(DataSource):
    """Fetches odds from Polymarket's gamma API -- futures + game markets (pre-game)."""

    # Class-level semaphore: limit to 4 concurrent Gamma/CLOB API requests
    # across all sport refreshes to avoid overwhelming the API when all 17
    # sports refresh concurrently.
    _api_sem = asyncio.Semaphore(4)

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
            },
        )
        # Cache all sports events so we don't refetch per sport
        self._cached_events = None  # type: Optional[list]
        # Mapping: event canonical ID → (home_token_id, away_token_id) for CLOB book lookups
        self._clob_token_map = {}  # type: Dict[str, Tuple[str, str]]
        # Spread/total CLOB token maps: event canonical ID → (token_a, token_b)
        self._clob_spread_token_map = {}  # type: Dict[str, Tuple[str, str]]
        self._clob_total_token_map = {}  # type: Dict[str, Tuple[str, str]]
        # Per-sport cache: sport_key → (events, timestamp)
        import time as _time
        self._sport_cache = {}  # type: Dict[str, Tuple[List[OddsEvent], float]]
        self._sport_cache_ttl = 30  # seconds — don't refetch from Gamma every 5s cycle

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        if bookmakers and "polymarket" not in bookmakers:
            return [], {"x-requests-remaining": "unlimited"}

        keywords = SPORT_KEYWORDS.get(sport_key)
        if not keywords:
            return [], {"x-requests-remaining": "unlimited"}

        # Return cached data if fresh (avoid refetching every 5s cycle)
        import time as _time
        cached = self._sport_cache.get(sport_key)
        if cached:
            events, ts = cached
            if (_time.time() - ts) < self._sport_cache_ttl:
                return events, {"x-requests-remaining": "unlimited"}

        try:
            sport_title = get_sport_title(sport_key)
            parsed = []  # type: List[OddsEvent]

            # 1) Fetch game events by series_id (reliable, gets both pre-game + live)
            series_id = SPORT_TO_SERIES_ID.get(sport_key)
            game_events = []  # type: List[dict]
            if series_id:
                game_events = await self._fetch_game_events_by_series(series_id)
                for event in game_events:
                    p = self._try_parse_game(event, sport_key, sport_title, keywords, series_match=True)
                    if p:
                        parsed.append(p)

            # 1b) If no series_id, try tag_slug-based fetch (e.g., boxing)
            tag_slug = SPORT_TO_TAG_SLUG.get(sport_key)
            if not series_id and tag_slug:
                tag_events = await self._fetch_events_by_tag_slug(tag_slug)
                for event in tag_events:
                    p = self._try_parse_game(event, sport_key, sport_title, keywords, series_match=True)
                    if p:
                        parsed.append(p)
                        game_events.append(event)

            # 2) Also check cached sports events for any missed game events
            if self._cached_events is None:
                self._cached_events = await self._fetch_sports_events()
            seen_slugs = {e.get("slug") for e in game_events}
            for event in self._cached_events:
                if event.get("slug") in seen_slugs:
                    continue
                p = self._try_parse_game(event, sport_key, sport_title, keywords, series_match=False)
                if p:
                    parsed.append(p)

            # Patch per-side liquidity and CLOB best-ask prices.
            # Snapshot the token map locally so concurrent sport fetches don't
            # interfere (the shared map is populated during parsing above).
            # Wrap in 5s timeout so events always return even if CLOB is slow.
            if parsed:
                local_token_map = dict(self._clob_token_map)
                local_spread_map = dict(self._clob_spread_token_map)
                local_total_map = dict(self._clob_total_token_map)
                try:
                    await asyncio.wait_for(
                        self._patch_clob_liquidity(
                            parsed, local_token_map, local_spread_map, local_total_map
                        ),
                        timeout=5.0,
                    )
                except asyncio.TimeoutError:
                    logger.info(
                        "Polymarket: CLOB liquidity timed out (5s) for %s, "
                        "returning %d events without liquidity",
                        sport_key, len(parsed),
                    )

            if parsed:
                logger.info(
                    f"Polymarket: {len(parsed)} game events for {sport_key}"
                )
            # Cache results to avoid refetching every 5s
            self._sport_cache[sport_key] = (parsed, _time.time())
            return parsed, {"x-requests-remaining": "unlimited"}

        except Exception as e:
            logger.warning(f"Polymarket failed for {sport_key}: {e}")
            # Return stale cache if available
            if cached:
                return cached[0], {"x-requests-remaining": "unlimited"}
            return [], {"x-requests-remaining": "unlimited"}

    async def get_team_futures(
        self, sport_key: str
    ) -> Optional[Dict[str, Any]]:
        """Extract championship futures odds for each team in a sport.

        Returns structured data that CompositeSource uses to enrich game events.
        """
        patterns = CHAMPIONSHIP_PATTERNS.get(sport_key)
        if not patterns:
            return None

        try:
            if self._cached_events is None:
                self._cached_events = await self._fetch_sports_events()

            for event in self._cached_events:
                title_lower = event.get("title", "").lower()
                if not any(p in title_lower for p in patterns):
                    continue

                event_markets = event.get("markets", [])
                if len(event_markets) < 2:
                    continue

                teams = {}  # type: Dict[str, Dict[str, Any]]
                event_liq = event.get("liquidityClob") or event.get("liquidity") or 0
                slug = event.get("slug", "")

                for mkt in event_markets:
                    team_name = mkt.get("groupItemTitle", "")
                    prices_str = mkt.get("outcomePrices", "")
                    if not team_name or not prices_str:
                        continue

                    try:
                        prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
                        prob = float(prices[0])  # "Yes" price = probability of winning

                        # Skip resolved markets (probability at 0 or 1)
                        if prob <= 0.001 or prob >= 0.99:
                            continue

                        # Convert probability to American odds (float precision)
                        odds = prob_to_american(prob)

                        # Get per-market liquidity
                        mkt_liq = mkt.get("liquidityClob") or mkt.get("liquidity") or 0
                        try:
                            mkt_liq = float(mkt_liq)
                        except (ValueError, TypeError):
                            mkt_liq = 0.0

                        norm_name = normalize_team_name(team_name)
                        teams[norm_name] = {
                            "raw_name": team_name,
                            "price": odds,
                            "probability": prob,
                            "liquidity": mkt_liq if mkt_liq > 0 else None,
                        }
                    except (json.JSONDecodeError, ValueError, IndexError):
                        continue

                if teams:
                    try:
                        total_liq = float(event_liq)
                    except (ValueError, TypeError):
                        total_liq = 0.0

                    logger.info(
                        f"Polymarket futures: {len(teams)} teams for {sport_key} "
                        f"(${total_liq:,.0f} total liquidity)"
                    )
                    return {
                        "bookmaker_key": "polymarket",
                        "bookmaker_title": "Polymarket",
                        "event_url": f"https://polymarket.com/event/{slug}" if slug else None,
                        "total_liquidity": total_liq,
                        "teams": teams,
                    }

        except Exception as e:
            logger.warning(f"Polymarket futures failed for {sport_key}: {e}")

        return None

    async def _fetch_game_events_by_series(self, series_id: int) -> list:
        """Fetch game-level events by series_id (e.g. 10470 for NCAA CBB).

        Uses series_id instead of slug_contains because the latter is broken
        in the Gamma API (returns unrelated events).
        """
        all_events = []  # type: List[dict]
        offset = 0
        limit = 100
        while True:
            url = f"{GAMMA_API_URL}/events"
            params = {
                "series_id": series_id,
                "active": "true",
                "closed": "false",
                "limit": limit,
                "offset": offset,
            }
            async with self._api_sem:
                response = await self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            events = data if isinstance(data, list) else data.get("events", data.get("data", []))
            if not events:
                break
            all_events.extend(events)
            if len(events) < limit:
                break
            offset += limit
            if offset >= 500:
                break

        # Filter out ended/cancelled events and far-future pre-game events
        from datetime import datetime, timedelta, timezone as tz
        now = datetime.now(tz.utc)
        max_future = now + timedelta(days=14)  # Show events within 14 days

        active_events = []
        for ev in all_events:
            period = ev.get("period") or ""
            ended = ev.get("ended")
            # Skip events that have ended or been cancelled
            if ended is True or period in ENDED_PERIODS:
                continue
            # Must have markets
            if not ev.get("markets"):
                continue
            # Filter by start time (only show next 3 days, skip stale)
            start_str = ev.get("startTime", "")
            if start_str:
                try:
                    start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                    if start_dt > max_future:
                        continue
                    if start_dt < now - timedelta(hours=6):
                        continue
                except (ValueError, TypeError):
                    pass
            active_events.append(ev)

        logger.info(
            f"Polymarket: fetched {len(all_events)} events for series {series_id}, "
            f"{len(active_events)} active (within 3 days)"
        )
        return active_events

    async def _fetch_events_by_tag_slug(self, tag_slug: str) -> list:
        """Fetch events by a specific tag_slug (e.g., 'boxing', 'tennis').

        Used as fallback for sports that don't have a series_id mapping.
        Applies the same active/date filters as _fetch_game_events_by_series.
        """
        all_events = []  # type: List[dict]
        offset = 0
        limit = 100
        while True:
            url = f"{GAMMA_API_URL}/events"
            params = {
                "tag_slug": tag_slug,
                "active": "true",
                "closed": "false",
                "limit": limit,
                "offset": offset,
            }
            async with self._api_sem:
                response = await self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            events = data if isinstance(data, list) else data.get("events", data.get("data", []))
            if not events:
                break
            all_events.extend(events)
            if len(events) < limit:
                break
            offset += limit
            if offset >= 500:
                break

        from datetime import datetime, timedelta, timezone as tz
        now = datetime.now(tz.utc)
        max_future = now + timedelta(days=14)

        active_events = []
        for ev in all_events:
            period = ev.get("period") or ""
            ended = ev.get("ended")
            if ended is True or period in ENDED_PERIODS:
                continue
            if not ev.get("markets"):
                continue
            start_str = ev.get("startTime", "")
            if start_str:
                try:
                    start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                    if start_dt > max_future:
                        continue
                    if start_dt < now - timedelta(hours=6):
                        continue
                except (ValueError, TypeError):
                    pass
            active_events.append(ev)

        logger.info(
            f"Polymarket: fetched {len(all_events)} events for tag_slug={tag_slug}, "
            f"{len(active_events)} active"
        )
        return active_events

    async def _fetch_sports_events(self) -> list:
        """Fetch all active sports events from gamma API using tag_slug=sports."""
        all_events = []  # type: List[dict]
        offset = 0
        limit = 100
        while True:
            url = f"{GAMMA_API_URL}/events"
            params = {
                "tag_slug": "sports",
                "limit": limit,
                "offset": offset,
                "active": "true",
                "closed": "false",
            }
            async with self._api_sem:
                response = await self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            events = data if isinstance(data, list) else data.get("events", data.get("data", []))
            if not events:
                break
            all_events.extend(events)
            if len(events) < limit:
                break
            offset += limit
            if offset >= 500:
                break

        logger.info(f"Polymarket: fetched {len(all_events)} total sports events")
        return all_events

    # Common mascot words used by Polymarket in team names
    _MASCOT_WORDS = {
        "49ers", "aces", "aggies", "anteaters", "aztecs", "badgers", "banana",
        "bandits", "battlers", "beach", "bearcats", "bears", "beavers",
        "bengals", "billikens", "bison", "blazers", "blue", "bobcats",
        "boilermakers", "braves", "broncos", "bruins", "buckeyes",
        "buffaloes", "bulldogs", "bulls", "camels", "cardinals", "catamounts",
        "cavaliers", "chargers", "chippewas", "clemson", "cobras",
        "colonels", "commodores", "cornhuskers", "cougars", "cowboys",
        "crimson", "crusaders", "cyclones", "deacons", "deacs", "demons",
        "devils", "dolphins", "dons", "ducks", "dukes", "eagles",
        "evangels", "explorers", "falcons", "fighting", "flash",
        "flyers", "friars", "gaels", "gators", "golden", "governors",
        "greyhounds", "grizzlies", "hawks", "heels", "hilltoppers",
        "hoosiers", "hornets", "hurricanes", "huskies", "illini",
        "irish", "jaguars", "jaspers", "javelinas", "jayhawks", "knights",
        "lancers", "leopards", "lions", "longhorns", "lumberjacks",
        "mastodons", "mavericks", "miners", "monarchs", "mountaineers",
        "musketeers", "mustangs", "nittany", "orange", "owls",
        "paladins", "panthers", "patriots", "peacocks", "penguins",
        "phoenix", "pilots", "pioneers", "pirates", "purple",
        "racers", "raiders", "rams", "rattlers", "razorbacks", "rebels",
        "red", "redhawks", "retrievers", "riverhawks", "roadrunners",
        "rockets", "runnin", "scarlet", "seahawks", "shockers",
        "skyhawks", "slugs", "sooners", "spartans", "spiders",
        "stags", "tar", "terrapins", "terriers", "texans", "thunderbirds",
        "tide", "tigers", "titans", "tommies", "toreros", "tritons",
        "trojans", "trailblazers", "tribe", "volunteers", "warriors",
        "wave", "wildcats", "wolf", "wolfpack", "wolverines", "zips",
        # Additional college mascots
        "salukis", "redbirds", "matadors", "highlanders", "roos",
        "penguins", "flames", "mean", "green", "pack", "runnin",
        "camels", "catamounts", "chanticleers", "chants",
        "hatters", "leathernecks", "lakers", "ospreys", "owls",
        "paladins", "phoenix", "ragin", "cajuns", "rainbow",
        # More mascots found in Polymarket data
        "gauchos", "waves", "antelopes", "griffins", "broncs",
        "saints", "sycamores", "foxes", "college", "runnin'",
        "herd", "utes", "midshipmen", "sun", "hokies", "seminoles",
        "gamecocks", "hoyas", "yellow", "jackets", "storm",
        "lobos", "billikens", "deacons", "demon",
        # Even more from live data
        "privateers", "islanders", "screaming", "vaqueros",
        "delta", "wolves", "ragin'",
    }

    @staticmethod
    def _parse_date_from_slug(slug: str) -> str:
        """Extract game date from Polymarket slug like 'cbb-lou-ncar-2026-02-23'.

        Returns an ISO timestamp at midnight UTC. Since the slug uses the UTC
        date of game start, midnight UTC converts to 7pm ET previous day --
        which is correct for the majority of evening college basketball games.
        """
        if not slug:
            return ""
        match = re.search(r"(\d{4}-\d{2}-\d{2})$", slug)
        if match:
            return f"{match.group(1)}T00:00:00Z"
        return ""

    @staticmethod
    def _estimate_start_from_end(end_date_str: str) -> str:
        """Estimate game start from Polymarket's event endDate.

        For game events, endDate is typically ~24h after game start.
        Subtracting 24h gives us an approximate game start that produces
        the correct ET date for canonical ID matching.
        """
        if not end_date_str:
            return ""
        try:
            from datetime import datetime, timedelta, timezone as tz
            dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            start = dt - timedelta(hours=24)
            return start.strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            return ""

    @classmethod
    def _strip_mascot(cls, raw_name: str, sport_key: str = "") -> str:
        """Strip mascot/suffix from Polymarket team names.

        'Louisville Cardinals' -> 'Louisville'
        'North Carolina Tar Heels' -> 'North Carolina'
        'Duke Blue Devils' -> 'Duke'
        'Texas A&M-CC Islanders' -> 'Texas A&M-CC'

        sport_key is passed through to resolve_team_name for sport-specific
        alias resolution (e.g. "Utah" → "Utah Hockey Club" only for NHL).
        """
        # First try: if the full name resolves via alias, use it
        resolved = resolve_team_name(raw_name, sport_key=sport_key)
        if resolved != raw_name:
            return resolved

        # Progressively strip trailing words and check aliases
        words = raw_name.split()
        for i in range(len(words) - 1, 0, -1):
            shorter = " ".join(words[:i])
            resolved = resolve_team_name(shorter, sport_key=sport_key)
            if resolved != shorter:
                return resolved

        # Strip trailing mascot words (e.g. "Duke Blue Devils" -> "Duke")
        while len(words) > 1 and words[-1].lower() in cls._MASCOT_WORDS:
            words = words[:-1]
        result = " ".join(words)
        if result != raw_name:
            # Try resolving the stripped name
            resolved = resolve_team_name(result, sport_key=sport_key)
            return resolved if resolved != result else result

        # For 2-word names where the second word looks like a mascot
        parts = raw_name.split()
        if len(parts) == 2:
            # Check if second word is a common mascot
            if parts[1].lower() in cls._MASCOT_WORDS:
                return parts[0]

        return raw_name

    def _find_market_by_type(
        self, event_markets: list, market_type: str
    ) -> Optional[dict]:
        """Find a market by sportsMarketType field."""
        for mkt in event_markets:
            if mkt.get("sportsMarketType") == market_type:
                return mkt
        return None

    def _find_moneyline_market(self, event_markets: list) -> Optional[dict]:
        """Find the moneyline market from a list of markets.

        Uses sportsMarketType='moneyline' first, falls back to question parsing.
        """
        # Prefer sportsMarketType field (reliable for sports events)
        for mkt in event_markets:
            if mkt.get("sportsMarketType") == "moneyline":
                return mkt

        # Fallback: parse question text
        for mkt in event_markets:
            question = (mkt.get("question", "") or "").lower()
            group_title = (mkt.get("groupItemTitle", "") or "").lower()
            # Skip spread and total markets
            if "spread" in question or "spread" in group_title:
                continue
            if "o/u" in question or "over" in question or "under" in question:
                continue
            if "total" in question:
                continue
            # This is likely the moneyline market
            return mkt
        # Fallback: return first market
        return event_markets[0] if event_markets else None

    def _parse_spread_market(
        self, event_markets: list, home_team: str, away_team: str,
        sport_key: str = "",
    ) -> Optional[Tuple[Market, List[str]]]:
        """Parse the primary spread market from Polymarket's sportsMarketType='spreads'.

        Returns a tuple of (Market, clobTokenIds) or None.
        Picks the spread with the best liquidity or first available.
        """
        spread_markets = [
            m for m in event_markets
            if m.get("sportsMarketType") == "spreads"
        ]
        if not spread_markets:
            return None

        # Pick the one with best liquidity, or first
        best = None
        best_liq = -1.0
        for sm in spread_markets:
            liq = 0.0
            try:
                liq = float(sm.get("liquidityClob") or sm.get("liquidity") or 0)
            except (ValueError, TypeError):
                pass
            if liq > best_liq:
                best_liq = liq
                best = sm
        if best is None:
            best = spread_markets[0]

        line = best.get("line")
        if line is None:
            return None

        prices_str = best.get("outcomePrices", "")
        outcomes_str = best.get("outcomes", "")
        try:
            prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
            outcomes_list = json.loads(outcomes_str) if isinstance(outcomes_str, str) else (outcomes_str or [])
            if len(prices) < 2 or len(outcomes_list) < 2:
                return None
        except (json.JSONDecodeError, ValueError):
            return None

        prob_a = float(prices[0])
        prob_b = float(prices[1])
        if prob_a <= 0.01 or prob_a >= 0.99:
            return None

        odds_a = prob_to_american(prob_a)
        odds_b = prob_to_american(prob_b)

        # Parse question to determine which team has the line
        # e.g. "Spread: Northwestern State Demons (-3.5)"
        question = best.get("question", "")
        line_val = float(line)

        # Determine home/away spread assignment
        # The first outcome is the favored team (with negative spread)
        fav_name_raw = outcomes_list[0]
        fav_name = self._strip_mascot(fav_name_raw, sport_key=sport_key)
        dog_name_raw = outcomes_list[1]
        dog_name = self._strip_mascot(dog_name_raw, sport_key=sport_key)

        # Extract CLOB token IDs for this spread market
        clob_tokens_raw = best.get("clobTokenIds", "[]")
        try:
            clob_tokens = json.loads(clob_tokens_raw) if isinstance(clob_tokens_raw, str) else (clob_tokens_raw or [])
        except (json.JSONDecodeError, TypeError):
            clob_tokens = []

        # If first outcome matches home_team, home gets the line
        if fav_name == home_team:
            home_point = line_val
            away_point = -line_val
            home_price = odds_a
            away_price = odds_b
            home_token_idx, away_token_idx = 0, 1
        elif fav_name == away_team:
            away_point = line_val
            home_point = -line_val
            away_price = odds_a
            home_price = odds_b
            home_token_idx, away_token_idx = 1, 0
        else:
            # Can't determine assignment, skip
            return None

        # Reorder CLOB tokens to match (home, away) outcome order
        ordered_tokens = []  # type: List[str]
        if len(clob_tokens) >= 2:
            ordered_tokens = [clob_tokens[home_token_idx], clob_tokens[away_token_idx]]

        return (Market(
            key="spreads",
            outcomes=[
                Outcome(name=home_team, price=home_price, point=home_point),
                Outcome(name=away_team, price=away_price, point=away_point),
            ],
        ), ordered_tokens)

    def _parse_total_market(self, event_markets: list) -> Optional[Tuple[Market, List[str]]]:
        """Parse the primary totals market from Polymarket's sportsMarketType='totals'.

        Returns a tuple of (Market, clobTokenIds) or None.
        Picks the total with best liquidity or first available.
        """
        total_markets = [
            m for m in event_markets
            if m.get("sportsMarketType") == "totals"
        ]
        if not total_markets:
            return None

        # Pick the one with best liquidity, or first
        best = None
        best_liq = -1.0
        for tm in total_markets:
            liq = 0.0
            try:
                liq = float(tm.get("liquidityClob") or tm.get("liquidity") or 0)
            except (ValueError, TypeError):
                pass
            if liq > best_liq:
                best_liq = liq
                best = tm
        if best is None:
            best = total_markets[0]

        line = best.get("line")
        if line is None:
            return None

        prices_str = best.get("outcomePrices", "")
        outcomes_str = best.get("outcomes", "")
        try:
            prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
            outcomes_list = json.loads(outcomes_str) if isinstance(outcomes_str, str) else (outcomes_str or [])
            if len(prices) < 2:
                return None
        except (json.JSONDecodeError, ValueError):
            return None

        prob_a = float(prices[0])
        prob_b = float(prices[1])
        if prob_a <= 0.01 or prob_a >= 0.99:
            return None

        odds_a = prob_to_american(prob_a)
        odds_b = prob_to_american(prob_b)

        line_val = float(line)

        # Extract CLOB token IDs for this total market
        clob_tokens_raw = best.get("clobTokenIds", "[]")
        try:
            clob_tokens = json.loads(clob_tokens_raw) if isinstance(clob_tokens_raw, str) else (clob_tokens_raw or [])
        except (json.JSONDecodeError, TypeError):
            clob_tokens = []

        # Outcomes are typically ["Over", "Under"]
        over_name = outcomes_list[0] if len(outcomes_list) > 0 else "Over"
        under_name = outcomes_list[1] if len(outcomes_list) > 1 else "Under"

        # If outcomes are "Over"/"Under", assign directly
        if "over" in over_name.lower():
            over_price = odds_a
            under_price = odds_b
            over_token_idx, under_token_idx = 0, 1
        elif "under" in over_name.lower():
            over_price = odds_b
            under_price = odds_a
            over_token_idx, under_token_idx = 1, 0
        else:
            # Assume first is Over
            over_price = odds_a
            under_price = odds_b
            over_token_idx, under_token_idx = 0, 1

        # Reorder CLOB tokens to match (Over, Under) outcome order
        ordered_tokens = []  # type: List[str]
        if len(clob_tokens) >= 2:
            ordered_tokens = [clob_tokens[over_token_idx], clob_tokens[under_token_idx]]

        return (Market(
            key="totals",
            outcomes=[
                Outcome(name="Over", price=over_price, point=line_val),
                Outcome(name="Under", price=under_price, point=line_val),
            ],
        ), ordered_tokens)

    def _parse_mma_distance_market(self, event_markets: list) -> Optional[Market]:
        """Parse 'Fight to Go the Distance?' Yes/No market for MMA."""
        for mkt in event_markets:
            question = (mkt.get("question", "") or "").lower()
            group_title = (mkt.get("groupItemTitle", "") or "").lower()
            all_text = f"{question} {group_title}"
            if "distance" not in all_text:
                continue

            prices_str = mkt.get("outcomePrices", "")
            outcomes_str = mkt.get("outcomes", "")
            try:
                prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
                outcomes_list = json.loads(outcomes_str) if isinstance(outcomes_str, str) else (outcomes_str or [])
                if len(prices) < 2:
                    continue
                prob_a = float(prices[0])
                prob_b = float(prices[1])
            except (json.JSONDecodeError, ValueError, IndexError):
                continue

            if prob_a <= 0.01 or prob_a >= 0.99:
                continue

            odds_a = prob_to_american(prob_a)
            odds_b = prob_to_american(prob_b)

            # Determine Yes/No from outcome names
            name_a = (outcomes_list[0] if len(outcomes_list) > 0 else "Yes").lower()
            if "yes" in name_a:
                return Market(
                    key="fight_to_go_distance",
                    outcomes=[
                        Outcome(name="Yes", price=odds_a),
                        Outcome(name="No", price=odds_b),
                    ],
                )
            elif "no" in name_a:
                return Market(
                    key="fight_to_go_distance",
                    outcomes=[
                        Outcome(name="Yes", price=odds_b),
                        Outcome(name="No", price=odds_a),
                    ],
                )
            else:
                # Assume first is Yes
                return Market(
                    key="fight_to_go_distance",
                    outcomes=[
                        Outcome(name="Yes", price=odds_a),
                        Outcome(name="No", price=odds_b),
                    ],
                )
        return None

    def _parse_mma_total_rounds_market(self, event_markets: list) -> Optional[Market]:
        """Parse 'O/U X.5 Rounds' market for MMA."""
        for mkt in event_markets:
            question = (mkt.get("question", "") or "").lower()
            group_title = (mkt.get("groupItemTitle", "") or "").lower()
            all_text = f"{question} {group_title}"
            if "round" not in all_text:
                continue
            # Must be an Over/Under style market (not "What round will the fight end?")
            if "o/u" not in all_text and "over" not in all_text and "total" not in all_text:
                continue

            prices_str = mkt.get("outcomePrices", "")
            outcomes_str = mkt.get("outcomes", "")
            line = mkt.get("line")
            try:
                prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
                outcomes_list = json.loads(outcomes_str) if isinstance(outcomes_str, str) else (outcomes_str or [])
                if len(prices) < 2:
                    continue
                prob_a = float(prices[0])
                prob_b = float(prices[1])
            except (json.JSONDecodeError, ValueError, IndexError):
                continue

            if prob_a <= 0.01 or prob_a >= 0.99:
                continue

            odds_a = prob_to_american(prob_a)
            odds_b = prob_to_american(prob_b)

            # Parse line from market data or from question text (e.g. "O/U 2.5 Rounds")
            point = None
            if line is not None:
                try:
                    point = float(line)
                except (ValueError, TypeError):
                    pass
            if point is None:
                # Try parsing from question/group_title: "O/U 2.5 Rounds"
                import re as _re
                m = _re.search(r"(\d+\.?\d*)\s*rounds?", all_text)
                if m:
                    try:
                        point = float(m.group(1))
                    except ValueError:
                        pass

            name_a = (outcomes_list[0] if len(outcomes_list) > 0 else "Over").lower()
            if "over" in name_a:
                return Market(
                    key="total_rounds",
                    outcomes=[
                        Outcome(name="Over", price=odds_a, point=point),
                        Outcome(name="Under", price=odds_b, point=point),
                    ],
                )
            elif "under" in name_a:
                return Market(
                    key="total_rounds",
                    outcomes=[
                        Outcome(name="Over", price=odds_b, point=point),
                        Outcome(name="Under", price=odds_a, point=point),
                    ],
                )
            else:
                return Market(
                    key="total_rounds",
                    outcomes=[
                        Outcome(name="Over", price=odds_a, point=point),
                        Outcome(name="Under", price=odds_b, point=point),
                    ],
                )
        return None

    def _try_parse_game(
        self,
        event: dict,
        sport_key: str,
        sport_title: str,
        keywords: List[str],
        series_match: bool = False,
    ) -> Optional[OddsEvent]:
        title = event.get("title", "")
        event_markets = event.get("markets", [])

        if not title or not event_markets:
            return None

        # For series-matched events, allow up to 100 markets (moneyline + spreads + totals + props)
        # For tag-matched events, keep stricter limit to avoid futures
        max_markets = 100 if series_match else 3
        if len(event_markets) > max_markets:
            return None

        # Strip sport prefix from title
        clean_title = TITLE_PREFIXES.sub("", title).strip()

        # Must match "Team A vs Team B" pattern
        match = GAME_PATTERN.match(clean_title)
        if not match:
            return None

        slug = event.get("slug", "")

        # Check if this event belongs to the requested sport
        if not series_match:
            title_lower = title.lower()
            slug_lower = slug.lower() if slug else ""
            desc = (event.get("description", "") or "").lower()
            all_text = f"{title_lower} {slug_lower} {desc}"
            if not any(kw in title_lower or kw in slug_lower for kw in keywords):
                if not any(kw in desc for kw in keywords):
                    return None
            # Check exclusion keywords
            exclude_kws = SPORT_EXCLUDE_KEYWORDS.get(sport_key, [])
            if exclude_kws and any(ekw in all_text for ekw in exclude_kws):
                return None

        team_a_raw = match.group(1).strip()
        team_b_raw = match.group(2).strip()

        # Strip mascot names (e.g. "Louisville Cardinals" -> "Louisville")
        team_a = self._strip_mascot(team_a_raw, sport_key=sport_key)
        team_b = self._strip_mascot(team_b_raw, sport_key=sport_key)

        if not team_a or not team_b:
            return None

        # Find the moneyline market (skip spreads and totals)
        mkt = self._find_moneyline_market(event_markets)
        if not mkt:
            return None

        outcomes_str = mkt.get("outcomes", "")
        prices_str = mkt.get("outcomePrices", "")
        if not prices_str:
            return None

        try:
            outcomes_list = json.loads(outcomes_str) if isinstance(outcomes_str, str) else (outcomes_str or [])
            prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
            if len(prices) < 2:
                return None
            prob_a = float(prices[0])
            prob_b = float(prices[1])
        except (json.JSONDecodeError, ValueError, IndexError):
            return None

        if prob_a <= 0.01 or prob_a >= 0.99 or prob_b <= 0.01 or prob_b >= 0.99:
            return None

        odds_a = prob_to_american(prob_a)
        odds_b = prob_to_american(prob_b)

        # Determine commence_time:
        # 1) Best: use startTime field (actual game start time from Polymarket)
        # 2) Fallback: endDate - 24h (endDate is ~24h after game start)
        # 3) Last resort: parse date from slug
        commence_time = event.get("startTime", "")
        if not commence_time:
            end_dt_str = event.get("endDate", "")
            if not end_dt_str:
                end_dt_str = mkt.get("endDate", mkt.get("end_date_iso", ""))
            commence_time = self._estimate_start_from_end(end_dt_str)
        if not commence_time:
            commence_time = self._parse_date_from_slug(slug)

        outcome_a_name = outcomes_list[0] if len(outcomes_list) > 0 else team_a
        outcome_b_name = outcomes_list[1] if len(outcomes_list) > 1 else team_b

        if outcome_a_name in ("Yes", "No"):
            home_team = team_b
            away_team = team_a
            home_odds = odds_b if outcome_a_name == "Yes" else odds_a
            away_odds = odds_a if outcome_a_name == "Yes" else odds_b
            # Token mapping: outcome 0 = A, outcome 1 = B
            home_token_idx = 1 if outcome_a_name == "Yes" else 0
            away_token_idx = 0 if outcome_a_name == "Yes" else 1
        else:
            home_team = team_b
            away_team = team_a
            home_odds = odds_b
            away_odds = odds_a
            home_token_idx = 1  # team_b = outcome 1
            away_token_idx = 0  # team_a = outcome 0

        cid = canonical_event_id(sport_key, home_team, away_team, commence_time)

        # Save CLOB token IDs for per-side liquidity patching later
        clob_tokens_raw = mkt.get("clobTokenIds", "[]")
        try:
            clob_tokens = json.loads(clob_tokens_raw) if isinstance(clob_tokens_raw, str) else (clob_tokens_raw or [])
        except (json.JSONDecodeError, TypeError):
            clob_tokens = []
        if len(clob_tokens) >= 2:
            self._clob_token_map[cid] = (
                clob_tokens[home_token_idx],
                clob_tokens[away_token_idx],
            )

        # Build markets list (moneyline + optional spreads + totals)
        all_markets = []  # type: List[Market]

        # Liquidity will be patched from CLOB order book in _patch_clob_liquidity()
        h2h_market = Market(
            key="h2h",
            outcomes=[
                Outcome(name=home_team, price=home_odds),
                Outcome(name=away_team, price=away_odds),
            ],
        )
        all_markets.append(h2h_market)

        # Parse spreads if available
        spread_result = self._parse_spread_market(event_markets, home_team, away_team, sport_key=sport_key)
        if spread_result:
            spread_market, spread_clob_tokens = spread_result
            all_markets.append(spread_market)
            if len(spread_clob_tokens) >= 2:
                self._clob_spread_token_map[cid] = (spread_clob_tokens[0], spread_clob_tokens[1])

        # Parse totals if available
        total_result = self._parse_total_market(event_markets)
        if total_result:
            total_market, total_clob_tokens = total_result
            all_markets.append(total_market)
            if len(total_clob_tokens) >= 2:
                self._clob_total_token_map[cid] = (total_clob_tokens[0], total_clob_tokens[1])

        # Parse MMA-specific markets (fight_to_go_distance, total_rounds)
        if sport_key in ("mma_mixed_martial_arts", "boxing_boxing"):
            distance_market = self._parse_mma_distance_market(event_markets)
            if distance_market:
                all_markets.append(distance_market)
            rounds_market = self._parse_mma_total_rounds_market(event_markets)
            if rounds_market:
                all_markets.append(rounds_market)

        event_url = f"https://polymarket.com/event/{slug}" if slug else None

        # Build ScoreData (pre-game only — live odds not supported yet)
        score_data = None  # type: Optional[ScoreData]
        if event.get("ended") is not True:
            score_data = ScoreData(status="pre")

        return OddsEvent(
            id=cid,
            sport_key=sport_key,
            sport_title=sport_title,
            commence_time=commence_time,
            home_team=home_team,
            away_team=away_team,
            bookmakers=[
                Bookmaker(
                    key="polymarket",
                    title="Polymarket",
                    markets=all_markets,
                    event_url=event_url,
                )
            ],
            score_data=score_data,
        )

    # ------------------------------------------------------------------
    # CLOB order book: per-side liquidity at best ask
    # ------------------------------------------------------------------

    async def _fetch_clob_book(self, token_id: str) -> Optional[dict]:
        """Fetch order book for a single CLOB token (3s timeout)."""
        try:
            async with self._api_sem:
                resp = await self._client.get(
                    f"{CLOB_API_URL}/book",
                    params={"token_id": token_id},
                    timeout=3.0,
                )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    async def _patch_clob_liquidity(
        self,
        events: List[OddsEvent],
        token_map: Optional[Dict[str, tuple]] = None,
        spread_token_map: Optional[Dict[str, tuple]] = None,
        total_token_map: Optional[Dict[str, tuple]] = None,
    ) -> None:
        """Fetch CLOB order books and patch per-outcome liquidity + odds at best ask.

        For each market (h2h, spreads, totals), fetches the order book for both
        outcome tokens and sets outcome.liquidity = contracts available at best
        ask price, and updates outcome.price to use the CLOB best ask (actual
        tradable price) instead of the midpoint outcomePrices.

        Fetches all tokens, batched 10 at a time with 0.1s delay between batches.
        """
        clob_map = token_map if token_map is not None else self._clob_token_map
        spread_map = spread_token_map if spread_token_map is not None else self._clob_spread_token_map
        total_map = total_token_map if total_token_map is not None else self._clob_total_token_map

        if not clob_map and not spread_map and not total_map:
            return

        # Collect all unique tokens to fetch across all market types
        tokens_seen = set()  # type: set
        tokens_needed = []  # type: List[str]

        for ev in events:
            # h2h tokens
            h2h_pair = clob_map.get(ev.id)
            if h2h_pair:
                for tok in h2h_pair:
                    if tok not in tokens_seen:
                        tokens_seen.add(tok)
                        tokens_needed.append(tok)
            # spread tokens
            spread_pair = spread_map.get(ev.id)
            if spread_pair:
                for tok in spread_pair:
                    if tok not in tokens_seen:
                        tokens_seen.add(tok)
                        tokens_needed.append(tok)
            # total tokens
            total_pair = total_map.get(ev.id)
            if total_pair:
                for tok in total_pair:
                    if tok not in tokens_seen:
                        tokens_seen.add(tok)
                        tokens_needed.append(tok)

        if not tokens_needed:
            return

        logger.debug("Polymarket CLOB: fetching %d tokens", len(tokens_needed))

        # Fetch all tokens concurrently with a semaphore to limit parallelism
        sem = asyncio.Semaphore(20)
        book_results = {}  # type: Dict[str, Optional[dict]]  # token → book

        async def _fetch_with_sem(tok: str) -> Tuple[str, Optional[dict]]:
            async with sem:
                result = await self._fetch_clob_book(tok)
                return (tok, result)

        all_results = await asyncio.gather(
            *[_fetch_with_sem(tok) for tok in tokens_needed],
            return_exceptions=True,
        )
        for item in all_results:
            if isinstance(item, tuple) and len(item) == 2:
                tok, result = item
                if isinstance(result, dict):
                    book_results[tok] = result

        # Extract best-ask price and depth for each token
        token_liq = {}  # type: Dict[str, float]  # token → $ liquidity at best ask
        token_ask_price = {}  # type: Dict[str, float]  # token → best ask probability
        for tok, book in book_results.items():
            if not book:
                continue
            asks = book.get("asks") or []
            if not asks:
                continue
            # Sort asks ascending by price, best ask = lowest price
            asks_sorted = sorted(asks, key=lambda x: float(x.get("price", 999)))
            best = asks_sorted[0]
            size = float(best.get("size", 0))
            price = float(best.get("price", 0))
            if size > 0 and price > 0:
                # Dollar liquidity = contracts * price per contract
                token_liq[tok] = round(size * price, 2)
                # Best ask probability for odds conversion
                token_ask_price[tok] = price

        def _patch_market_odds(market: "Market", tok_a: str, tok_b: str) -> bool:
            """Patch a single market's outcomes with CLOB best-ask odds and liquidity.
            Returns True if anything was patched."""
            if len(market.outcomes) < 2:
                return False
            liq_a = token_liq.get(tok_a)
            liq_b = token_liq.get(tok_b)
            if liq_a is None and liq_b is None:
                return False

            # Patch liquidity
            if liq_a is not None:
                market.outcomes[0].liquidity = liq_a
            if liq_b is not None:
                market.outcomes[1].liquidity = liq_b
            both = [l for l in [liq_a, liq_b] if l is not None]
            if both:
                market.liquidity = round(sum(both), 2)

            # Update odds to use CLOB best-ask prices (real tradable odds)
            # instead of midpoint outcomePrices (which are symmetric/fake).
            ask_a = token_ask_price.get(tok_a)
            ask_b = token_ask_price.get(tok_b)
            if ask_a is not None:
                market.outcomes[0].price = prob_to_american(ask_a)
            if ask_b is not None:
                market.outcomes[1].price = prob_to_american(ask_b)
            return True

        # Patch all markets for each event
        patched_h2h = 0
        patched_spread = 0
        patched_total = 0
        for ev in events:
            bm = ev.bookmakers[0] if ev.bookmakers else None
            if not bm:
                continue
            for market in bm.markets:
                if market.key == "h2h":
                    h2h_pair = clob_map.get(ev.id)
                    if h2h_pair and _patch_market_odds(market, h2h_pair[0], h2h_pair[1]):
                        patched_h2h += 1
                elif market.key == "spreads":
                    spread_pair = spread_map.get(ev.id)
                    if spread_pair and _patch_market_odds(market, spread_pair[0], spread_pair[1]):
                        patched_spread += 1
                elif market.key == "totals":
                    total_pair = total_map.get(ev.id)
                    if total_pair and _patch_market_odds(market, total_pair[0], total_pair[1]):
                        patched_total += 1

        logger.info(
            "Polymarket: patched CLOB best-ask odds for %d h2h, %d spread, %d total markets (%d events)",
            patched_h2h, patched_spread, patched_total, len(events),
        )

    async def close(self) -> None:
        self._cached_events = None
        await self._client.aclose()
