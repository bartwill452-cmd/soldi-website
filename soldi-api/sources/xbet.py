"""
XBet sportsbook scraper.

Uses httpx to fetch the server-rendered HTML from xbet.ag/sportsbook/
and parses odds using regex and HTML parsing.

XBet renders ALL sports on a single page as HTML (no JSON API). Each game
is wrapped in a `div.game-line` with Schema.org `Event` markup containing
team names, start times, and odds (spread, moneyline, total).

Website: https://www.xbet.ag/sportsbook/
"""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

SITE_URL = "https://www.xbet.ag"

# Map XBet league titles (from .league-title text or nav links) → our sport_key
_LEAGUE_TO_SPORT: Dict[str, str] = {
    # Major leagues
    "NBA": "basketball_nba",
    "NCAA Basketball": "basketball_ncaab",
    "NCAA Football": "americanfootball_ncaaf",
    "NFL": "americanfootball_nfl",
    "NHL": "icehockey_nhl",
    "MLB": "baseball_mlb",
    "MLB Spring Training": "baseball_mlb",
    # Combat sports
    "UFC": "mma_mixed_martial_arts",
    "MMA": "mma_mixed_martial_arts",
    "Boxing": "boxing_boxing",
    # Soccer
    "English Premier League": "soccer_epl",
    "EPL": "soccer_epl",
    "La Liga": "soccer_spain_la_liga",
    "Spanish La Liga": "soccer_spain_la_liga",
    "Bundesliga": "soccer_germany_bundesliga",
    "German Bundesliga": "soccer_germany_bundesliga",
    "Serie A": "soccer_italy_serie_a",
    "Italian Serie A": "soccer_italy_serie_a",
    "Ligue 1": "soccer_france_ligue_one",
    "French Ligue 1": "soccer_france_ligue_one",
    "UEFA Champions League": "soccer_uefa_champs_league",
    "Champions League": "soccer_uefa_champs_league",
    "MLS": "soccer_usa_mls",
    # Tennis
    "ATP Lines": "tennis_atp",
    "ATP": "tennis_atp",
    "WTA Lines": "tennis_wta",
    "WTA": "tennis_wta",
    "Tennis": "tennis_atp",
    # Skip these
    "NCAA Women, Regular Season": None,
    "PGA Round Matchups": None,
    "Featured": None,  # skip the main featured section (it's an aggregate)
}

# Keywords that indicate a futures market (skip these)
_FUTURES_KEYWORDS = frozenset([
    "wins", "champion", "coin toss", "futures", "conference",
    "division", "mvp", "award", "super bowl", "world series",
    "stanley cup", "nba finals", "pennant", "cy young", "heisman",
    "1st half", "1st quarter", "2nd half", "3rd quarter", "4th quarter",
])

# Sports we want to scrape
_SUPPORTED_SPORTS = {
    "basketball_nba", "basketball_ncaab",
    "americanfootball_nfl", "americanfootball_ncaaf",
    "icehockey_nhl", "baseball_mlb",
    "mma_mixed_martial_arts", "boxing_boxing",
    "soccer_epl", "soccer_spain_la_liga", "soccer_germany_bundesliga",
    "soccer_italy_serie_a", "soccer_france_ligue_one",
    "soccer_uefa_champs_league", "soccer_usa_mls",
    "tennis_atp", "tennis_wta",
}

# Cache TTL for parsed data
_CACHE_TTL = 60  # seconds — prefetch loop keeps cache warm every ~30s

# Regex patterns for parsing server-rendered HTML
# Extract FEATURED sections with their IDs
_SECTION_RE = re.compile(
    r'id="scroll-title-FEATURED-(\d+)"[^>]*>(.*?)(?=id="scroll-title-FEATURED-|$)',
    re.DOTALL,
)

# Extract league title from section
_LEAGUE_TITLE_RE = re.compile(
    r'class="league-title[^"]*"[^>]*>(?:<a[^>]*>)?([^<]+)',
)

# Extract nav link text for FEATURED sections
_NAV_LINK_RE = re.compile(
    r'href="[^"]*FEATURED-(\d+)[^"]*"[^>]*>([^<]+)<',
)

# Extract game-line blocks
_GAME_LINE_RE = re.compile(
    r'class="game-line\b[^"]*"(.*?)(?=class="game-line\b|</section|$)',
    re.DOTALL,
)

# Extract Schema.org event name
_EVENT_NAME_RE = re.compile(
    r'itemprop="name"\s+content="([^"]+)"',
)
_EVENT_NAME_TEXT_RE = re.compile(
    r'itemprop="name"[^>]*>([^<]+)<',
)

# Extract Schema.org start date
_START_DATE_RE = re.compile(
    r'itemprop="startDate"\s+content="([^"]+)"',
)
_START_DATE_TEXT_RE = re.compile(
    r'itemprop="startDate"[^>]*>([^<]+)<',
)

# Extract odds values from button/cell text
_ODDS_PATTERN_RE = re.compile(
    r'[+-]\d{1,4}(?:\.\d)?(?:\s+[+-]\d{2,4})?|[OU]\s*\d+\.?\d*\s*[+-]?\d{2,4}',
)


class XBetSource(DataSource):
    """Fetches odds from XBet by parsing the server-rendered sportsbook HTML.

    XBet renders all sports on a single page (/sportsbook/). This source
    fetches the HTML via httpx and parses it with regex to extract odds.
    """

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            follow_redirects=True,
        )
        # Cache: sport_key -> (events, timestamp)
        self._cache: Dict[str, Tuple[List[OddsEvent], float]] = {}
        self._prefetch_task = None  # type: object

    def start_prefetch(self) -> None:
        """Start background prefetch (call after event loop is running)."""
        self._prefetch_task = asyncio.ensure_future(self._prefetch_all())

    async def _prefetch_all(self) -> None:
        """Background task: fetch the sportsbook page and parse all sports."""
        await asyncio.sleep(4)  # Let other sources initialize first
        logger.info("XBet: Starting continuous background prefetch (HTTP)")
        cycle = 0
        while True:
            cycle += 1
            try:
                all_events = await self._fetch_and_parse_all()

                sport_counts = {}  # type: Dict[str, int]
                for sport_key, events in all_events.items():
                    self._cache[sport_key] = (events, time.time())
                    sport_counts[sport_key] = len(events)

                total = sum(sport_counts.values())
                sports_str = " ".join(
                    f"{k.split('_', 1)[-1]}:{v}" for k, v in sorted(sport_counts.items()) if v > 0
                )
                logger.info(
                    "XBet prefetch cycle #%d: %d events across %d sports (%s)",
                    cycle, total, len(sport_counts), sports_str,
                )

            except Exception as e:
                logger.warning("XBet prefetch error: %s: %s", type(e).__name__, e)

            await asyncio.sleep(30)

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        headers = {"x-requests-remaining": "unlimited"}

        if bookmakers and "xbet" not in bookmakers:
            return [], headers

        if sport_key not in _SUPPORTED_SPORTS:
            return [], headers

        # Return from cache
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _CACHE_TTL:
            return cached[0], headers

        return [], headers

    # ------------------------------------------------------------------
    # HTTP fetching & HTML parsing
    # ------------------------------------------------------------------

    async def _fetch_and_parse_all(self) -> Dict[str, List[OddsEvent]]:
        """Fetch the sportsbook HTML page and parse all games."""
        try:
            response = await self._client.get(f"{SITE_URL}/sportsbook/")
            response.raise_for_status()
            html = response.text
        except Exception as e:
            logger.warning("XBet: HTTP fetch error: %s: %s", type(e).__name__, e)
            return {}

        return self._parse_html(html)

    def _parse_html(self, html: str) -> Dict[str, List[OddsEvent]]:
        """Parse the XBet sportsbook HTML into events grouped by sport."""
        # Build nav link mapping: section ID → league name
        nav_map = {}  # type: Dict[str, str]
        for m in _NAV_LINK_RE.finditer(html):
            section_id = m.group(1)
            link_text = m.group(2).strip()
            if link_text and section_id not in nav_map:
                nav_map[section_id] = link_text

        # Find all FEATURED sections
        events_by_sport: Dict[str, List[OddsEvent]] = {}
        seen_event_ids: set = set()

        for section_match in _SECTION_RE.finditer(html):
            section_id = section_match.group(1)
            section_html = section_match.group(2)

            # Determine league name
            league = ""
            title_m = _LEAGUE_TITLE_RE.search(section_html)
            if title_m:
                league = title_m.group(1).strip()
            if not league and section_id in nav_map:
                league = nav_map[section_id]

            # Find all game-line blocks in this section
            for game_match in _GAME_LINE_RE.finditer(section_html):
                game_html = game_match.group(1)

                # Extract event name
                name_m = _EVENT_NAME_RE.search(game_html)
                if not name_m:
                    name_m = _EVENT_NAME_TEXT_RE.search(game_html)
                if not name_m:
                    continue
                match_name = name_m.group(1).strip()
                if not match_name or match_name == "Join Xbet Today":
                    continue

                # Extract start date
                start_date = ""
                date_m = _START_DATE_RE.search(game_html)
                if not date_m:
                    date_m = _START_DATE_TEXT_RE.search(game_html)
                if date_m:
                    start_date = date_m.group(1).strip()

                # Extract odds
                odds_texts = _ODDS_PATTERN_RE.findall(game_html)

                # Process this game
                sport_key = self._resolve_league(league)
                if not sport_key:
                    continue

                teams = self._parse_match_name(match_name)
                if not teams:
                    continue
                away_team, home_team = teams

                combined = (away_team + " " + home_team + " " + league).lower()
                if any(kw in combined for kw in _FUTURES_KEYWORDS):
                    continue

                commence_time = self._parse_start_date(start_date)
                markets_list = self._parse_odds(odds_texts, away_team, home_team, sport_key)
                if not markets_list:
                    continue

                cid = canonical_event_id(sport_key, home_team, away_team, commence_time)

                if cid in seen_event_ids:
                    continue
                seen_event_ids.add(cid)

                sport_title = get_sport_title(sport_key)

                event = OddsEvent(
                    id=cid,
                    sport_key=sport_key,
                    sport_title=sport_title,
                    commence_time=commence_time,
                    home_team=home_team,
                    away_team=away_team,
                    bookmakers=[
                        Bookmaker(
                            key="xbet",
                            title="XBet",
                            markets=markets_list,
                        )
                    ],
                )

                if sport_key not in events_by_sport:
                    events_by_sport[sport_key] = []
                events_by_sport[sport_key].append(event)

        return events_by_sport

    # ------------------------------------------------------------------
    # Parsing helpers (unchanged from original)
    # ------------------------------------------------------------------

    def _resolve_league(self, league_text: str) -> Optional[str]:
        """Map an XBet league title to our sport_key."""
        if not league_text:
            return None
        # Exact match first
        sport_key = _LEAGUE_TO_SPORT.get(league_text)
        if sport_key is not None:
            return sport_key if sport_key in _SUPPORTED_SPORTS else None

        # Partial match
        lower = league_text.lower()
        for title, key in _LEAGUE_TO_SPORT.items():
            if title.lower() in lower or lower in title.lower():
                return key if key and key in _SUPPORTED_SPORTS else None

        return None

    @staticmethod
    def _parse_match_name(name: str) -> Optional[Tuple[str, str]]:
        """Parse 'Away Team v Home Team' into (away, home)."""
        if not name:
            return None
        # Split on " v " (XBet uses " v " separator)
        parts = name.split(" v ", 1)
        if len(parts) != 2:
            # Try " vs " as fallback
            parts = name.split(" vs ", 1)
        if len(parts) != 2:
            return None
        away = resolve_team_name(parts[0].strip())
        home = resolve_team_name(parts[1].strip())
        if not away or not home:
            return None
        return away, home

    @staticmethod
    def _parse_start_date(raw: str) -> str:
        """Parse XBet date format '2026 - 03 - 05 18:00-05:00' to ISO 8601."""
        if not raw:
            return ""
        cleaned = raw.strip()
        try:
            m = re.match(
                r"(\d{4})\s*-\s*(\d{2})\s*-\s*(\d{2})\s+(\d{2}:\d{2})([-+]\d{2}:\d{2})?",
                cleaned,
            )
            if m:
                date_str = f"{m.group(1)}-{m.group(2)}-{m.group(3)}T{m.group(4)}:00"
                tz_str = m.group(5) or "-05:00"  # Default to EST if no tz
                dt = datetime.fromisoformat(date_str + tz_str)
                return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            pass
        return raw

    def _parse_odds(
        self,
        odds_texts: List[str],
        away_team: str,
        home_team: str,
        sport_key: str,
    ) -> List[Market]:
        """Parse odds texts into Market objects."""
        markets = []  # type: List[Market]

        if len(odds_texts) < 6:
            if len(odds_texts) >= 2:
                ml_away = self._safe_int(odds_texts[0])
                ml_home = self._safe_int(odds_texts[1])
                if ml_away is not None and ml_home is not None:
                    markets.append(Market(
                        key="h2h",
                        outcomes=[
                            Outcome(name=home_team, price=ml_home),
                            Outcome(name=away_team, price=ml_away),
                        ],
                    ))
            return markets

        away_spread_text = odds_texts[0]
        away_ml_text = odds_texts[1]
        away_total_text = odds_texts[2]
        home_spread_text = odds_texts[3]
        home_ml_text = odds_texts[4]
        home_total_text = odds_texts[5]

        # --- Moneyline ---
        away_ml = self._safe_int(away_ml_text)
        home_ml = self._safe_int(home_ml_text)
        if away_ml is not None and home_ml is not None:
            outcomes = [
                Outcome(name=home_team, price=home_ml),
                Outcome(name=away_team, price=away_ml),
            ]
            if len(odds_texts) >= 9 and sport_key.startswith("soccer_"):
                draw_ml = self._safe_int(odds_texts[6])
                if draw_ml is not None:
                    outcomes.append(Outcome(name="Draw", price=draw_ml))
            markets.append(Market(key="h2h", outcomes=outcomes))

        # --- Spreads ---
        away_sp = self._parse_spread_text(away_spread_text)
        home_sp = self._parse_spread_text(home_spread_text)
        if away_sp and home_sp:
            markets.append(Market(
                key="spreads",
                outcomes=[
                    Outcome(name=home_team, price=home_sp[1], point=home_sp[0]),
                    Outcome(name=away_team, price=away_sp[1], point=away_sp[0]),
                ],
            ))

        # --- Totals ---
        over = self._parse_total_text(away_total_text)
        under = self._parse_total_text(home_total_text)
        if over and under:
            point = over[0]
            markets.append(Market(
                key="totals",
                outcomes=[
                    Outcome(name="Over", price=over[1], point=point),
                    Outcome(name="Under", price=under[1], point=point),
                ],
            ))

        return markets

    @staticmethod
    def _parse_spread_text(text: str) -> Optional[Tuple[float, int]]:
        """Parse '+9 -110' into (9.0, -110)."""
        m = re.match(r"([+-]?\d+\.?\d*)\s+([+-]\d+)", text.strip())
        if m:
            try:
                return float(m.group(1)), int(m.group(2))
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _parse_total_text(text: str) -> Optional[Tuple[float, int]]:
        """Parse 'O 229.5 -110' or 'U 229.5 -110' into (229.5, -110)."""
        m = re.match(r"[OU]\s*(\d+\.?\d*)\s+([+-]\d+)", text.strip())
        if m:
            try:
                return float(m.group(1)), int(m.group(2))
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _safe_int(text: str) -> Optional[int]:
        """Extract first integer from text like '+301' or '-400'."""
        if not text:
            return None
        m = re.search(r"([+-]?\d+)", text.strip())
        if m:
            try:
                return int(m.group(1))
            except (ValueError, TypeError):
                return None
        return None

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()
