"""
SBR (SportsbookReview) Odds Scraper
====================================
Extracts odds from sportsbookreview.com via their Next.js server-side rendered pages.
This provides DraftKings, Caesars, BetMGM, FanDuel (SBR), bet365, and Fanatics
lines for NBA, NCAAB, NHL, and MLB — all without any API key or Cloudflare issues.

Each request fetches a single market type for a single sport, so we make
multiple requests per sport and merge them together.
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome, PlayerProp
from sources.base import DataSource
from sources.sport_mapping import canonical_event_id

logger = logging.getLogger(__name__)

# ── SBR URL mapping ──────────────────────────────────────────────────────────
# Maps our internal sport_key to the SBR URL slug
_SPORT_SLUGS: Dict[str, str] = {
    "basketball_nba": "nba-basketball",
    "basketball_ncaab": "ncaa-basketball",
    "icehockey_nhl": "nhl-hockey",
    "baseball_mlb": "mlb-baseball",
}

# Maps our internal sport_key to SBR display name
_SPORT_TITLES: Dict[str, str] = {
    "basketball_nba": "NBA",
    "basketball_ncaab": "NCAAB",
    "icehockey_nhl": "NHL",
    "baseball_mlb": "MLB",
}

# Market type → SBR URL fragment(s)
# For each market we need, define (url_market, url_period, our_market_key)
_MARKET_MAP: Dict[str, List[Tuple[str, str, str]]] = {
    "basketball_nba": [
        ("money-line", "full-game", "h2h"),
        ("pointspread", "full-game", "spreads"),
        ("totals", "full-game", "totals"),
        ("money-line", "1st-half", "h2h_h1"),
        ("pointspread", "1st-half", "spreads_h1"),
        ("totals", "1st-half", "totals_h1"),
        ("money-line", "1st-quarter", "h2h_q1"),
        ("pointspread", "1st-quarter", "spreads_q1"),
        ("totals", "1st-quarter", "totals_q1"),
    ],
    "basketball_ncaab": [
        ("money-line", "full-game", "h2h"),
        ("pointspread", "full-game", "spreads"),
        ("totals", "full-game", "totals"),
        ("money-line", "1st-half", "h2h_h1"),
        ("pointspread", "1st-half", "spreads_h1"),
        ("totals", "1st-half", "totals_h1"),
    ],
    "icehockey_nhl": [
        ("money-line", "full-game", "h2h"),
        ("pointspread", "full-game", "spreads"),
        ("totals", "full-game", "totals"),
        # NOTE: SBR returns 500 for NHL 1st-period pages (no data available).
        # Our other scrapers (Pinnacle, BetRivers, Bookmaker) cover period markets.
    ],
    "baseball_mlb": [
        ("money-line", "full-game", "h2h"),
        ("pointspread", "full-game", "spreads"),
        ("totals", "full-game", "totals"),
        # NOTE: SBR returns 500 for F5 innings pages during spring training.
        # Our other scrapers cover period markets.
    ],
}

# SBR sportsbook key → our internal key
# NOTE: Caesars uses "williamhill_us" internally (legacy from William Hill merger)
_BOOK_KEY_MAP: Dict[str, str] = {
    "draftkings": "draftkings",
    "caesars": "williamhill_us",
    "betmgm": "betmgm",
    "fanduel": "fanduel_sbr",  # Suffix to distinguish from our direct FanDuel scraper
    "bet365": "bet365",
    "fanatics": "fanatics",
}

# SBR sportsbook key → display title
_BOOK_TITLE_MAP: Dict[str, str] = {
    "draftkings": "DraftKings",
    "caesars": "Caesars",
    "betmgm": "BetMGM",
    "fanduel": "FanDuel (SBR)",
    "bet365": "bet365",
    "fanatics": "Fanatics",
}

# Scrape ALL available SBR books for maximum coverage
_TARGET_BOOKS = frozenset(["draftkings", "caesars", "betmgm", "bet365", "fanatics"])

_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL
)

_BASE_URL = "https://www.sportsbookreview.com/betting-odds"


class SBRSource(DataSource):
    """
    Scrapes odds from SportsbookReview.com via their Next.js SSR pages.

    This provides DraftKings and Caesars odds for 4 major sports
    (NBA, NCAAB, NHL, MLB) with full-game and period markets.
    """

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(20.0, connect=10.0),
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:

        slug = _SPORT_SLUGS.get(sport_key)
        if not slug:
            return [], {"x-requests-remaining": "999"}

        market_defs = _MARKET_MAP.get(sport_key, [])
        if not market_defs:
            return [], {"x-requests-remaining": "999"}

        # Fetch all market pages concurrently (with small semaphore to be nice)
        sem = asyncio.Semaphore(4)

        async def _fetch_market(url_market: str, url_period: str, market_key: str):
            async with sem:
                return await self._fetch_page(slug, url_market, url_period, market_key, sport_key)

        tasks = [
            _fetch_market(url_mkt, url_per, mkt_key)
            for url_mkt, url_per, mkt_key in market_defs
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Merge all market results into unified events
        # Key: game_id → {event_data, bookmakers: {book_key: {markets: [Market]}}}
        events_map: Dict[int, Dict[str, Any]] = {}

        for result in results:
            if isinstance(result, Exception):
                logger.warning("SBR market fetch failed: %s", result)
                continue
            if not result:
                continue

            for game_data in result:
                game_id = game_data["game_id"]
                if game_id not in events_map:
                    events_map[game_id] = {
                        "game_id": game_id,
                        "home_team": game_data["home_team"],
                        "away_team": game_data["away_team"],
                        "commence_time": game_data["commence_time"],
                        "bookmakers": {},  # book_key → list of Markets
                    }

                # Merge bookmaker markets
                for book_key, book_markets in game_data.get("book_markets", {}).items():
                    if book_key not in events_map[game_id]["bookmakers"]:
                        events_map[game_id]["bookmakers"][book_key] = []
                    events_map[game_id]["bookmakers"][book_key].extend(book_markets)

        # Convert to OddsEvent objects
        events: List[OddsEvent] = []
        sport_title = _SPORT_TITLES.get(sport_key, sport_key)

        for gid, edata in events_map.items():
            bookmakers_list = []
            for book_key, market_list in edata["bookmakers"].items():
                our_key = _BOOK_KEY_MAP.get(book_key, book_key)
                title = _BOOK_TITLE_MAP.get(book_key, book_key.title())
                bookmakers_list.append(
                    Bookmaker(
                        key=our_key,
                        title=title,
                        markets=market_list,
                    )
                )

            if not bookmakers_list:
                continue

            # Generate a canonical event ID so the composite can merge
            # SBR events with events from other sources by matching on
            # sport + teams + date (instead of an opaque md5 hash).
            event_id = canonical_event_id(
                sport_key,
                edata["home_team"],
                edata["away_team"],
                edata["commence_time"],
            )

            events.append(
                OddsEvent(
                    id=event_id,
                    sport_key=sport_key,
                    sport_title=sport_title,
                    commence_time=edata["commence_time"],
                    home_team=edata["home_team"],
                    away_team=edata["away_team"],
                    bookmakers=bookmakers_list,
                )
            )

        logger.info(
            "SBR %s: %d events, %d market pages fetched",
            sport_key, len(events), len(market_defs),
        )

        return events, {"x-requests-remaining": "999"}

    async def _fetch_page(
        self,
        sport_slug: str,
        url_market: str,
        url_period: str,
        market_key: str,
        sport_key: str,
    ) -> Optional[List[Dict[str, Any]]]:
        """Fetch a single SBR odds page and extract game + odds data."""

        url = f"{_BASE_URL}/{sport_slug}/{url_market}/{url_period}/"
        try:
            resp = await self._client.get(url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning("SBR fetch failed %s: %s", url, e)
            return None

        html = resp.text
        match = _NEXT_DATA_RE.search(html)
        if not match:
            logger.warning("SBR: No __NEXT_DATA__ in %s", url)
            return None

        try:
            next_data = json.loads(match.group(1))
        except json.JSONDecodeError:
            logger.warning("SBR: Bad JSON in %s", url)
            return None

        page_props = next_data.get("props", {}).get("pageProps", {})
        odds_tables = page_props.get("oddsTables", [])
        if not odds_tables:
            return None

        results = []
        for table in odds_tables:
            model = table.get("oddsTableModel", {})
            rows = model.get("gameRows", [])
            for row in rows:
                game_data = self._parse_game_row(row, market_key, url_market)
                if game_data:
                    results.append(game_data)

        return results

    def _parse_game_row(
        self, row: Dict[str, Any], market_key: str, url_market: str
    ) -> Optional[Dict[str, Any]]:
        """Parse a single game row from SBR's Next.js data."""

        game_view = row.get("gameView", {})
        if not game_view:
            return None

        game_id = game_view.get("gameId")
        if not game_id:
            return None

        # Skip live games
        status = game_view.get("status", "")
        if status in ("2", "3"):  # 2=live, 3=final
            return None

        home_team_data = game_view.get("homeTeam", {})
        away_team_data = game_view.get("awayTeam", {})
        home_team = home_team_data.get("fullName", home_team_data.get("name", ""))
        away_team = away_team_data.get("fullName", away_team_data.get("name", ""))

        if not home_team or not away_team:
            return None

        commence_time = game_view.get("startDate", "")
        if not commence_time:
            return None

        # Parse odds views (one per sportsbook)
        odds_views = row.get("oddsViews", [])
        book_markets: Dict[str, List[Market]] = {}

        for ov in odds_views:
            if not ov:
                continue

            sbr_book = ov.get("sportsbook", "")
            if sbr_book not in _TARGET_BOOKS:
                continue

            current_line = ov.get("currentLine")
            if not current_line:
                continue

            market = self._parse_line(current_line, market_key, url_market, home_team, away_team)
            if market:
                book_markets.setdefault(sbr_book, []).append(market)

        if not book_markets:
            return None

        return {
            "game_id": game_id,
            "home_team": home_team,
            "away_team": away_team,
            "commence_time": commence_time,
            "book_markets": book_markets,
        }

    def _parse_line(
        self,
        line: Dict[str, Any],
        market_key: str,
        url_market: str,
        home_team: str,
        away_team: str,
    ) -> Optional[Market]:
        """Parse a currentLine object into a Market."""

        outcomes: List[Outcome] = []

        if url_market == "money-line":
            home_odds = line.get("homeOdds")
            away_odds = line.get("awayOdds")
            if home_odds is None and away_odds is None:
                return None
            if home_odds is not None:
                outcomes.append(Outcome(name=home_team, price=int(home_odds)))
            if away_odds is not None:
                outcomes.append(Outcome(name=away_team, price=int(away_odds)))

        elif url_market == "pointspread":
            home_odds = line.get("homeOdds")
            away_odds = line.get("awayOdds")
            home_spread = line.get("homeSpread")
            away_spread = line.get("awaySpread")
            if home_odds is None and away_odds is None:
                return None
            if home_odds is not None and home_spread is not None:
                outcomes.append(
                    Outcome(name=home_team, price=int(home_odds), point=float(home_spread))
                )
            if away_odds is not None and away_spread is not None:
                outcomes.append(
                    Outcome(name=away_team, price=int(away_odds), point=float(away_spread))
                )

        elif url_market == "totals":
            over_odds = line.get("overOdds")
            under_odds = line.get("underOdds")
            total = line.get("total")
            if over_odds is None and under_odds is None:
                return None
            if over_odds is not None and total is not None:
                outcomes.append(
                    Outcome(name="Over", price=int(over_odds), point=float(total))
                )
            if under_odds is not None and total is not None:
                outcomes.append(
                    Outcome(name="Under", price=int(under_odds), point=float(total))
                )

        if not outcomes:
            return None

        return Market(key=market_key, outcomes=outcomes)

    async def close(self) -> None:
        await self._client.aclose()

    async def get_player_props(
        self, sport_key: str, event_id: str
    ) -> List[PlayerProp]:
        return []
