"""
Kalshi prediction market scraper.
Uses Kalshi's trade API to fetch sports event odds.
Supports optional API key authentication for higher rate limits.
"""

import asyncio
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from sources.sport_mapping import (
    KALSHI_SERIES_TICKERS,
    canonical_event_id,
    cents_to_american,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# Kalshi ticker abbreviation → full team name (sport-specific)
KALSHI_TEAMS: Dict[str, Dict[str, str]] = {
    "basketball_nba": {
        "ATL": "Atlanta Hawks", "BOS": "Boston Celtics", "BKN": "Brooklyn Nets",
        "CHA": "Charlotte Hornets", "CHI": "Chicago Bulls", "CLE": "Cleveland Cavaliers",
        "DAL": "Dallas Mavericks", "DEN": "Denver Nuggets", "DET": "Detroit Pistons",
        "GSW": "Golden State Warriors", "HOU": "Houston Rockets", "IND": "Indiana Pacers",
        "LAC": "Los Angeles Clippers", "LAL": "Los Angeles Lakers", "MEM": "Memphis Grizzlies",
        "MIA": "Miami Heat", "MIL": "Milwaukee Bucks", "MIN": "Minnesota Timberwolves",
        "NOP": "New Orleans Pelicans", "NYK": "New York Knicks", "OKC": "Oklahoma City Thunder",
        "ORL": "Orlando Magic", "PHI": "Philadelphia 76ers", "PHX": "Phoenix Suns",
        "POR": "Portland Trail Blazers", "SAC": "Sacramento Kings", "SAS": "San Antonio Spurs",
        "TOR": "Toronto Raptors", "UTA": "Utah Jazz", "WAS": "Washington Wizards",
    },
    "americanfootball_nfl": {
        "ARI": "Arizona Cardinals", "ATL": "Atlanta Falcons", "BAL": "Baltimore Ravens",
        "BUF": "Buffalo Bills", "CAR": "Carolina Panthers", "CHI": "Chicago Bears",
        "CIN": "Cincinnati Bengals", "CLE": "Cleveland Browns", "DAL": "Dallas Cowboys",
        "DEN": "Denver Broncos", "DET": "Detroit Lions", "GB": "Green Bay Packers",
        "HOU": "Houston Texans", "IND": "Indianapolis Colts", "JAX": "Jacksonville Jaguars",
        "KC": "Kansas City Chiefs", "LV": "Las Vegas Raiders", "LAC": "Los Angeles Chargers",
        "LAR": "Los Angeles Rams", "MIA": "Miami Dolphins", "MIN": "Minnesota Vikings",
        "NE": "New England Patriots", "NO": "New Orleans Saints", "NYG": "New York Giants",
        "NYJ": "New York Jets", "PHI": "Philadelphia Eagles", "PIT": "Pittsburgh Steelers",
        "SF": "San Francisco 49ers", "SEA": "Seattle Seahawks", "TB": "Tampa Bay Buccaneers",
        "TEN": "Tennessee Titans", "WAS": "Washington Commanders",
    },
    "icehockey_nhl": {
        "ANA": "Anaheim Ducks", "BOS": "Boston Bruins", "BUF": "Buffalo Sabres",
        "CGY": "Calgary Flames", "CAR": "Carolina Hurricanes", "CHI": "Chicago Blackhawks",
        "COL": "Colorado Avalanche", "CBJ": "Columbus Blue Jackets", "DAL": "Dallas Stars",
        "DET": "Detroit Red Wings", "EDM": "Edmonton Oilers", "FLA": "Florida Panthers",
        "LA": "Los Angeles Kings", "MIN": "Minnesota Wild", "MTL": "Montreal Canadiens",
        "NSH": "Nashville Predators", "NJ": "New Jersey Devils", "NYI": "New York Islanders",
        "NYR": "New York Rangers", "OTT": "Ottawa Senators", "PHI": "Philadelphia Flyers",
        "PIT": "Pittsburgh Penguins", "SJ": "San Jose Sharks", "SEA": "Seattle Kraken",
        "STL": "St. Louis Blues", "TB": "Tampa Bay Lightning", "TOR": "Toronto Maple Leafs",
        "VAN": "Vancouver Canucks", "VGK": "Vegas Golden Knights", "WSH": "Washington Capitals",
        "WPG": "Winnipeg Jets", "UTA": "Utah Hockey Club",
    },
    "baseball_mlb": {
        "ARI": "Arizona Diamondbacks", "ATL": "Atlanta Braves", "BAL": "Baltimore Orioles",
        "BOS": "Boston Red Sox", "CHC": "Chicago Cubs", "CWS": "Chicago White Sox",
        "CIN": "Cincinnati Reds", "CLE": "Cleveland Guardians", "COL": "Colorado Rockies",
        "DET": "Detroit Tigers", "HOU": "Houston Astros", "KC": "Kansas City Royals",
        "LAA": "Los Angeles Angels", "LAD": "Los Angeles Dodgers", "MIA": "Miami Marlins",
        "MIL": "Milwaukee Brewers", "MIN": "Minnesota Twins", "NYM": "New York Mets",
        "NYY": "New York Yankees", "OAK": "Oakland Athletics", "PHI": "Philadelphia Phillies",
        "PIT": "Pittsburgh Pirates", "SD": "San Diego Padres", "SF": "San Francisco Giants",
        "SEA": "Seattle Mariners", "STL": "St. Louis Cardinals", "TB": "Tampa Bay Rays",
        "TEX": "Texas Rangers", "TOR": "Toronto Blue Jays", "WAS": "Washington Nationals",
    },
}


class KalshiSource(DataSource):
    """Fetches odds from Kalshi's public trade API."""

    # Class-level semaphore: limit to 10 concurrent Kalshi API requests
    # across all sport refreshes to avoid 429 rate limits.
    # Increased from 6 to handle 17 sport series (was 7 before).
    _api_sem = asyncio.Semaphore(10)

    def __init__(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        api_key = os.environ.get("KALSHI_API_KEY", "")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            logger.info("Kalshi: using authenticated API key")
        self._client = httpx.AsyncClient(timeout=25.0, headers=headers)
        # Mapping: event_ticker → {"home": mkt_ticker, "away": mkt_ticker}
        self._ticker_side_map = {}  # type: Dict[str, Dict[str, str]]
        # Per-sport cache: sport_key → (events, timestamp)
        import time as _time
        self._sport_cache = {}  # type: Dict[str, Tuple[List[OddsEvent], float]]
        self._sport_cache_ttl = 30  # seconds — don't refetch every 5s cycle

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        meta = {"x-requests-remaining": "unlimited"}
        if bookmakers and "kalshi" not in bookmakers:
            return [], meta

        # Return cached data if fresh (avoid refetching every 5s cycle)
        import time as _time
        cached = self._sport_cache.get(sport_key)
        if cached:
            events, ts = cached
            if (_time.time() - ts) < self._sport_cache_ttl:
                return events, meta

        series = KALSHI_SERIES_TICKERS.get(sport_key)
        if not series:
            return [], meta

        game_ticker = series.get("game")
        if not game_ticker:
            return [], meta

        try:
            spread_ticker = series.get("spread")
            total_ticker = series.get("total")

            # Fetch game, spread, and total series concurrently
            game_data, spread_data, total_data = await asyncio.gather(
                self._fetch_events(game_ticker),
                self._safe_fetch(spread_ticker),
                self._safe_fetch(total_ticker),
            )

            team_map = KALSHI_TEAMS.get(sport_key, {})
            sport_title = get_sport_title(sport_key)

            # Parse game events with date_teams keys for cross-series matching
            events_keyed = self._parse_game_events(game_data, sport_key, sport_title, team_map)

            # Build spread/total market maps keyed by date_teams portion
            spread_map = self._build_spread_map(spread_data, team_map)
            total_map = self._build_total_map(total_data)

            # Merge spread/total markets into game events
            events = []  # type: List[OddsEvent]
            for dt_key, ev in events_keyed:
                if ev.bookmakers and dt_key:
                    bm = ev.bookmakers[0]
                    if dt_key in spread_map:
                        bm.markets.append(spread_map[dt_key])
                    if dt_key in total_map:
                        bm.markets.append(total_map[dt_key])
                events.append(ev)

            # Fetch real orderbook liquidity at current prices for h2h markets.
            # This replaces the volume-based fallback with actual depth available
            # at the current yes_ask price.  Kalshi contracts = $1 each.
            # Wrap in 5s timeout so base events always return even if orderbook is slow.
            try:
                await asyncio.wait_for(
                    self._patch_orderbook_liquidity(events_keyed, game_data),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                logger.info("Kalshi: orderbook liquidity timed out (5s) for %s, returning without liquidity", sport_key)

            logger.info(
                "Kalshi: %d events for %s (spreads: %d, totals: %d)",
                len(events), sport_key, len(spread_map), len(total_map),
            )
            # Store in per-sport cache before returning
            self._sport_cache[sport_key] = (events, _time.time())
            return events, meta
        except Exception as e:
            logger.warning(f"Kalshi failed for {sport_key}: {type(e).__name__}: {e}")
            # Return stale cache if available on error
            stale = self._sport_cache.get(sport_key)
            if stale:
                stale_events, _ = stale
                logger.info("Kalshi: returning %d stale cached events for %s", len(stale_events), sport_key)
                return stale_events, meta
            return [], meta

    async def _fetch_events(self, series_ticker: str) -> list:
        url = f"{BASE_URL}/events"
        params = {
            "series_ticker": series_ticker,
            "with_nested_markets": "true",
            "status": "open",
            "limit": 200,
        }
        async with self._api_sem:
            for attempt in range(3):
                response = await self._client.get(url, params=params)
                if response.status_code == 429:
                    wait = 1.0 * (2 ** attempt)  # 1s, 2s, 4s
                    logger.debug("Kalshi 429 for %s, retrying in %.1fs", series_ticker, wait)
                    await asyncio.sleep(wait)
                    continue
                response.raise_for_status()
                return response.json().get("events", [])
            # Final attempt failed with 429
            response.raise_for_status()
            return []

    async def _safe_fetch(self, ticker: Optional[str]) -> list:
        """Fetch events for a ticker, returning [] on failure or if ticker is None."""
        if not ticker:
            return []
        try:
            return await self._fetch_events(ticker)
        except Exception as e:
            logger.debug("Kalshi: optional fetch failed for %s: %s", ticker, e)
            return []

    @staticmethod
    def _extract_date_teams_key(event_ticker: str) -> str:
        """Extract date+teams portion from event ticker for cross-series matching.

        E.g., 'KXNBAGAME-26FEB22CLEOKC' → '26FEB22CLEOKC'
        """
        parts = event_ticker.split("-")
        return parts[1] if len(parts) >= 2 else ""

    def _parse_game_events(
        self,
        events_data: list,
        sport_key: str,
        sport_title: str,
        team_map: Dict[str, str],
    ) -> List[Tuple[str, OddsEvent]]:
        """Parse game series events. Returns (date_teams_key, OddsEvent) pairs."""
        result = []  # type: List[Tuple[str, OddsEvent]]
        for event in events_data:
            parsed = self._parse_single_event(event, sport_key, sport_title, team_map)
            if parsed:
                dt_key = self._extract_date_teams_key(event.get("event_ticker", ""))
                result.append((dt_key, parsed))
        return result

    def _parse_events(self, events_data: list, sport_key: str) -> List[OddsEvent]:
        sport_title = get_sport_title(sport_key)
        team_map = KALSHI_TEAMS.get(sport_key, {})
        result = []

        for event in events_data:
            parsed = self._parse_single_event(event, sport_key, sport_title, team_map)
            if parsed:
                result.append(parsed)

        return result

    def _parse_single_event(
        self,
        event: dict,
        sport_key: str,
        sport_title: str,
        team_map: Dict[str, str],
    ) -> Optional[OddsEvent]:
        event_ticker = event.get("event_ticker", "")
        title = event.get("title", "")
        markets_data = event.get("markets", [])

        if len(markets_data) < 2:
            return None

        # Extract team abbreviations from market tickers
        # Market ticker: KXNBAGAME-26FEB22CLEOKC-CLE → suffix "CLE"
        market_by_abbr: Dict[str, dict] = {}
        draw_mkt = None  # type: Optional[dict]
        for mkt in markets_data:
            if mkt.get("status") != "active":
                continue
            ticker = mkt.get("ticker", "")
            abbr = ticker.rsplit("-", 1)[-1] if "-" in ticker else ""
            if abbr:
                # Detect draw/tie market (soccer 3-way)
                if abbr.upper() in ("TIE", "DRAW"):
                    draw_mkt = mkt
                    continue
                # Accept abbreviation if it's in the team map OR if no map exists
                # (college sports have 200+ teams, so we parse names from title)
                if team_map and abbr not in team_map:
                    continue
                market_by_abbr[abbr] = mkt

        if len(market_by_abbr) < 2:
            return None

        abbrs = list(market_by_abbr.keys())

        # Determine home/away from event_ticker
        # Format: SERIES-{YY}{MON}{DD}{AWAYABBR}{HOMEABBR}
        away_abbr, home_abbr = self._determine_sides(event_ticker, abbrs, title, team_map)

        # Resolve team names: use map if available, else parse from title
        if team_map:
            away_name = team_map.get(away_abbr, away_abbr)
            home_name = team_map.get(home_abbr, home_abbr)
        else:
            away_name, home_name = self._names_from_title(title, away_abbr, home_abbr)

        # Normalize through alias resolution for clean display + canonical matching
        away_name = resolve_team_name(away_name)
        home_name = resolve_team_name(home_name)

        # Convert cent prices to American odds
        away_mkt = market_by_abbr[away_abbr]
        home_mkt = market_by_abbr[home_abbr]

        # Save market ticker → side mapping for per-outcome liquidity patching
        home_ticker = home_mkt.get("ticker", "")
        away_ticker = away_mkt.get("ticker", "")
        if event_ticker and home_ticker and away_ticker:
            self._ticker_side_map[event_ticker] = {
                "home": home_ticker,
                "away": away_ticker,
            }

        away_price = away_mkt.get("yes_ask")
        home_price = home_mkt.get("yes_ask")

        if not away_price or not home_price:
            return None
        if away_price <= 0 or away_price >= 100 or home_price <= 0 or home_price >= 100:
            return None

        away_odds = cents_to_american(away_price)
        home_odds = cents_to_american(home_price)

        # Estimate game start from expected_expiration_time (which is game end).
        # Subtract ~3 hours so the UTC date aligns with other sources.
        expiration = away_mkt.get("expected_expiration_time", "")
        commence_time = self._estimate_start_time(expiration)
        if not commence_time:
            commence_time = self._parse_date_from_ticker(event_ticker)

        cid = canonical_event_id(sport_key, home_name, away_name, commence_time)

        # Build h2h market outcomes
        h2h_outcomes = [
            Outcome(name=home_name, price=home_odds),
            Outcome(name=away_name, price=away_odds),
        ]

        # Add draw outcome for soccer 3-way markets
        if draw_mkt:
            draw_price = draw_mkt.get("yes_ask")
            if draw_price and 0 < draw_price < 100:
                draw_odds = cents_to_american(draw_price)
                h2h_outcomes.append(Outcome(name="Draw", price=draw_odds))

        h2h_market = Market(key="h2h", outcomes=h2h_outcomes)

        # Build event deep-link URL from event ticker
        event_url = f"https://kalshi.com/markets/{event_ticker}" if event_ticker else None

        return OddsEvent(
            id=cid,
            sport_key=sport_key,
            sport_title=sport_title,
            commence_time=commence_time,
            home_team=home_name,
            away_team=away_name,
            bookmakers=[
                Bookmaker(key="kalshi", title="Kalshi", markets=[h2h_market], event_url=event_url)
            ],
        )

    # Regex to strip sport/event prefixes from Kalshi titles
    # Handles: "UFC 326: ", "UFC Fight Night: ", "MMA: ", "NBA: ", etc.
    _TITLE_PREFIX_RE = re.compile(
        r"^(?:UFC|MMA|NBA|NFL|NHL|MLB|NCAAB|NCAAF|WNBA|EPL|Boxing)"
        r"(?:\s+\d+|\s+Fight\s+Night)?\s*:\s*",
        re.IGNORECASE,
    )

    def _names_from_title(
        self, title: str, away_abbr: str, home_abbr: str
    ) -> Tuple[str, str]:
        """Extract team names from event title like 'Louisville at North Carolina'.

        Handles UFC-style prefixes: 'UFC 326: Holloway vs Oliveira' → ('Holloway', 'Oliveira')
        """
        # Strip sport/event prefix (e.g. "UFC 326: ")
        clean = self._TITLE_PREFIX_RE.sub("", title).strip()

        # Try "Away at Home" pattern (with optional " Winner?" suffix)
        match = re.match(r"(.+?)\s+at\s+(.+?)(?:\s+Winner\??)?$", clean, re.IGNORECASE)
        if match:
            return match.group(1).strip(), match.group(2).strip()
        # Try "Away vs Home" or "Away vs. Home"
        match = re.match(r"(.+?)\s+vs\.?\s+(.+?)(?:\s+Winner\??)?$", clean, re.IGNORECASE)
        if match:
            return match.group(1).strip(), match.group(2).strip()
        # Fallback: return abbreviations
        return away_abbr, home_abbr

    def _determine_sides(
        self,
        event_ticker: str,
        abbrs: List[str],
        title: str,
        team_map: Dict[str, str],
    ) -> Tuple[str, str]:
        """Determine which abbreviation is away and which is home."""
        # Try parsing from event_ticker: SERIES-{YYMONDD}{AWAY}{HOME}
        parts = event_ticker.split("-")
        if len(parts) >= 2:
            date_teams = parts[-1]  # e.g., "26FEB22CLEOKC"
            if len(date_teams) > 7:
                teams_str = date_teams[7:]  # e.g., "CLEOKC"
                # Try both orderings of the two abbreviations
                if teams_str == abbrs[0] + abbrs[1]:
                    return abbrs[0], abbrs[1]
                elif teams_str == abbrs[1] + abbrs[0]:
                    return abbrs[1], abbrs[0]

        # Fallback: parse from title "{Away} at {Home} Winner?"
        match = re.match(r"(.+?)\s+at\s+(.+?)(?:\s+Winner\??)?$", title, re.IGNORECASE)
        if match:
            away_city = match.group(1).strip().lower()
            home_city = match.group(2).strip().lower()
            if team_map:
                # Match abbreviations against full team names in the map
                for ab in abbrs:
                    full_name = team_map.get(ab, "").lower()
                    if away_city in full_name:
                        other = [a for a in abbrs if a != ab]
                        if other:
                            return ab, other[0]
                    elif home_city in full_name:
                        other = [a for a in abbrs if a != ab]
                        if other:
                            return other[0], ab
            else:
                # No team map (college sports): match abbreviation against title position
                # The ticker suffix for away team typically appears first in the date_teams
                # Just return abbrs in order — _names_from_title will use the title anyway
                return abbrs[0], abbrs[1]

        # Last resort: first is away, second is home
        return abbrs[0], abbrs[1]

    # ------------------------------------------------------------------
    # Orderbook liquidity
    # ------------------------------------------------------------------

    async def _fetch_orderbook(self, ticker: str) -> Optional[dict]:
        """Fetch the orderbook for a single market ticker."""
        try:
            async with self._api_sem:
                resp = await self._client.get(f"{BASE_URL}/markets/{ticker}/orderbook")
                if resp.status_code == 429:
                    await asyncio.sleep(1.0)
                    resp = await self._client.get(f"{BASE_URL}/markets/{ticker}/orderbook")
                resp.raise_for_status()
                return resp.json().get("orderbook")
        except Exception:
            return None

    async def _patch_orderbook_liquidity(
        self,
        events_keyed: List[Tuple[str, "OddsEvent"]],
        game_events_data: list,
    ) -> None:
        """Fetch orderbooks and patch per-outcome liquidity at best ask.

        For each side's market, calculates the depth available at the
        current yes_ask price (NO bid depth at 100 - yes_ask) and sets
        it as that outcome's liquidity. This shows how much you can
        actually bet on each side at the displayed price.

        Rate limited: max 40 markets (20 events), batched 5 at a time
        with 350ms delay between batches.
        """
        # Collect market tickers + yes_ask + volume from raw event data.
        market_info = []  # type: List[Tuple[str, str, int, int]]
        event_ticker_map = {}  # type: Dict[str, List[str]]

        for event in game_events_data:
            et = event.get("event_ticker", "")
            mkt_tickers = []
            for mkt in event.get("markets", []):
                if mkt.get("status") != "active":
                    continue
                tk = mkt.get("ticker", "")
                ya = mkt.get("yes_ask")
                vol = mkt.get("volume") or 0
                if tk and ya:
                    market_info.append((et, tk, ya, vol))
                    mkt_tickers.append(tk)
            if mkt_tickers:
                event_ticker_map[et] = mkt_tickers

        if not market_info:
            return

        # Sort by volume descending, cap at 40 markets (= ~20 games)
        market_info.sort(key=lambda x: x[3], reverse=True)
        market_info = market_info[:40]

        ticker_to_ask = {}  # type: Dict[str, int]
        for _, tk, ya, _ in market_info:
            ticker_to_ask[tk] = ya

        tickers_to_fetch = [tk for _, tk, _, _ in market_info]

        # Fetch orderbooks in small batches with rate limit delays
        orderbooks = {}  # type: Dict[str, Optional[dict]]
        batch_size = 5
        for i in range(0, len(tickers_to_fetch), batch_size):
            if i > 0:
                await asyncio.sleep(0.35)
            batch = tickers_to_fetch[i:i + batch_size]
            results = await asyncio.gather(
                *[self._fetch_orderbook(tk) for tk in batch],
                return_exceptions=True,
            )
            for tk, ob in zip(batch, results):
                if isinstance(ob, dict):
                    orderbooks[tk] = ob

        # Calculate liquidity at yes_ask for each market ticker.
        # Depth = NO bid quantity at (100 - yes_ask) cents.
        # Dollar value = depth * (yes_ask / 100) since each contract costs yes_ask cents.
        ticker_liq = {}  # type: Dict[str, float]

        for tk, ya in ticker_to_ask.items():
            ob = orderbooks.get(tk)
            if not ob:
                continue
            no_side = ob.get("no") or []
            target_price = 100 - ya
            depth = 0
            for price, qty in no_side:
                if price == target_price:
                    depth = qty
                    break
            # Dollar liquidity at this price
            dollar_liq = float(depth) * (ya / 100.0)
            ticker_liq[tk] = round(dollar_liq, 2)

        # Patch per-outcome liquidity into OddsEvent h2h markets
        patched = 0
        for dt_key, ev in events_keyed:
            if not ev.bookmakers:
                continue
            bm = ev.bookmakers[0]

            # Find the raw event ticker matching this parsed event
            et = None
            for event in game_events_data:
                raw_et = event.get("event_ticker", "")
                raw_dt = self._extract_date_teams_key(raw_et)
                if raw_dt == dt_key:
                    et = raw_et
                    break
            if not et:
                continue

            # Get home/away ticker mapping (set during parsing)
            side_map = self._ticker_side_map.get(et)
            if not side_map:
                continue

            home_tk = side_map.get("home", "")
            away_tk = side_map.get("away", "")
            home_liq = ticker_liq.get(home_tk)
            away_liq = ticker_liq.get(away_tk)

            if home_liq is None and away_liq is None:
                continue

            for market in bm.markets:
                if market.key != "h2h" or len(market.outcomes) < 2:
                    continue
                # outcomes[0] = home, outcomes[1] = away (set in _parse_single_event)
                if home_liq is not None and home_liq > 0:
                    market.outcomes[0].liquidity = home_liq
                if away_liq is not None and away_liq > 0:
                    market.outcomes[1].liquidity = away_liq
                # Market-level liquidity = sum of both sides
                both = [l for l in [home_liq, away_liq] if l and l > 0]
                if both:
                    market.liquidity = round(sum(both), 2)
                patched += 1
                break

        logger.info("Kalshi: patched per-side orderbook liquidity for %d/%d events", patched, len(events_keyed))
        # Clear side map for next call
        self._ticker_side_map = {}

    def _estimate_start_time(self, expected_expiration: str) -> str:
        """Estimate game start from Kalshi's expected expiration (game end) time."""
        if not expected_expiration:
            return ""
        try:
            dt = datetime.fromisoformat(expected_expiration.replace("Z", "+00:00"))
            start = dt - timedelta(hours=3)
            return start.strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            return expected_expiration

    def _parse_date_from_ticker(self, event_ticker: str) -> str:
        """Parse game date from Kalshi event ticker like KXNBAGAME-26FEB22CLEOKC."""
        parts = event_ticker.split("-")
        if len(parts) < 2:
            return ""
        date_teams = parts[-1]
        if len(date_teams) < 7:
            return ""
        try:
            year = 2000 + int(date_teams[0:2])
            month_str = date_teams[2:5].upper()
            day = int(date_teams[5:7])
            month = MONTH_MAP.get(month_str)
            if not month:
                return ""
            return f"{year}-{month:02d}-{day:02d}T00:00:00Z"
        except (ValueError, IndexError):
            return ""

    # ------------------------------------------------------------------
    # Spread / Total market builders
    # ------------------------------------------------------------------

    def _build_spread_map(
        self, spread_events: list, team_map: Dict[str, str]
    ) -> Dict[str, Market]:
        """Build date_teams_key → spread Market from spread series events.

        Each Kalshi spread event contains multiple binary markets at different
        point thresholds (e.g., "Team wins by 3.5+", "Team wins by 5.5+").
        We pick the market closest to 50 cents yes_ask (the "main line").
        """
        result = {}  # type: Dict[str, Market]
        for event in spread_events:
            event_ticker = event.get("event_ticker", "")
            dt_key = self._extract_date_teams_key(event_ticker)
            if not dt_key:
                continue

            markets_data = event.get("markets", [])
            if not markets_data:
                continue

            # Find the active market closest to 50 cents
            best = None  # type: Optional[dict]
            best_dist = 100
            for mkt in markets_data:
                if mkt.get("status") != "active":
                    continue
                yes_ask = mkt.get("yes_ask")
                if not yes_ask or yes_ask <= 1 or yes_ask >= 99:
                    continue
                dist = abs(yes_ask - 50)
                if dist < best_dist:
                    best_dist = dist
                    best = mkt

            if not best:
                continue

            spread_market = self._parse_spread_from_market(best, dt_key, team_map)
            if spread_market:
                result[dt_key] = spread_market

        return result

    def _parse_spread_from_market(
        self,
        market: dict,
        dt_key: str,
        team_map: Dict[str, str],
    ) -> Optional[Market]:
        """Parse a single Kalshi spread market into a spreads Market."""
        ticker = market.get("ticker", "")
        yes_ask = market.get("yes_ask")
        if not yes_ask:
            return None

        # Extract team abbreviation and point from ticker suffix
        # Ticker: KXNBASPREAD-26FEB22CLEOKC-CLE5P5
        parts = ticker.split("-")
        if len(parts) < 3:
            return None

        suffix = parts[-1]  # e.g., "CLE5P5" or "CLEPLUS5P5"

        # Parse team abbr + numeric point value
        # Handle: CLE5P5, CLE5, CLEPLUS5P5, CLE10, GSW3P5, etc.
        match = re.match(r"([A-Z]{2,4})(?:PLUS|MINUS)?(\d+(?:P\d+)?)", suffix)
        if not match:
            return None

        fav_abbr = match.group(1)
        point_str = match.group(2).replace("P", ".")
        try:
            point = float(point_str)
        except ValueError:
            return None

        # Determine the other team from the date_teams key
        # dt_key: "26FEB22CLEOKC" → teams_str: "CLEOKC"
        teams_str = dt_key[7:] if len(dt_key) > 7 else ""

        other_abbr = ""
        if teams_str.startswith(fav_abbr):
            other_abbr = teams_str[len(fav_abbr):]
        elif teams_str.endswith(fav_abbr):
            other_abbr = teams_str[: len(teams_str) - len(fav_abbr)]

        if not other_abbr:
            return None

        # Resolve team names
        fav_name = team_map.get(fav_abbr, fav_abbr) if team_map else fav_abbr
        other_name = team_map.get(other_abbr, other_abbr) if team_map else other_abbr

        # Calculate American odds
        fav_odds = cents_to_american(yes_ask)
        no_ask = market.get("no_ask")
        no_price = no_ask if no_ask and 0 < no_ask < 100 else max(1, min(99, 100 - yes_ask))
        other_odds = cents_to_american(no_price)

        return Market(
            key="spreads",
            outcomes=[
                Outcome(name=fav_name, price=fav_odds, point=-point),
                Outcome(name=other_name, price=other_odds, point=point),
            ],
        )

    def _build_total_map(self, total_events: list) -> Dict[str, Market]:
        """Build date_teams_key → total Market from total series events.

        Each Kalshi total event contains markets at different point thresholds
        (e.g., "Over 215.5", "Over 220.5"). We pick closest to 50 cents.
        """
        result = {}  # type: Dict[str, Market]
        for event in total_events:
            event_ticker = event.get("event_ticker", "")
            dt_key = self._extract_date_teams_key(event_ticker)
            if not dt_key:
                continue

            markets_data = event.get("markets", [])
            if not markets_data:
                continue

            best = None  # type: Optional[dict]
            best_dist = 100
            for mkt in markets_data:
                if mkt.get("status") != "active":
                    continue
                yes_ask = mkt.get("yes_ask")
                if not yes_ask or yes_ask <= 1 or yes_ask >= 99:
                    continue
                dist = abs(yes_ask - 50)
                if dist < best_dist:
                    best_dist = dist
                    best = mkt

            if not best:
                continue

            total_market = self._parse_total_from_market(best)
            if total_market:
                result[dt_key] = total_market

        return result

    def _parse_total_from_market(self, market: dict) -> Optional[Market]:
        """Parse a single Kalshi total market into a totals Market."""
        ticker = market.get("ticker", "")
        yes_ask = market.get("yes_ask")
        if not yes_ask:
            return None

        # Extract total point from ticker suffix
        # Ticker: KXNBATOTAL-26FEB22CLEOKC-T220P5 (or O220P5, U220P5, 220P5)
        parts = ticker.split("-")
        if len(parts) < 3:
            return None

        suffix = parts[-1]  # e.g., "T220P5" or "O220P5" or "220P5"

        # Parse numeric value with optional letter prefix
        match = re.match(r"[A-Z]?(\d+(?:P\d+)?)", suffix)
        if not match:
            return None

        point_str = match.group(1).replace("P", ".")
        try:
            point = float(point_str)
        except ValueError:
            return None

        # Yes = Over, No = Under
        over_odds = cents_to_american(yes_ask)
        no_ask = market.get("no_ask")
        no_price = no_ask if no_ask and 0 < no_ask < 100 else max(1, min(99, 100 - yes_ask))
        under_odds = cents_to_american(no_price)

        return Market(
            key="totals",
            outcomes=[
                Outcome(name="Over", price=over_odds, point=point),
                Outcome(name="Under", price=under_odds, point=point),
            ],
        )

    async def close(self) -> None:
        await self._client.aclose()
