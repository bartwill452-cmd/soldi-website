"""
Novig P2P exchange scraper.

Uses Novig's public GraphQL API at api.novig.us to fetch odds data.
No authentication required for public market data.

Novig is a peer-to-peer sports prediction exchange. Prices are derived from the
order book using buy_price = 1 - opponent's best bid (the actual tradeable ASK price).
The `available` probability field is only used as a fallback when no order book data
exists. We convert probabilities to American odds for consistency with other sources.

Liquidity is fetched from the order book batch endpoint for ALL market types.
For each outcome, we show the available liquidity at the displayed price.

API Base: https://api.novig.us/v1/graphql
Order Book: https://api.novig.us/nbx/v1/markets/book/batch
Markets: MONEY (h2h), SPREAD, TOTAL, TEAM_TOTAL, MONEY_1H, SPREAD_1H, TOTAL_1H, TEAM_TOTAL_1H
"""

import asyncio
import logging
import math
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

GRAPHQL_URL = "https://api.novig.us/v1/graphql"
ORDERBOOK_URL = "https://api.novig.us/nbx/v1/markets/book/batch"

# OddsScreen sport_key -> Novig league string
NOVIG_LEAGUES = {
    "basketball_nba": "NBA",
    "americanfootball_nfl": "NFL",
    "icehockey_nhl": "NHL",
    "baseball_mlb": "MLB",
    "basketball_ncaab": "NCAAB",
    "americanfootball_ncaaf": "NCAAF",
    "mma_mixed_martial_arts": "UFC",
    "soccer_epl": "EPL",
    "soccer_spain_la_liga": "La Liga",
    "soccer_germany_bundesliga": "Bundesliga",
    "soccer_italy_serie_a": "Serie A",
    "soccer_france_ligue_one": "Ligue 1",
    "soccer_uefa_champs_league": "Champions League",
    "tennis_atp": "ATP",
    "tennis_wta": "WTA",
    "boxing_boxing": "Boxing",
}  # type: Dict[str, str]

# Novig market type -> OddsScreen market key
_MARKET_TYPE_MAP = {
    "MONEY": "h2h",
    "SPREAD": "spreads",
    "TOTAL": "totals",
    "TEAM_TOTAL": "team_total",
    "MONEY_1H": "h2h_h1",
    "SPREAD_1H": "spreads_h1",
    "TOTAL_1H": "totals_h1",
    "TEAM_TOTAL_1H": "team_total_h1",
}  # type: Dict[str, str]

# Market types we care about (used in GraphQL query filter)
_WANTED_TYPES = list(_MARKET_TYPE_MAP.keys())

# Cache TTL
_CACHE_TTL = 10  # seconds

# GraphQL query to fetch events with their markets and odds
_EVENTS_QUERY = """
query GetEvents($where_event: event_bool_exp!, $limit: Int!) @cached(ttl: 5) {
  event(where: $where_event, order_by: {scheduled_start: asc}, limit: $limit) {
    id
    type
    description
    status
    league
    scheduled_start
    game {
      homeTeam { name symbol short_name }
      awayTeam { name symbol short_name }
      sport
    }
    markets(where: {status: {_eq: "OPEN"}, type: {_in: [%TYPES%]}}) {
      id
      type
      strike
      status
      outcomes {
        id
        index
        description
        available
        altAvailable
      }
    }
  }
}
""".replace("%TYPES%", ", ".join(f'"{t}"' for t in _WANTED_TYPES))


def _prob_to_american(prob: float) -> int:
    """Convert probability (0.0-1.0) to American odds."""
    if prob is None or prob <= 0 or prob >= 1:
        return 0
    if prob >= 0.5:
        return round(-100 * prob / (1 - prob))
    else:
        return round(100 * (1 - prob) / prob)


def _pick_consensus_line(
    markets_of_type: list,
) -> Optional[dict]:
    """
    Given multiple alternate lines of the same type (e.g., many SPREAD lines),
    pick the one closest to a 50/50 split (the 'consensus' / primary line).
    For MONEY markets (no alternates), just return the single one.
    """
    if not markets_of_type:
        return None
    if len(markets_of_type) == 1:
        return markets_of_type[0]

    best = None
    best_score = float("inf")
    for mkt in markets_of_type:
        outcomes = mkt.get("outcomes", [])
        # Try available first, fall back to altAvailable
        probs = []
        for o in outcomes:
            p = o.get("available")
            if p is None:
                p = o.get("altAvailable")
            if p is not None:
                probs.append(p)
        if len(probs) < 1:
            continue
        # If only one side has a prob, infer the other for scoring
        if len(probs) == 1:
            probs.append(1.0 - probs[0])
        # Score: how far the probabilities are from 0.5/0.5
        score = sum(abs(p - 0.5) for p in probs[:2])
        if score < best_score:
            best_score = score
            best = mkt
    return best if best else markets_of_type[0]


class NovigSource(DataSource):
    """Fetches odds from Novig's public GraphQL API."""

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            },
        )
        self._cache = {}  # type: Dict[str, Tuple[List[OddsEvent], float]]

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        if bookmakers and "novig" not in bookmakers:
            return [], {"x-requests-remaining": "unlimited"}

        league = NOVIG_LEAGUES.get(sport_key)
        if league is None:
            return [], {"x-requests-remaining": "unlimited"}

        # Check cache
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _CACHE_TTL:
            return cached[0], {"x-requests-remaining": "unlimited"}

        try:
            events = await self._fetch_events(sport_key, league)
            self._cache[sport_key] = (events, time.time())
            logger.info("Novig: %d events for %s", len(events), sport_key)
            return events, {"x-requests-remaining": "unlimited"}
        except Exception as e:
            logger.warning("Novig failed for %s: %s", sport_key, e)
            # Return stale cache if available
            if cached:
                return cached[0], {"x-requests-remaining": "unlimited"}
            return [], {"x-requests-remaining": "unlimited"}

    async def _fetch_events(self, sport_key: str, league: str) -> List[OddsEvent]:
        """Fetch events for a league from the Novig GraphQL API."""
        variables = {
            "where_event": {
                "_and": [
                    {"league": {"_eq": league}},
                    {"status": {"_in": ["OPEN_PREGAME", "OPEN_INGAME", "CLOSED_PREGAME"]}},
                    {"type": {"_eq": "Game"}},
                ]
            },
            "limit": 50,
        }

        resp = await self._client.post(
            GRAPHQL_URL,
            json={"query": _EVENTS_QUERY, "variables": variables},
        )
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            logger.warning("Novig GraphQL errors: %s", data["errors"])
            return []

        raw_events = data.get("data", {}).get("event", [])

        # Collect ALL market IDs for order book buy-price + liquidity fetch
        all_market_ids = []  # type: List[str]
        for ev in raw_events:
            for mkt in ev.get("markets", []):
                if mkt.get("type") in _MARKET_TYPE_MAP and mkt.get("id"):
                    all_market_ids.append(mkt["id"])

        # Fetch order book data for all markets in one batch
        liquidity_map = await self._fetch_orderbook_liquidity(all_market_ids)

        return self._parse_events(raw_events, sport_key, liquidity_map)

    async def _fetch_orderbook_liquidity(
        self, market_ids: List[str]
    ) -> Dict[str, Dict[str, dict]]:
        """
        Fetch order book depth for markets and compute per-outcome
        buy price and liquidity.

        Returns: {market_id: {outcome_id: {"buy_price": float, "liquidity": float}}}

        For a binary market:
        - Buy price for outcome A = 1 - best_bid_price_on_B
          (buying A means matching against someone bidding on B)
        - Liquidity for outcome A = qty at best bid on B
          (how many contracts you can buy at that price)
        - Dollar liquidity = qty * buy_price
        """
        if not market_ids:
            return {}

        # Batch in chunks of 25 to avoid overly long URLs
        result = {}  # type: Dict[str, Dict[str, dict]]
        for i in range(0, len(market_ids), 25):
            chunk = market_ids[i:i + 25]
            ids_param = ",".join(chunk)
            try:
                resp = await self._client.get(
                    ORDERBOOK_URL,
                    params={"marketIds": ids_param, "currency": "CASH"},
                )
                resp.raise_for_status()
                book_data = resp.json()

                for entry in book_data:
                    mid = entry.get("market", {}).get("id", "")
                    ladders = entry.get("ladders", {})
                    outcomes_info = entry.get("market", {}).get("outcomes", [])

                    # Build per-outcome best bid info
                    # {outcome_id: (best_bid_price, total_qty_at_best)}
                    bid_info = {}  # type: Dict[str, Tuple[float, float]]
                    for oid, ladder in ladders.items():
                        bids = ladder.get("bids", [])
                        if not bids:
                            continue
                        best_price = max(b["price"] for b in bids)
                        qty_at_best = sum(
                            b["qty"] for b in bids if b["price"] == best_price
                        )
                        bid_info[oid] = (best_price, qty_at_best)

                    # In a binary market:
                    # - Buy price for A = 1 - best_bid_price_on_B
                    # - Liquidity for A = qty at best bid on B
                    outcome_ids = [o["id"] for o in outcomes_info]
                    if len(outcome_ids) == 2:
                        oid_a, oid_b = outcome_ids[0], outcome_ids[1]
                        info_a = bid_info.get(oid_a, (0, 0.0))
                        info_b = bid_info.get(oid_b, (0, 0.0))

                        # A's buy price = 1 - B's best bid price
                        buy_price_a = 1.0 - info_b[0] if info_b[0] > 0 else 0.0
                        buy_price_b = 1.0 - info_a[0] if info_a[0] > 0 else 0.0

                        # A's liquidity (contracts) = B's best bid qty
                        qty_a = info_b[1]
                        qty_b = info_a[1]

                        # Dollar liquidity = contracts * buy price per contract
                        # Novig qty is in cents, so divide by 100 to get dollars
                        dollar_liq_a = qty_a * buy_price_a / 100.0 if buy_price_a > 0 else 0.0
                        dollar_liq_b = qty_b * buy_price_b / 100.0 if buy_price_b > 0 else 0.0

                        result[mid] = {
                            oid_a: {"buy_price": buy_price_a, "liquidity": round(dollar_liq_a, 2)},
                            oid_b: {"buy_price": buy_price_b, "liquidity": round(dollar_liq_b, 2)},
                        }
                        logger.debug(
                            "Novig book mid=%s: A(%s) bid=%.3f qty=%.0f → buy=%.3f liq=$%.2f | B(%s) bid=%.3f qty=%.0f → buy=%.3f liq=$%.2f",
                            mid, oid_a, info_a[0], info_a[1], buy_price_a, dollar_liq_a,
                            oid_b, info_b[0], info_b[1], buy_price_b, dollar_liq_b,
                        )

            except Exception as e:
                logger.debug("Novig orderbook fetch failed for batch: %s", e)

        return result

    def _parse_events(
        self,
        raw_events: list,
        sport_key: str,
        liquidity_map: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> List[OddsEvent]:
        """Parse GraphQL response into OddsEvent list."""
        events = []  # type: List[OddsEvent]
        sport_title = get_sport_title(sport_key)
        if liquidity_map is None:
            liquidity_map = {}

        for ev in raw_events:
            game = ev.get("game")
            if not game:
                continue

            home_team_raw = game.get("homeTeam", {})
            away_team_raw = game.get("awayTeam", {})
            home_name = home_team_raw.get("name", "")
            away_name = away_team_raw.get("name", "")
            if not home_name or not away_name:
                continue

            home_name = resolve_team_name(home_name)
            away_name = resolve_team_name(away_name)

            commence_time = ev.get("scheduled_start", "")
            event_id = canonical_event_id(sport_key, home_name, away_name, commence_time)

            # Group raw markets by base type (many alternate lines per type)
            markets_by_type = {}  # type: Dict[str, list]
            for mkt in ev.get("markets", []):
                mtype = mkt.get("type", "")
                if mtype not in _MARKET_TYPE_MAP:
                    continue
                markets_by_type.setdefault(mtype, []).append(mkt)

            # Build Market objects
            parsed_markets = []  # type: List[Market]

            for novig_type, our_key in _MARKET_TYPE_MAP.items():
                raw_markets = markets_by_type.get(novig_type, [])
                if not raw_markets:
                    continue

                if novig_type in ("MONEY", "MONEY_1H"):
                    # Moneyline: single market, just convert probs to American
                    mkt = raw_markets[0]
                    parsed = self._parse_moneyline(
                        mkt, our_key, home_name, away_name,
                        home_team_raw, away_team_raw, liquidity_map,
                    )
                    if parsed:
                        parsed_markets.append(parsed)
                elif novig_type in ("SPREAD", "SPREAD_1H"):
                    # Pick consensus spread line
                    mkt = _pick_consensus_line(raw_markets)
                    if mkt:
                        parsed = self._parse_spread(
                            mkt, our_key, home_name, away_name,
                            home_team_raw, away_team_raw, liquidity_map,
                        )
                        if parsed:
                            parsed_markets.append(parsed)
                elif novig_type in ("TOTAL", "TOTAL_1H"):
                    # Pick consensus total line
                    mkt = _pick_consensus_line(raw_markets)
                    if mkt:
                        parsed = self._parse_total(mkt, our_key, liquidity_map)
                        if parsed:
                            parsed_markets.append(parsed)
                elif novig_type in ("TEAM_TOTAL", "TEAM_TOTAL_1H"):
                    # Team totals: parse each alternate line
                    for raw_mkt in raw_markets:
                        parsed = self._parse_team_total(
                            raw_mkt, our_key, home_name, away_name,
                            home_team_raw, away_team_raw, liquidity_map,
                        )
                        if parsed:
                            parsed_markets.append(parsed)
                            break  # take the first valid one (consensus)

            if not parsed_markets:
                continue

            bookmaker = Bookmaker(
                key="novig",
                title="Novig",
                last_update=datetime.now(timezone.utc).isoformat(),
                markets=parsed_markets,
                event_url="https://novig.com",
            )

            odds_event = OddsEvent(
                id=event_id,
                sport_key=sport_key,
                sport_title=sport_title,
                commence_time=commence_time,
                home_team=home_name,
                away_team=away_name,
                bookmakers=[bookmaker],
            )
            events.append(odds_event)

        return events

    @staticmethod
    def _team_symbols(team_raw: dict) -> set:
        """Get all possible abbreviations for a team (symbol + short_name)."""
        syms = set()  # type: set
        for key in ("symbol", "short_name"):
            v = (team_raw.get(key) or "").upper().strip()
            if v:
                syms.add(v)
        return syms

    def _parse_moneyline(
        self,
        mkt: dict,
        market_key: str,
        home_name: str,
        away_name: str,
        home_team_raw: dict,
        away_team_raw: dict,
        liquidity_map: Optional[Dict[str, Dict[str, dict]]] = None,
    ) -> Optional[Market]:
        """Parse a MONEY or MONEY_1H market.

        Uses order book buy prices when available (accurate tradeable odds),
        falling back to the `available` probability field.

        During live games, one outcome may have available=None (no offers on that side).
        We handle this by:
        1. Trying altAvailable as a fallback
        2. Inferring the missing side from the complement (1 - other_prob) for binary markets
        """
        outcomes = mkt.get("outcomes", [])
        if len(outcomes) < 2:
            return None

        home_syms = self._team_symbols(home_team_raw)
        away_syms = self._team_symbols(away_team_raw)

        home_prob = None  # type: Optional[float]
        away_prob = None  # type: Optional[float]
        home_outcome_id = None  # type: Optional[str]
        away_outcome_id = None  # type: Optional[str]
        home_inferred = False
        away_inferred = False

        for o in outcomes:
            desc = (o.get("description") or "").upper().strip()
            # Try available first, fall back to altAvailable
            prob = o.get("available")
            if prob is None:
                prob = o.get("altAvailable")
            if desc in home_syms:
                home_prob = prob
                home_outcome_id = o.get("id")
            elif desc in away_syms:
                away_prob = prob
                away_outcome_id = o.get("id")

        # For binary markets: infer missing side from complement
        if home_prob is not None and away_prob is None:
            away_prob = 1.0 - home_prob
            away_inferred = True
            logger.debug("Novig: inferred away prob %.3f from home %.3f", away_prob, home_prob)
        elif away_prob is not None and home_prob is None:
            home_prob = 1.0 - away_prob
            home_inferred = True
            logger.debug("Novig: inferred home prob %.3f from away %.3f", home_prob, away_prob)

        if home_prob is None and away_prob is None:
            return None

        # Look up per-outcome order book data (buy prices + liquidity)
        market_id = mkt.get("id", "")
        mkt_book = (liquidity_map or {}).get(market_id, {})

        # Override prices with order book buy prices when available.
        # The `available` field is the API's probability estimate, but the
        # actual tradeable price comes from the order book:
        # buy_price_A = 1 - best_bid_on_B
        home_book = mkt_book.get(home_outcome_id, {}) if home_outcome_id else {}
        away_book = mkt_book.get(away_outcome_id, {}) if away_outcome_id else {}

        # Use order book buy_price if available, otherwise fall back to `available` prob
        home_buy_price = home_book.get("buy_price") if isinstance(home_book, dict) else None
        away_buy_price = away_book.get("buy_price") if isinstance(away_book, dict) else None

        if home_buy_price and 0 < home_buy_price < 1:
            home_odds = _prob_to_american(home_buy_price)
        elif home_prob is not None and 0 < home_prob < 1:
            home_odds = _prob_to_american(home_prob)
        else:
            home_odds = None

        if away_buy_price and 0 < away_buy_price < 1:
            away_odds = _prob_to_american(away_buy_price)
        elif away_prob is not None and 0 < away_prob < 1:
            away_odds = _prob_to_american(away_prob)
        else:
            away_odds = None

        parsed_outcomes = []  # type: List[Outcome]
        if home_odds is not None:
            home_liq = None  # type: Optional[float]
            if not home_inferred and isinstance(home_book, dict):
                home_liq = home_book.get("liquidity")
            parsed_outcomes.append(Outcome(
                name=home_name,
                price=home_odds,
                liquidity=round(home_liq, 2) if home_liq else None,
            ))
        if away_odds is not None:
            away_liq = None  # type: Optional[float]
            if not away_inferred and isinstance(away_book, dict):
                away_liq = away_book.get("liquidity")
            parsed_outcomes.append(Outcome(
                name=away_name,
                price=away_odds,
                liquidity=round(away_liq, 2) if away_liq else None,
            ))

        if len(parsed_outcomes) < 2:
            return None

        # Also set total market liquidity (sum of both sides)
        total_liq = None  # type: Optional[float]
        liq_vals = [o.liquidity for o in parsed_outcomes if o.liquidity]
        if liq_vals:
            total_liq = round(sum(liq_vals), 2)

        return Market(key=market_key, outcomes=parsed_outcomes, liquidity=total_liq)

    def _parse_spread(
        self,
        mkt: dict,
        market_key: str,
        home_name: str,
        away_name: str,
        home_team_raw: dict,
        away_team_raw: dict,
        liquidity_map: Optional[Dict[str, Dict[str, dict]]] = None,
    ) -> Optional[Market]:
        """Parse a SPREAD or SPREAD_1H market.

        Uses order book buy prices when available (actual tradeable price),
        falling back to the `available` probability field.
        """
        outcomes = mkt.get("outcomes", [])
        strike = mkt.get("strike", 0)
        if len(outcomes) < 2:
            return None

        home_syms = self._team_symbols(home_team_raw)
        away_syms = self._team_symbols(away_team_raw)

        # First pass: collect probs, points, and outcome IDs for each side
        home_prob = None  # type: Optional[float]
        away_prob = None  # type: Optional[float]
        home_point = None  # type: Optional[float]
        away_point = None  # type: Optional[float]
        home_outcome_id = None  # type: Optional[str]
        away_outcome_id = None  # type: Optional[str]

        for o in outcomes:
            desc = (o.get("description") or "").upper()
            prob = o.get("available")
            if prob is None:
                prob = o.get("altAvailable")

            parts = desc.split()
            team_sym = parts[0].strip() if parts else ""
            point = None  # type: Optional[float]
            if len(parts) >= 2:
                try:
                    point = float(parts[1])
                except (ValueError, IndexError):
                    pass

            if team_sym in home_syms:
                home_prob = prob
                home_point = point
                home_outcome_id = o.get("id")
            elif team_sym in away_syms:
                away_prob = prob
                away_point = point
                away_outcome_id = o.get("id")

        # Infer missing side from complement
        if home_prob is not None and away_prob is None:
            away_prob = 1.0 - home_prob
        elif away_prob is not None and home_prob is None:
            home_prob = 1.0 - away_prob

        # Look up order book buy prices (override `available` probability)
        market_id = mkt.get("id", "")
        mkt_book = (liquidity_map or {}).get(market_id, {})
        home_book = mkt_book.get(home_outcome_id, {}) if home_outcome_id else {}
        away_book = mkt_book.get(away_outcome_id, {}) if away_outcome_id else {}

        home_buy_price = home_book.get("buy_price") if isinstance(home_book, dict) else None
        away_buy_price = away_book.get("buy_price") if isinstance(away_book, dict) else None

        # Order book buy price ALWAYS overrides the probability estimate
        if home_buy_price and 0 < home_buy_price < 1:
            home_odds = _prob_to_american(home_buy_price)
        elif home_prob is not None and 0 < home_prob < 1:
            home_odds = _prob_to_american(home_prob)
        else:
            home_odds = None

        if away_buy_price and 0 < away_buy_price < 1:
            away_odds = _prob_to_american(away_buy_price)
        elif away_prob is not None and 0 < away_prob < 1:
            away_odds = _prob_to_american(away_prob)
        else:
            away_odds = None

        parsed_outcomes = []  # type: List[Outcome]
        if home_odds is not None:
            home_liq = None  # type: Optional[float]
            if isinstance(home_book, dict):
                home_liq = home_book.get("liquidity")
            parsed_outcomes.append(Outcome(
                name=home_name,
                price=home_odds,
                point=home_point,
                liquidity=round(home_liq, 2) if home_liq else None,
            ))
        if away_odds is not None:
            away_liq = None  # type: Optional[float]
            if isinstance(away_book, dict):
                away_liq = away_book.get("liquidity")
            parsed_outcomes.append(Outcome(
                name=away_name,
                price=away_odds,
                point=away_point,
                liquidity=round(away_liq, 2) if away_liq else None,
            ))

        if len(parsed_outcomes) < 2:
            return None

        total_liq = None  # type: Optional[float]
        liq_vals = [o.liquidity for o in parsed_outcomes if o.liquidity]
        if liq_vals:
            total_liq = round(sum(liq_vals), 2)

        return Market(key=market_key, outcomes=parsed_outcomes, liquidity=total_liq)

    def _parse_total(
        self,
        mkt: dict,
        market_key: str,
        liquidity_map: Optional[Dict[str, Dict[str, dict]]] = None,
    ) -> Optional[Market]:
        """Parse a TOTAL or TOTAL_1H market.

        Uses order book buy prices when available (actual tradeable price),
        falling back to the `available` probability field.
        """
        outcomes = mkt.get("outcomes", [])
        strike = mkt.get("strike", 0)
        if len(outcomes) < 2:
            return None

        # First pass: collect probs, points, and outcome IDs for each side
        over_prob = None  # type: Optional[float]
        under_prob = None  # type: Optional[float]
        over_point = None  # type: Optional[float]
        under_point = None  # type: Optional[float]
        over_outcome_id = None  # type: Optional[str]
        under_outcome_id = None  # type: Optional[str]

        for o in outcomes:
            desc = (o.get("description") or "")
            prob = o.get("available")
            if prob is None:
                prob = o.get("altAvailable")

            parts = desc.split()
            side = parts[0] if parts else ""
            point = None  # type: Optional[float]
            if len(parts) >= 2:
                try:
                    point = float(parts[1])
                except (ValueError, IndexError):
                    pass

            if point is None:
                point = float(strike) if strike else None

            if side.lower().startswith("over"):
                over_prob = prob
                over_point = point
                over_outcome_id = o.get("id")
            elif side.lower().startswith("under"):
                under_prob = prob
                under_point = point
                under_outcome_id = o.get("id")

        # Infer missing side from complement
        if over_prob is not None and under_prob is None:
            under_prob = 1.0 - over_prob
            under_point = over_point  # same line
        elif under_prob is not None and over_prob is None:
            over_prob = 1.0 - under_prob
            over_point = under_point

        # Look up order book buy prices (override `available` probability)
        market_id = mkt.get("id", "")
        mkt_book = (liquidity_map or {}).get(market_id, {})
        over_book = mkt_book.get(over_outcome_id, {}) if over_outcome_id else {}
        under_book = mkt_book.get(under_outcome_id, {}) if under_outcome_id else {}

        over_buy_price = over_book.get("buy_price") if isinstance(over_book, dict) else None
        under_buy_price = under_book.get("buy_price") if isinstance(under_book, dict) else None

        # Order book buy price ALWAYS overrides the probability estimate
        if over_buy_price and 0 < over_buy_price < 1:
            over_odds = _prob_to_american(over_buy_price)
        elif over_prob is not None and 0 < over_prob < 1:
            over_odds = _prob_to_american(over_prob)
        else:
            over_odds = None

        if under_buy_price and 0 < under_buy_price < 1:
            under_odds = _prob_to_american(under_buy_price)
        elif under_prob is not None and 0 < under_prob < 1:
            under_odds = _prob_to_american(under_prob)
        else:
            under_odds = None

        parsed_outcomes = []  # type: List[Outcome]
        if over_odds is not None:
            over_liq = None  # type: Optional[float]
            if isinstance(over_book, dict):
                over_liq = over_book.get("liquidity")
            parsed_outcomes.append(Outcome(
                name="Over",
                price=over_odds,
                point=over_point,
                liquidity=round(over_liq, 2) if over_liq else None,
            ))
        if under_odds is not None:
            under_liq = None  # type: Optional[float]
            if isinstance(under_book, dict):
                under_liq = under_book.get("liquidity")
            parsed_outcomes.append(Outcome(
                name="Under",
                price=under_odds,
                point=under_point,
                liquidity=round(under_liq, 2) if under_liq else None,
            ))

        if len(parsed_outcomes) < 2:
            return None

        total_liq = None  # type: Optional[float]
        liq_vals = [o.liquidity for o in parsed_outcomes if o.liquidity]
        if liq_vals:
            total_liq = round(sum(liq_vals), 2)

        return Market(key=market_key, outcomes=parsed_outcomes, liquidity=total_liq)

    def _parse_team_total(
        self,
        mkt: dict,
        market_key: str,
        home_name: str,
        away_name: str,
        home_team_raw: dict,
        away_team_raw: dict,
        liquidity_map: Optional[Dict[str, Dict[str, dict]]] = None,
    ) -> Optional[Market]:
        """Parse a TEAM_TOTAL or TEAM_TOTAL_1H market.

        Determines home/away from outcome descriptions, then delegates to
        _parse_total logic for Over/Under pricing.
        """
        outcomes = mkt.get("outcomes", [])
        if len(outcomes) < 2:
            return None

        home_syms = self._team_symbols(home_team_raw)
        away_syms = self._team_symbols(away_team_raw)

        # Determine which team from outcome descriptions
        # Novig team_total descriptions look like "HOU Over 112.5" or "BOS Under 112.5"
        side = ""
        for o in outcomes:
            desc = (o.get("description") or "").upper()
            parts = desc.split()
            if parts:
                team_sym = parts[0].strip()
                if team_sym in home_syms:
                    side = "home"
                    break
                elif team_sym in away_syms:
                    side = "away"
                    break

        if not side:
            return None

        # Parse as a regular total (Over/Under)
        # Adjust the key based on home/away and period suffix from market_key
        parsed = self._parse_total(mkt, market_key, liquidity_map)
        if parsed is None:
            return None

        # Set the correct team_total key
        if side == "home":
            if "_h1" in market_key:
                parsed.key = "team_total_home_h1"
            else:
                parsed.key = "team_total_home"
        elif side == "away":
            if "_h1" in market_key:
                parsed.key = "team_total_away_h1"
            else:
                parsed.key = "team_total_away"

        return parsed

    async def close(self) -> None:
        await self._client.aclose()
