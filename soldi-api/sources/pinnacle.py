"""
Pinnacle sportsbook scraper.
Uses Pinnacle's public guest Arcadia API to fetch odds.
No authentication required for matchups and straight markets.
"""

import logging
from typing import Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from market_keys import get_pinnacle_period_suffix
from sources.sport_mapping import (
    PINNACLE_LEAGUE_IDS,
    PINNACLE_SPORT_IDS,
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

# Keyword filters for dynamic tennis league discovery
_TENNIS_ATP_KEYWORDS = ["atp", "challenger"]
_TENNIS_WTA_KEYWORDS = ["wta"]

logger = logging.getLogger(__name__)

BASE_URL = "https://guest.api.arcadia.pinnacle.com/0.1"


class PinnacleSource(DataSource):
    """Fetches odds from Pinnacle's public guest API."""

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
                "X-API-Key": "CmX2KcMrXuFmNg6YFbmTxE0y9CIrOi0R",
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
        if bookmakers and "pinnacle" not in bookmakers:
            return [], {"x-requests-remaining": "unlimited"}

        league_id = PINNACLE_LEAGUE_IDS.get(sport_key)
        sport_id = PINNACLE_SPORT_IDS.get(sport_key)

        if league_id is None and sport_id is None:
            return [], {"x-requests-remaining": "unlimited"}

        try:
            if league_id is not None:
                # Single league — standard fetch
                matchups = await self._fetch_matchups(league_id)
                markets_data = await self._fetch_markets(league_id)
                # Fetch special markets for MMA (total rounds, fight distance)
                # and other sports (may have additional markets)
                if "mma" in sport_key or "boxing" in sport_key:
                    special = await self._fetch_special_markets(league_id)
                    if special:
                        logger.info("Pinnacle: %d special markets for %s (league %d)",
                                    len(special), sport_key, league_id)
                        for s in special[:5]:
                            logger.info("  Special: matchupId=%s name=%r keys=%s",
                                        s.get("matchupId"), s.get("name", "?"),
                                        [k for k in s.keys() if k not in ("contestantLines", "participants")])
                        markets_data = markets_data + special
                    else:
                        logger.info("Pinnacle: 0 special markets for %s (league %d)",
                                    sport_key, league_id)
                events = self._parse(matchups, markets_data, sport_key)
            else:
                # Dynamic multi-league sport (e.g. tennis)
                events = await self._fetch_multi_league(sport_id, sport_key)

            logger.info(f"Pinnacle: {len(events)} events for {sport_key}")
            return events, {"x-requests-remaining": "unlimited"}

        except Exception as e:
            logger.warning(f"Pinnacle failed for {sport_key}: {e}")
            return [], {"x-requests-remaining": "unlimited"}

    async def _fetch_matchups(self, league_id: int) -> list:
        url = f"{BASE_URL}/leagues/{league_id}/matchups"
        response = await self._client.get(url)
        response.raise_for_status()
        return response.json()

    async def _fetch_markets(self, league_id: int) -> list:
        url = f"{BASE_URL}/leagues/{league_id}/markets/straight"
        response = await self._client.get(url)
        response.raise_for_status()
        return response.json()

    async def _fetch_special_markets(self, league_id: int) -> list:
        """Fetch special/prop markets (total rounds, fight distance, etc.)."""
        url = f"{BASE_URL}/leagues/{league_id}/markets/special"
        try:
            response = await self._client.get(url)
            response.raise_for_status()
            return response.json()
        except Exception:
            return []

    async def _fetch_multi_league(
        self, sport_id: int, sport_key: str,
    ) -> List[OddsEvent]:
        """Fetch all active leagues for a sport and aggregate events.

        Used for sports like tennis where each tournament is a separate league.
        Filters leagues by keyword (ATP vs WTA) based on sport_key.
        """
        import asyncio

        # Discover active leagues
        url = f"{BASE_URL}/sports/{sport_id}/leagues?all=false"
        resp = await self._client.get(url)
        resp.raise_for_status()
        leagues = resp.json()

        # Filter leagues by keyword for ATP vs WTA
        if sport_key == "tennis_atp":
            keywords = _TENNIS_ATP_KEYWORDS
        elif sport_key == "tennis_wta":
            keywords = _TENNIS_WTA_KEYWORDS
        else:
            keywords = []

        active_leagues = []
        for lg in leagues:
            if lg.get("matchupCount", 0) == 0:
                continue
            name_lower = lg.get("name", "").lower()
            if keywords:
                if any(kw in name_lower for kw in keywords):
                    active_leagues.append(lg["id"])
            else:
                active_leagues.append(lg["id"])

        if not active_leagues:
            return []

        # Fetch matchups and markets for all leagues concurrently
        sem = asyncio.Semaphore(10)

        async def _fetch_league(lid: int):
            async with sem:
                try:
                    matchups = await self._fetch_matchups(lid)
                    mkts = await self._fetch_markets(lid)
                    return self._parse(matchups, mkts, sport_key)
                except Exception:
                    return []

        results = await asyncio.gather(
            *[_fetch_league(lid) for lid in active_leagues],
            return_exceptions=True,
        )

        all_events = []
        for r in results:
            if isinstance(r, list):
                all_events.extend(r)

        logger.info(
            f"Pinnacle: fetched {len(active_leagues)} leagues, "
            f"{len(all_events)} events for {sport_key}"
        )
        return all_events

    def _parse(self, matchups: list, markets_data: list, sport_key: str) -> List[OddsEvent]:
        is_soccer = "soccer" in sport_key

        # Build matchup info: id -> {home, away, startTime, participants_by_alignment}
        matchup_map = {}  # type: Dict[int, dict]
        for m in matchups:
            mid = m.get("id")
            if not mid:
                continue
            # Skip non-game matchups (e.g. specials, futures)
            if m.get("type") != "matchup":
                continue

            participants = m.get("participants", [])
            if len(participants) < 2:
                continue

            home = ""
            away = ""
            for p in participants:
                alignment = p.get("alignment", "")
                name = p.get("name", "")
                if alignment == "home":
                    home = name
                elif alignment == "away":
                    away = name

            # Some sports use neutral alignment — assign by order
            if not home and not away and len(participants) >= 2:
                home = participants[0].get("name", "")
                away = participants[1].get("name", "")

            if not home or not away:
                continue

            matchup_map[mid] = {
                "home": resolve_team_name(home),
                "away": resolve_team_name(away),
                "startTime": m.get("startTime", ""),
            }

        # Group markets by matchupId
        # Markets have: matchupId, type ("moneyline"/"spread"/"total"/"team_total"), period, prices[]
        # Special markets have: matchupId, name, contestantLines (from /markets/special)
        markets_by_matchup = {}  # type: Dict[int, List[dict]]
        for mkt in markets_data:
            mid = mkt.get("matchupId")
            if mid is None:
                continue
            # Handle special markets (different format from /markets/special)
            if "contestantLines" in mkt or ("name" in mkt and "type" not in mkt):
                # Special market — attach directly without period filtering
                mkt["_period_suffix"] = ""
                mkt["_is_special"] = True
                if mid not in markets_by_matchup:
                    markets_by_matchup[mid] = []
                markets_by_matchup[mid].append(mkt)
                continue
            # Skip alternate lines
            if mkt.get("isAlternate", False):
                continue
            # Include period markets (0=full game, 1=1st half/period, etc.)
            period = mkt.get("period", 0)
            suffix = get_pinnacle_period_suffix(sport_key, period)
            if suffix is None:
                # Unknown period for this sport, skip
                continue
            # Attach the computed suffix to the market data for later use
            mkt["_period_suffix"] = suffix
            if mid not in markets_by_matchup:
                markets_by_matchup[mid] = []
            markets_by_matchup[mid].append(mkt)

        # Build events
        sport_title = get_sport_title(sport_key)
        events = []

        for mid, info in matchup_map.items():
            mkt_list = markets_by_matchup.get(mid, [])
            if not mkt_list:
                continue

            pin_markets = []
            seen_keys = set()  # type: set
            for mkt in mkt_list:
                mkt_type = mkt.get("type", "")
                prices = mkt.get("prices", [])
                suffix = mkt.get("_period_suffix", "")

                if mkt_type == "moneyline":
                    # Soccer moneylines are 3-way (home/draw/away)
                    has_draw = any(p.get("designation") == "draw" for p in prices)
                    if is_soccer and has_draw:
                        market_key = "h2h_3way" + suffix
                        if market_key in seen_keys:
                            continue
                        parsed = self._parse_moneyline_3way(prices, info["home"], info["away"])
                        if parsed:
                            pin_markets.append(Market(key=market_key, outcomes=parsed))
                            seen_keys.add(market_key)
                    else:
                        market_key = "h2h" + suffix
                        if market_key in seen_keys:
                            continue
                        parsed = self._parse_moneyline(prices, info["home"], info["away"])
                        if parsed:
                            pin_markets.append(Market(key=market_key, outcomes=parsed))
                            seen_keys.add(market_key)

                elif mkt_type == "spread":
                    market_key = "spreads" + suffix
                    if market_key in seen_keys:
                        continue
                    parsed = self._parse_spread(prices, info["home"], info["away"])
                    if parsed:
                        pin_markets.append(Market(key=market_key, outcomes=parsed))
                        seen_keys.add(market_key)

                elif mkt_type == "total":
                    market_key = "totals" + suffix
                    if market_key in seen_keys:
                        continue
                    parsed = self._parse_total(prices)
                    if parsed:
                        pin_markets.append(Market(key=market_key, outcomes=parsed))
                        seen_keys.add(market_key)

                elif mkt_type == "team_total":
                    # team_total has a "side" field: "home" or "away"
                    side = mkt.get("side", "")
                    if side == "home":
                        market_key = "team_total_home" + suffix
                    elif side == "away":
                        market_key = "team_total_away" + suffix
                    else:
                        continue
                    if market_key in seen_keys:
                        continue
                    parsed = self._parse_total(prices)
                    if parsed:
                        pin_markets.append(Market(key=market_key, outcomes=parsed))
                        seen_keys.add(market_key)

                # Handle special markets from /markets/special endpoint
                if mkt.get("_is_special"):
                    special_name = (mkt.get("name") or "").lower()
                    contestant_lines = mkt.get("contestantLines", [])
                    if not contestant_lines:
                        continue
                    if "total rounds" in special_name or "round" in special_name:
                        parsed = self._parse_special_total(contestant_lines)
                        if parsed and "total_rounds" not in seen_keys:
                            pin_markets.append(Market(key="total_rounds", outcomes=parsed))
                            seen_keys.add("total_rounds")
                    elif "distance" in special_name or "go the distance" in special_name:
                        parsed = self._parse_special_yes_no(contestant_lines)
                        if parsed and "fight_to_go_distance" not in seen_keys:
                            pin_markets.append(Market(key="fight_to_go_distance", outcomes=parsed))
                            seen_keys.add("fight_to_go_distance")

            if not pin_markets:
                continue

            # Build event deep-link URL from Pinnacle matchup ID
            event_url = f"https://www.pinnacle.com/en/sports/matchup/{mid}"

            cid = canonical_event_id(sport_key, info["home"], info["away"], info["startTime"])
            events.append(OddsEvent(
                id=cid,
                sport_key=sport_key,
                sport_title=sport_title,
                commence_time=info["startTime"],
                home_team=info["home"],
                away_team=info["away"],
                bookmakers=[
                    Bookmaker(key="pinnacle", title="Pinnacle", markets=pin_markets, event_url=event_url)
                ],
            ))

        return events

    def _parse_moneyline(self, prices: list, home: str, away: str) -> List[Outcome]:
        result = []
        for p in prices:
            designation = p.get("designation", "")
            price = p.get("price")
            if price is None:
                continue
            if designation == "draw":
                continue  # Skip draw for 2-way moneyline
            # Pinnacle prices are already in American format
            name = home if designation == "home" else away if designation == "away" else designation
            result.append(Outcome(name=name, price=int(price)))
        return result if len(result) >= 2 else []

    def _parse_moneyline_3way(self, prices: list, home: str, away: str) -> List[Outcome]:
        """Parse a 3-way moneyline (home/draw/away) for soccer."""
        result = []
        for p in prices:
            designation = p.get("designation", "")
            price = p.get("price")
            if price is None:
                continue
            if designation == "home":
                name = home
            elif designation == "away":
                name = away
            elif designation == "draw":
                name = "Draw"
            else:
                name = designation
            result.append(Outcome(name=name, price=int(price)))
        return result if len(result) >= 3 else []

    def _parse_spread(self, prices: list, home: str, away: str) -> List[Outcome]:
        result = []
        for p in prices:
            designation = p.get("designation", "")
            price = p.get("price")
            points = p.get("points")
            if price is None or points is None:
                continue
            name = home if designation == "home" else away if designation == "away" else designation
            result.append(Outcome(name=name, price=int(price), point=float(points)))
        return result if len(result) >= 2 else []

    def _parse_total(self, prices: list) -> List[Outcome]:
        result = []
        for p in prices:
            designation = p.get("designation", "")
            price = p.get("price")
            points = p.get("points")
            if price is None or points is None:
                continue
            name = "Over" if designation == "over" else "Under" if designation == "under" else designation
            result.append(Outcome(name=name, price=int(price), point=float(points)))
        return result if len(result) >= 2 else []

    def _parse_special_total(self, contestant_lines: list) -> List[Outcome]:
        """Parse special market O/U (e.g., total rounds) from contestantLines."""
        result = []
        for cl in contestant_lines:
            name = (cl.get("name") or "").strip()
            price = cl.get("price")
            handicap = cl.get("handicap")
            if price is None:
                continue
            if "over" in name.lower():
                result.append(Outcome(name="Over", price=int(price), point=float(handicap) if handicap else None))
            elif "under" in name.lower():
                result.append(Outcome(name="Under", price=int(price), point=float(handicap) if handicap else None))
        return result if len(result) >= 2 else []

    def _parse_special_yes_no(self, contestant_lines: list) -> List[Outcome]:
        """Parse special market Yes/No (e.g., fight to go distance) from contestantLines."""
        result = []
        for cl in contestant_lines:
            name = (cl.get("name") or "").strip()
            price = cl.get("price")
            if price is None:
                continue
            if "yes" in name.lower():
                result.append(Outcome(name="Yes", price=int(price)))
            elif "no" in name.lower():
                result.append(Outcome(name="No", price=int(price)))
        return result if len(result) >= 2 else []

    async def close(self) -> None:
        await self._client.aclose()
