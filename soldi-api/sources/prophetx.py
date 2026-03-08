"""
ProphetX sports exchange scraper.

Uses ProphetX's PUBLIC API endpoints (no authentication required).
Discovered by intercepting the prophetx.co website's network requests.

API endpoints:
  - Tournaments + Events: GET /trade/public/api/v1/tournaments?expand=events&type=highlight&limit=150
  - Markets/Odds:         GET /partner/v2/public/get_multiple_markets?market_types=moneyline,spread,total&event_ids=...

ProphetX is a peer-to-peer sports prediction exchange where selections
include an order book with multiple price levels. We take the best
available price (first level) and sum all stakes for liquidity.

Odds are returned in American format directly (e.g., -158, +156).
"""

import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from market_keys import detect_period_suffix, classify_market_type
from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://cash.api.prophetx.co"
TOURNAMENTS_URL = BASE_URL + "/trade/public/api/v1/tournaments"
MARKETS_URL = BASE_URL + "/partner/v2/public/get_multiple_markets"

# Fallback: staging URL (less data — 9 tournaments vs 18 on production)
_STAGING_URL = "https://api-ss-staging.betprophet.co"

# Cache TTL — longer to avoid spamming if API is blocked
_CACHE_TTL = 60  # seconds

# ProphetX tournament_id -> OddsScreen sport_key
TOURNAMENT_TO_SPORT = {
    # Basketball
    132: "basketball_nba",
    648: "basketball_ncaab",
    # Football
    31: "americanfootball_nfl",
    233: "americanfootball_nfl",     # NFL Preseason
    # Ice Hockey
    234: "icehockey_nhl",
    # Baseball
    109: "baseball_mlb",
    # MMA
    1500000003: "mma_mixed_martial_arts",
    # Soccer
    7: "soccer_uefa_champs_league",
    679: "soccer_uefa_europa_league",
    44: "soccer_england_championship",
    46: "soccer_argentina_primera_division",
    37: "soccer_brazil_serie_a",
}  # type: Dict[int, str]

# Reverse: sport_key -> list of tournament_ids
SPORT_TO_TOURNAMENTS = {}  # type: Dict[str, List[int]]
for _tid, _skey in TOURNAMENT_TO_SPORT.items():
    SPORT_TO_TOURNAMENTS.setdefault(_skey, []).append(_tid)


class ProphetXSource(DataSource):
    """Fetches odds from ProphetX's public API (no auth required)."""

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/131.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.prophetx.co/",
                "Origin": "https://www.prophetx.co",
            },
        )
        self._cache = {}  # type: Dict[str, Tuple[List[OddsEvent], float]]
        # Cache the full tournament->events data so we don't re-fetch per sport
        self._tournaments_cache = None  # type: Optional[Tuple[Dict[int, List[dict]], float]]

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        if bookmakers and "prophetx" not in bookmakers:
            return [], {"x-requests-remaining": "unlimited"}

        # Check sport-level cache
        cached = self._cache.get(sport_key)
        if cached and (time.time() - cached[1]) < _CACHE_TTL:
            return cached[0], {"x-requests-remaining": "unlimited"}

        try:
            events = await self._fetch_odds_for_sport(sport_key)
            self._cache[sport_key] = (events, time.time())
            if events:
                logger.info("ProphetX: %d events for %s", len(events), sport_key)
            return events, {"x-requests-remaining": "unlimited"}
        except Exception as e:
            logger.warning("ProphetX failed for %s: %s", sport_key, e)
            # Return stale cache if available
            if cached:
                return cached[0], {"x-requests-remaining": "unlimited"}
            return [], {"x-requests-remaining": "unlimited"}

    async def _fetch_odds_for_sport(self, sport_key: str) -> List[OddsEvent]:
        """Fetch tournaments, filter events by sport, get markets, parse."""
        # Step 1: Get all tournaments + events (cached for 90s)
        tournaments_by_id = await self._get_tournaments()

        # Step 2: Collect events for this sport
        tournament_ids = SPORT_TO_TOURNAMENTS.get(sport_key)
        if not tournament_ids:
            # Try dynamic matching from tournament data
            tournament_ids = self._discover_tournaments(tournaments_by_id, sport_key)
            if not tournament_ids:
                return []

        sport_events = []  # type: List[dict]
        for tid in tournament_ids:
            evts = tournaments_by_id.get(tid, [])
            sport_events.extend(evts)

        if not sport_events:
            return []

        # Step 3: Filter to active events and collect IDs
        active_events = []  # type: List[dict]
        for ev in sport_events:
            status = ev.get("status", "")
            if status in ("closed", "completed", "settled", "cancelled"):
                continue
            if ev.get("id"):
                active_events.append(ev)

        if not active_events:
            return []

        event_ids = [ev["id"] for ev in active_events]
        event_map = {ev["id"]: ev for ev in active_events}

        # Step 4: Fetch markets in batches of 50
        all_markets = await self._fetch_markets_batch(event_ids)

        # Step 5: Parse into OddsEvent objects
        sport_title = get_sport_title(sport_key)
        results = []  # type: List[OddsEvent]
        for eid in event_ids:
            ev = event_map.get(eid)
            if not ev:
                continue
            mkts = all_markets.get(eid, [])
            parsed = self._parse_event(ev, mkts, sport_key, sport_title)
            if parsed:
                results.append(parsed)

        return results

    async def _get_tournaments(self) -> Dict[int, List[dict]]:
        """
        Fetch all tournaments with events. Cached for _CACHE_TTL seconds.
        Returns: {tournament_id: [sport_events]}
        """
        if self._tournaments_cache:
            data, ts = self._tournaments_cache
            if (time.time() - ts) < _CACHE_TTL:
                return data

        resp = await self._client.get(
            TOURNAMENTS_URL,
            params={"expand": "events", "type": "highlight", "limit": "150"},
        )
        resp.raise_for_status()
        raw = resp.json()

        tournaments = raw.get("data", {}).get("tournaments", [])
        result = {}  # type: Dict[int, List[dict]]
        for t in tournaments:
            tid = t.get("id")
            if tid is None:
                continue
            # Events are in "sportEvents" key
            events = t.get("sportEvents", [])
            if events:
                result[tid] = events

        self._tournaments_cache = (result, time.time())
        logger.debug("ProphetX: fetched %d tournaments", len(result))
        return result

    def _discover_tournaments(
        self, tournaments_by_id: Dict[int, List[dict]], sport_key: str
    ) -> List[int]:
        """
        Try to match unknown tournaments to sport_key by inspecting
        the sport.name field on their events.
        """
        # Map sport names from ProphetX to our sport_key prefixes
        sport_name_map = {
            "Basketball": "basketball_",
            "Ice Hockey": "icehockey_",
            "Baseball": "baseball_",
            "American Football": "americanfootball_",
            "MMA": "mma_",
            "Soccer": "soccer_",
            "Tennis": "tennis_",
        }  # type: Dict[str, str]

        matched = []  # type: List[int]
        for tid, events in tournaments_by_id.items():
            if tid in TOURNAMENT_TO_SPORT:
                continue  # Already mapped
            if not events:
                continue
            first = events[0]
            px_sport = first.get("sport", {}).get("name", "")
            prefix = sport_name_map.get(px_sport, "")
            if prefix and sport_key.startswith(prefix):
                matched.append(tid)
                # Cache the mapping for future lookups
                TOURNAMENT_TO_SPORT[tid] = sport_key
                SPORT_TO_TOURNAMENTS.setdefault(sport_key, []).append(tid)
                logger.info(
                    "ProphetX: auto-mapped tournament %d to %s", tid, sport_key
                )

        return matched

    async def _fetch_markets_batch(
        self, event_ids: List[int]
    ) -> Dict[int, List[dict]]:
        """
        Fetch markets for events in batches of 50.
        Returns: {event_id: [markets]}
        """
        all_markets = {}  # type: Dict[int, List[dict]]

        for i in range(0, len(event_ids), 50):
            batch = event_ids[i:i + 50]
            batch_str = ",".join(str(eid) for eid in batch)
            try:
                resp = await self._client.get(
                    MARKETS_URL,
                    params={
                        "market_types": ",".join([
                            "moneyline", "spread", "total",
                            "team_total", "home_total", "away_total",
                            "total_rounds", "fight_to_go_distance", "fight_distance",
                            "method_of_victory", "round_betting",
                            "moneyline_h1", "spread_h1", "total_h1",
                            "team_total_h1", "home_total_h1", "away_total_h1",
                            "moneyline_h2", "spread_h2", "total_h2",
                            "team_total_h2", "home_total_h2", "away_total_h2",
                            "moneyline_q1", "spread_q1", "total_q1",
                            "team_total_q1", "home_total_q1", "away_total_q1",
                            "moneyline_q2", "spread_q2", "total_q2",
                            "team_total_q2", "home_total_q2", "away_total_q2",
                            "moneyline_q3", "spread_q3", "total_q3",
                            "team_total_q3", "home_total_q3", "away_total_q3",
                            "moneyline_q4", "spread_q4", "total_q4",
                            "team_total_q4", "home_total_q4", "away_total_q4",
                            "first_half_moneyline", "first_half_spread", "first_half_total",
                            "1h_moneyline", "1h_spread", "1h_total",
                        ]),
                        "event_ids": batch_str,
                    },
                )
                resp.raise_for_status()
                raw = resp.json()

                # Response: {data: [{eventId, markets, totalStake, marketCount}]}
                items = raw.get("data", [])
                if isinstance(items, list):
                    for item in items:
                        eid = item.get("eventId")
                        mkts = item.get("markets", [])
                        if eid and mkts:
                            all_markets[eid] = mkts
            except Exception as e:
                logger.warning("ProphetX: markets batch failed: %s", e)

        return all_markets

    def _parse_event(
        self, event: dict, markets_data: List[dict],
        sport_key: str, sport_title: str,
    ) -> Optional[OddsEvent]:
        """Parse a ProphetX event + markets into an OddsEvent."""
        competitors = event.get("competitors", [])
        if len(competitors) < 2:
            return None

        # Determine home/away from seq field and event name format
        # ProphetX uses "Away at Home" naming and seq=0 for home, seq=1 for away
        # But we also check the event name pattern: "X at Y" → X=away, Y=home
        home_team = ""
        away_team = ""

        # Sort by seq (0=home, 1=away based on position)
        sorted_comps = sorted(competitors[:2], key=lambda c: c.get("seq", 0))
        comp_0 = sorted_comps[0]  # seq=0 (first listed, home)
        comp_1 = sorted_comps[1]  # seq=1 (second listed, away)

        # Event name is "Away at Home" format
        event_name = event.get("name", "")
        if " at " in event_name:
            parts = event_name.split(" at ", 1)
            away_team = resolve_team_name(parts[0].strip())
            home_team = resolve_team_name(parts[1].strip())
        else:
            # Fallback: seq=0 as home, seq=1 as away
            home_team = resolve_team_name(
                comp_0.get("displayName") or comp_0.get("name", "")
            )
            away_team = resolve_team_name(
                comp_1.get("displayName") or comp_1.get("name", "")
            )

        if not home_team or not away_team:
            return None

        commence_time = event.get("scheduled", "")

        # Build competitor ID -> name mapping for market parsing
        comp_id_to_name = {}  # type: Dict[int, str]
        for c in competitors:
            cid = c.get("id")
            cname = resolve_team_name(
                c.get("displayName") or c.get("name", "")
            )
            if cid and cname:
                comp_id_to_name[cid] = cname

        # Parse markets
        parsed_markets = []  # type: List[Market]
        for mkt in markets_data:
            parsed = self._parse_market(mkt, home_team, away_team, comp_id_to_name)
            if parsed:
                parsed_markets.append(parsed)

        if not parsed_markets:
            return None

        event_id = canonical_event_id(sport_key, home_team, away_team, commence_time)

        return OddsEvent(
            id=event_id,
            sport_key=sport_key,
            sport_title=sport_title,
            commence_time=commence_time,
            home_team=home_team,
            away_team=away_team,
            bookmakers=[
                Bookmaker(
                    key="prophetx",
                    title="ProphetX",
                    last_update=datetime.now(timezone.utc).isoformat(),
                    markets=parsed_markets,
                    event_url="https://www.prophetx.co/",
                )
            ],
        )

    def _parse_market(
        self, market: dict, home_team: str, away_team: str,
        comp_id_to_name: Dict[int, str],
    ) -> Optional[Market]:
        """Parse a single ProphetX market into our Market model.

        Markets have nested selections: selections[0] = side_a levels, selections[1] = side_b levels.
        Each level has: name, competitorId, line, odds (American), displayOdds, stake.
        We take the best price (first level) from each side.
        """
        market_name = (market.get("name") or "").lower()
        market_type = (market.get("type") or "").lower()
        selections = market.get("selections", [])

        if not selections or len(selections) < 2:
            return None

        # Detect period suffix from market name (e.g., "1st Half Moneyline" -> "_h1")
        base_name, period_suffix = detect_period_suffix(market_name)

        # Determine base market type from name
        parsed = None  # type: Optional[Market]
        if "total rounds" in market_name or "total_rounds" in market_type:
            parsed = self._parse_total(selections)
            if parsed:
                parsed.key = "total_rounds"
                return parsed
        elif "distance" in market_name or "fight_distance" in market_type or "fight_to_go_distance" in market_type:
            parsed = self._parse_moneyline(selections, home_team, away_team, comp_id_to_name)
            if parsed:
                # Convert to Yes/No format
                for o in parsed.outcomes:
                    if o.name == home_team or o.name == away_team:
                        pass  # keep team names for now
                parsed.key = "fight_to_go_distance"
                return parsed
        elif "moneyline" in market_name:
            parsed = self._parse_moneyline(selections, home_team, away_team, comp_id_to_name)
        elif market_name.startswith("fixed home") or "spread" in market_name:
            parsed = self._parse_spread(selections, home_team, away_team, comp_id_to_name)
        elif "team_total" in market_type or "team total" in market_name:
            parsed = self._parse_team_total(
                selections, home_team, away_team, comp_id_to_name, market_name, market_type,
            )
        elif "home_total" in market_type or "home total" in market_name:
            parsed = self._parse_total(selections)
            if parsed:
                parsed.key = "team_total_home"
        elif "away_total" in market_type or "away total" in market_name:
            parsed = self._parse_total(selections)
            if parsed:
                parsed.key = "team_total_away"
        elif market_name.startswith("fixed total") or "total" in market_name:
            parsed = self._parse_total(selections)

        if parsed is None:
            return None

        # Apply period suffix to market key (e.g., "h2h" -> "h2h_h1")
        if period_suffix:
            parsed.key = parsed.key + period_suffix

        return parsed

    def _parse_moneyline(
        self, selections: list, home_team: str, away_team: str,
        comp_id_to_name: Dict[int, str],
    ) -> Optional[Market]:
        """Parse moneyline market. Take best price from each side.
        Liquidity = total stake across ALL price levels (full depth).
        """
        outcomes = []  # type: List[Outcome]
        total_liquidity = 0.0

        for side in selections:
            if not side or not isinstance(side, list):
                continue
            # First entry is best price
            best = side[0]
            odds = best.get("odds")
            if odds is None:
                continue

            price = int(odds)
            name = best.get("name", "")

            # Map competitor to home/away
            comp_id = best.get("competitorId")
            resolved = comp_id_to_name.get(comp_id, name)
            if resolved == home_team:
                display_name = home_team
            elif resolved == away_team:
                display_name = away_team
            else:
                # Fallback: try matching by name substring
                display_name = self._match_team(name, home_team, away_team)

            # Liquidity = total stake across ALL price levels (full depth)
            side_liquidity = 0.0
            for level in side:
                stake = level.get("stake")
                if stake:
                    try:
                        side_liquidity += float(stake)
                    except (ValueError, TypeError):
                        pass
            total_liquidity += side_liquidity

            outcomes.append(Outcome(
                name=display_name,
                price=price,
                liquidity=round(side_liquidity, 2) if side_liquidity > 0 else None,
            ))

        if len(outcomes) < 2:
            return None

        return Market(
            key="h2h",
            outcomes=outcomes,
            liquidity=round(total_liquidity, 2) if total_liquidity > 0 else None,
        )

    def _parse_spread(
        self, selections: list, home_team: str, away_team: str,
        comp_id_to_name: Dict[int, str],
    ) -> Optional[Market]:
        """Parse spread market. Take best price and line from each side.
        Liquidity = total stake across ALL price levels (full depth).
        """
        outcomes = []  # type: List[Outcome]
        total_liquidity = 0.0

        for side in selections:
            if not side or not isinstance(side, list):
                continue
            best = side[0]
            odds = best.get("odds")
            if odds is None:
                continue

            price = int(odds)
            name = best.get("name", "")
            line = best.get("line")

            # Get the spread point
            point = None  # type: Optional[float]
            if line is not None:
                try:
                    point = float(line)
                except (ValueError, TypeError):
                    pass

            # Map competitor to home/away
            comp_id = best.get("competitorId")
            resolved = comp_id_to_name.get(comp_id, name)
            if resolved == home_team:
                display_name = home_team
            elif resolved == away_team:
                display_name = away_team
            else:
                display_name = self._match_team(name, home_team, away_team)

            # Liquidity = total stake across ALL price levels (full depth)
            side_liquidity = 0.0
            for level in side:
                stake = level.get("stake")
                if stake:
                    try:
                        side_liquidity += float(stake)
                    except (ValueError, TypeError):
                        pass
            total_liquidity += side_liquidity

            outcomes.append(Outcome(
                name=display_name,
                price=price,
                point=point,
                liquidity=round(side_liquidity, 2) if side_liquidity > 0 else None,
            ))

        if len(outcomes) < 2:
            return None

        return Market(
            key="spreads",
            outcomes=outcomes,
            liquidity=round(total_liquidity, 2) if total_liquidity > 0 else None,
        )

    def _parse_total(self, selections: list) -> Optional[Market]:
        """Parse totals (O/U) market. Take best price and line from each side.
        Liquidity = total stake across ALL price levels (full depth).
        """
        outcomes = []  # type: List[Outcome]
        total_liquidity = 0.0

        for side in selections:
            if not side or not isinstance(side, list):
                continue
            best = side[0]
            odds = best.get("odds")
            if odds is None:
                continue

            price = int(odds)
            name = (best.get("name") or "").lower()
            line = best.get("line")

            # Determine Over/Under from name
            if "over" in name:
                display_name = "Over"
            elif "under" in name:
                display_name = "Under"
            else:
                # Try from displayName
                display = (best.get("displayName") or "").lower()
                if "over" in display:
                    display_name = "Over"
                elif "under" in display:
                    display_name = "Under"
                else:
                    # First side = over, second = under (convention)
                    display_name = "Over" if len(outcomes) == 0 else "Under"

            point = None  # type: Optional[float]
            if line is not None:
                try:
                    point = float(line)
                except (ValueError, TypeError):
                    pass

            # Liquidity = total stake across ALL price levels (full depth)
            side_liquidity = 0.0
            for level in side:
                stake = level.get("stake")
                if stake:
                    try:
                        side_liquidity += float(stake)
                    except (ValueError, TypeError):
                        pass
            total_liquidity += side_liquidity

            outcomes.append(Outcome(
                name=display_name,
                price=price,
                point=point,
                liquidity=round(side_liquidity, 2) if side_liquidity > 0 else None,
            ))

        if len(outcomes) < 2:
            return None

        return Market(
            key="totals",
            outcomes=outcomes,
            liquidity=round(total_liquidity, 2) if total_liquidity > 0 else None,
        )

    def _parse_team_total(
        self, selections: list, home_team: str, away_team: str,
        comp_id_to_name: Dict[int, str], market_name: str, market_type: str,
    ) -> Optional[Market]:
        """Parse team total market. Determine home/away from market name or competitor."""
        # Try to determine which team from market name or type
        side = ""
        lower_name = market_name.lower()
        lower_type = market_type.lower()
        combined = lower_name + " " + lower_type
        home_lower = home_team.lower()
        away_lower = away_team.lower()

        # Check if team name appears in market name
        for word in home_lower.split():
            if len(word) > 2 and word in combined:
                side = "home"
                break
        if not side:
            for word in away_lower.split():
                if len(word) > 2 and word in combined:
                    side = "away"
                    break
        if not side:
            if "home" in combined:
                side = "home"
            elif "away" in combined:
                side = "away"

        # Parse as a regular total (Over/Under)
        parsed = self._parse_total(selections)
        if parsed is None:
            return None

        if side == "home":
            parsed.key = "team_total_home"
        elif side == "away":
            parsed.key = "team_total_away"
        else:
            # Can't determine side, skip
            return None

        return parsed

    @staticmethod
    def _match_team(name: str, home_team: str, away_team: str) -> str:
        """Fuzzy match a competitor name to home or away team."""
        name_lower = name.lower().strip()
        home_lower = home_team.lower()
        away_lower = away_team.lower()

        # Check if the competitor name contains or is contained by team name
        if name_lower in home_lower or home_lower in name_lower:
            return home_team
        if name_lower in away_lower or away_lower in name_lower:
            return away_team

        # Check last word (usually the team mascot/city)
        name_parts = name_lower.split()
        if name_parts:
            last = name_parts[-1]
            if last in home_lower:
                return home_team
            if last in away_lower:
                return away_team

        # Give up, use raw name
        return name

    async def close(self) -> None:
        await self._client.aclose()
