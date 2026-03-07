"""
Stake.us sportsbook scraper.
Uses Stake.us public GraphQL API at stake.us/api/graphql.
No authentication required.
"""

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    decimal_to_american,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://stake.us/api/graphql"

BOOKMAKER_KEY = "stakeus"
BOOKMAKER_TITLE = "Stake.us"

# ─── Sport key → Stake sport slug ────────────────────────────────
_SPORT_SLUG_MAP: Dict[str, str] = {
    "basketball_nba": "basketball",
    "basketball_ncaab": "basketball",
    "icehockey_nhl": "ice-hockey",
    "baseball_mlb": "baseball",
    "mma_mixed_martial_arts": "mma",
}

# ─── Sport key → competition name patterns for filtering ─────────
_COMP_NAME_MAP: Dict[str, List[str]] = {
    "basketball_nba": ["NBA"],
    "basketball_ncaab": ["NCAAB", "NCAA", "College Basketball"],
    "icehockey_nhl": ["NHL"],
    "baseball_mlb": ["MLB"],
    "mma_mixed_martial_arts": ["UFC", "MMA"],
}

# ─── Period detection from market names ──────────────────────────
_PERIOD_NAME_PATTERNS = [
    (re.compile(r"1st\s+quarter", re.I), "_q1"),
    (re.compile(r"2nd\s+quarter", re.I), "_q2"),
    (re.compile(r"3rd\s+quarter", re.I), "_q3"),
    (re.compile(r"4th\s+quarter", re.I), "_q4"),
    (re.compile(r"1st\s+half", re.I), "_h1"),
    (re.compile(r"2nd\s+half", re.I), "_h2"),
    (re.compile(r"1st\s+period", re.I), "_p1"),
    (re.compile(r"2nd\s+period", re.I), "_p2"),
    (re.compile(r"3rd\s+period", re.I), "_p3"),
    (re.compile(r"first\s+5\s+innings", re.I), "_f5"),
    (re.compile(r"1st\s+5\s+innings", re.I), "_f5"),
]


def _detect_period_from_name(market_name: str) -> str:
    """Detect period suffix from market name."""
    for pattern, suffix in _PERIOD_NAME_PATTERNS:
        if pattern.search(market_name):
            return suffix
    return ""


def _extract_line(name: str) -> Optional[float]:
    """
    Extract a numeric line from a selection name.
    E.g. "Over 228.5" -> 228.5, "Warriors -3.5" -> -3.5
    """
    m = re.search(r"([+-]?\d+\.?\d*)\s*$", name)
    if m:
        return float(m.group(1))
    return None


# ─── GraphQL queries ─────────────────────────────────────────────

# Query to list available sport categories and competitions (leagues)
_SPORT_COMPETITIONS_QUERY = """
query SportCompetitions($sportSlug: String!) {
  sport(slug: $sportSlug) {
    slug
    name
    tournaments {
      id
      slug
      name
      category {
        id
        slug
        name
      }
      events(limit: 0) {
        totalCount
      }
    }
  }
}
"""

# Query to fetch events with markets for a specific competition/tournament
_EVENTS_QUERY = """
query SportEvents($sportSlug: String!, $tournamentSlug: String, $limit: Int, $offset: Int) {
  sport(slug: $sportSlug) {
    tournaments(filter: {slug: $tournamentSlug}) {
      id
      slug
      name
      events(limit: $limit, offset: $offset) {
        nodes {
          id
          slug
          name
          status
          startTime
          sport {
            slug
          }
          tournament {
            slug
            name
          }
          competitors {
            id
            name
            qualifier
          }
          markets {
            id
            name
            status
            type
            outcomes {
              id
              name
              odds
              status
            }
          }
        }
        totalCount
      }
    }
  }
}
"""

# Alternate: fetch all upcoming events for a sport slug directly
_ALL_EVENTS_QUERY = """
query AllSportEvents($sportSlug: String!, $limit: Int, $offset: Int) {
  sport(slug: $sportSlug) {
    slug
    name
    upcomingEvents(limit: $limit, offset: $offset) {
      nodes {
        id
        slug
        name
        status
        startTime
        sport {
          slug
        }
        tournament {
          slug
          name
          category {
            slug
            name
          }
        }
        competitors {
          id
          name
          qualifier
        }
        markets {
          id
          name
          status
          type
          outcomes {
            id
            name
            odds
            status
          }
        }
      }
      totalCount
    }
  }
}
"""


class StakeUsSource(DataSource):
    """Fetches odds from Stake.us via their public GraphQL API."""

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=20.0,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Origin": "https://stake.us",
                "Referer": "https://stake.us/",
            },
        )
        # Cache: sport_key -> list of tournament slugs
        self._tournament_slugs: Dict[str, List[str]] = {}
        self._tournaments_fetched: bool = False

    async def _discover_tournaments(self) -> None:
        """Discover available tournaments/competitions per sport slug."""
        if self._tournaments_fetched:
            return

        for sport_key, sport_slug in _SPORT_SLUG_MAP.items():
            if sport_key in self._tournament_slugs:
                continue
            try:
                resp = await self._client.post(
                    GRAPHQL_URL,
                    json={
                        "query": _SPORT_COMPETITIONS_QUERY,
                        "variables": {"sportSlug": sport_slug},
                    },
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.warning("StakeUs: tournament discovery failed for %s: %s", sport_slug, e)
                continue

            if "errors" in data:
                logger.warning("StakeUs: GraphQL errors for %s: %s", sport_slug, data["errors"][:2])
                continue

            sport_data = data.get("data", {}).get("sport")
            if not sport_data:
                continue

            tournaments = sport_data.get("tournaments", [])
            patterns = _COMP_NAME_MAP.get(sport_key, [])

            for tourn in tournaments:
                tourn_name = tourn.get("name", "")
                tourn_slug = tourn.get("slug", "")
                total_count = (tourn.get("events") or {}).get("totalCount", 0)
                if not tourn_slug or total_count == 0:
                    continue

                # Check if tournament name matches our competition patterns
                cat_name = (tourn.get("category") or {}).get("name", "")
                combined = f"{cat_name} {tourn_name}".lower()

                for pat in patterns:
                    if pat.lower() in combined or combined in pat.lower():
                        if sport_key not in self._tournament_slugs:
                            self._tournament_slugs[sport_key] = []
                        if tourn_slug not in self._tournament_slugs[sport_key]:
                            self._tournament_slugs[sport_key].append(tourn_slug)
                            logger.info(
                                "StakeUs: %s -> tournament '%s' (slug=%s, %d events)",
                                sport_key, tourn_name, tourn_slug, total_count,
                            )
                        break

            # MMA: also include all tournaments if patterns match broadly
            if sport_key == "mma_mixed_martial_arts":
                for tourn in tournaments:
                    tourn_slug = tourn.get("slug", "")
                    total_count = (tourn.get("events") or {}).get("totalCount", 0)
                    if not tourn_slug or total_count == 0:
                        continue
                    if sport_key not in self._tournament_slugs:
                        self._tournament_slugs[sport_key] = []
                    if tourn_slug not in self._tournament_slugs[sport_key]:
                        self._tournament_slugs[sport_key].append(tourn_slug)

        self._tournaments_fetched = True
        total = sum(len(v) for v in self._tournament_slugs.values())
        logger.info(
            "StakeUs: discovered %d tournaments across %d sports",
            total, len(self._tournament_slugs),
        )

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        if bookmakers and BOOKMAKER_KEY not in bookmakers:
            return [], {"x-requests-remaining": "unlimited"}

        sport_slug = _SPORT_SLUG_MAP.get(sport_key)
        if not sport_slug:
            return [], {"x-requests-remaining": "unlimited"}

        # Discover tournaments if needed
        await self._discover_tournaments()

        tournament_slugs = self._tournament_slugs.get(sport_key, [])

        try:
            all_raw_events: List[dict] = []

            if tournament_slugs:
                # Fetch per-tournament
                tasks = [
                    self._fetch_tournament_events(sport_slug, t_slug)
                    for t_slug in tournament_slugs
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        logger.warning("StakeUs: tournament fetch error: %s", result)
                        continue
                    all_raw_events.extend(result)
            else:
                # Fallback: fetch all upcoming events for the sport
                all_raw_events = await self._fetch_all_events(sport_slug)

            # Parse events
            parsed: List[OddsEvent] = []
            sport_title = get_sport_title(sport_key)
            is_mma = sport_key == "mma_mixed_martial_arts"

            for event_data in all_raw_events:
                event = self._parse_event(event_data, sport_key, sport_title, is_mma)
                if event:
                    parsed.append(event)

            logger.info("StakeUs: %d events for %s", len(parsed), sport_key)
            return parsed, {"x-requests-remaining": "unlimited"}

        except Exception as e:
            logger.warning("StakeUs: failed for %s: %s", sport_key, e)
            return [], {"x-requests-remaining": "unlimited"}

    async def _fetch_tournament_events(
        self, sport_slug: str, tournament_slug: str
    ) -> List[dict]:
        """Fetch events for a specific tournament."""
        try:
            resp = await self._client.post(
                GRAPHQL_URL,
                json={
                    "query": _EVENTS_QUERY,
                    "variables": {
                        "sportSlug": sport_slug,
                        "tournamentSlug": tournament_slug,
                        "limit": 200,
                        "offset": 0,
                    },
                },
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(
                "StakeUs: events fetch failed for %s/%s: %s",
                sport_slug, tournament_slug, e,
            )
            return []

        if "errors" in data:
            logger.warning("StakeUs: GraphQL errors: %s", data["errors"][:2])
            return []

        tournaments = (
            data.get("data", {}).get("sport", {}).get("tournaments", [])
        )
        events: List[dict] = []
        for tourn in tournaments:
            nodes = (tourn.get("events") or {}).get("nodes", [])
            events.extend(nodes)
        return events

    async def _fetch_all_events(self, sport_slug: str) -> List[dict]:
        """Fetch all upcoming events for a sport slug."""
        try:
            resp = await self._client.post(
                GRAPHQL_URL,
                json={
                    "query": _ALL_EVENTS_QUERY,
                    "variables": {
                        "sportSlug": sport_slug,
                        "limit": 200,
                        "offset": 0,
                    },
                },
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning("StakeUs: all events fetch failed for %s: %s", sport_slug, e)
            return []

        if "errors" in data:
            logger.warning("StakeUs: GraphQL errors: %s", data["errors"][:2])
            return []

        sport_data = data.get("data", {}).get("sport", {})
        nodes = (sport_data.get("upcomingEvents") or {}).get("nodes", [])
        return nodes

    # ─── Event parsing ────────────────────────────────────────────

    def _parse_event(
        self,
        event_data: dict,
        sport_key: str,
        sport_title: str,
        is_mma: bool = False,
    ) -> Optional[OddsEvent]:
        """Parse a single event from the GraphQL response into an OddsEvent."""
        status = event_data.get("status", "")
        # Skip live/settled events, only want pre-match
        if status and status.lower() not in ("prematch", "upcoming", "open", "not_started", ""):
            return None

        # Extract team names from competitors
        competitors = event_data.get("competitors") or []
        home_name = ""
        away_name = ""

        if len(competitors) >= 2:
            for comp in competitors:
                qualifier = (comp.get("qualifier") or "").lower()
                name = resolve_team_name(comp.get("name", ""))
                if qualifier == "home":
                    home_name = name
                elif qualifier == "away":
                    away_name = name

            # Fallback: if qualifiers not set, use order (first=home, second=away)
            if not home_name and not away_name:
                home_name = resolve_team_name(competitors[0].get("name", ""))
                away_name = resolve_team_name(competitors[1].get("name", ""))
        else:
            # Try parsing from event name: "Team A vs Team B" or "Team A v Team B"
            event_name = event_data.get("name", "")
            m = re.match(r"(.+?)\s+(?:vs?\.?|@)\s+(.+)", event_name, re.IGNORECASE)
            if m:
                away_name = resolve_team_name(m.group(1).strip())
                home_name = resolve_team_name(m.group(2).strip())
            else:
                return None

        if not home_name or not away_name:
            return None

        # Parse start time
        start_time = event_data.get("startTime")
        if not start_time:
            return None

        # startTime may be ISO string or epoch
        if isinstance(start_time, (int, float)):
            dt = datetime.fromtimestamp(start_time / 1000, tz=timezone.utc)
            commence_time = dt.isoformat()
        elif isinstance(start_time, str):
            # Ensure it has timezone info
            if start_time.endswith("Z"):
                commence_time = start_time.replace("Z", "+00:00")
            elif "+" not in start_time and "-" not in start_time[10:]:
                commence_time = start_time + "+00:00"
            else:
                commence_time = start_time
        else:
            return None

        # Parse markets
        raw_markets = event_data.get("markets") or []
        parsed_markets = self._parse_markets(raw_markets, home_name, away_name, is_mma)

        if not parsed_markets:
            return None

        cid = canonical_event_id(sport_key, home_name, away_name, commence_time)

        event_slug = event_data.get("slug", "")
        sport_slug = _SPORT_SLUG_MAP.get(sport_key, "")
        event_url = f"https://stake.us/sports/{sport_slug}/{event_slug}" if event_slug else None

        return OddsEvent(
            id=cid,
            sport_key=sport_key,
            sport_title=sport_title,
            commence_time=commence_time,
            home_team=home_name,
            away_team=away_name,
            bookmakers=[
                Bookmaker(
                    key=BOOKMAKER_KEY,
                    title=BOOKMAKER_TITLE,
                    markets=parsed_markets,
                    event_url=event_url,
                )
            ],
        )

    # ─── Market parsing ──────────────────────────────────────────

    def _parse_markets(
        self,
        raw_markets: list,
        home_name: str,
        away_name: str,
        is_mma: bool = False,
    ) -> List[Market]:
        """Parse all markets for an event."""
        result: List[Market] = []
        # Track which market keys we've already added to avoid duplicates
        seen_keys: set = set()

        # Separate markets by type for structured parsing
        moneyline_markets: List[dict] = []
        spread_markets: List[dict] = []
        total_markets: List[dict] = []
        home_total_markets: List[dict] = []
        away_total_markets: List[dict] = []
        gtd_market: Optional[dict] = None
        ou_rounds_market: Optional[dict] = None

        # Period-specific buckets: period_suffix -> list of markets
        period_moneyline: Dict[str, List[dict]] = {}
        period_spread: Dict[str, List[dict]] = {}
        period_total: Dict[str, List[dict]] = {}
        period_home_total: Dict[str, List[dict]] = {}
        period_away_total: Dict[str, List[dict]] = {}

        for mkt in raw_markets:
            mkt_status = (mkt.get("status") or "").lower()
            if mkt_status in ("suspended", "closed", "settled"):
                continue

            mkt_name = mkt.get("name", "")
            mkt_type = (mkt.get("type") or "").lower()
            mkt_name_lower = mkt_name.lower()

            outcomes = mkt.get("outcomes") or []
            if not outcomes:
                continue

            # Detect period from market name
            period_suffix = _detect_period_from_name(mkt_name)

            # ── MMA special markets ──────────────────────────────
            if is_mma:
                if "go the distance" in mkt_name_lower or "goes the distance" in mkt_name_lower:
                    gtd_market = mkt
                    continue
                if ("over/under" in mkt_name_lower or "total rounds" in mkt_name_lower) and "round" in mkt_name_lower:
                    ou_rounds_market = mkt
                    continue

            # ── Classify market by name/type ─────────────────────
            # Moneyline / Winner / Match Result
            if self._is_moneyline_market(mkt_name_lower, mkt_type):
                if period_suffix:
                    period_moneyline.setdefault(period_suffix, []).append(mkt)
                else:
                    moneyline_markets.append(mkt)

            # Spread / Handicap / Point Spread
            elif self._is_spread_market(mkt_name_lower, mkt_type):
                if period_suffix:
                    period_spread.setdefault(period_suffix, []).append(mkt)
                else:
                    spread_markets.append(mkt)

            # Team Totals
            elif self._is_team_total_market(mkt_name_lower, mkt_type, home_name, away_name):
                is_home = self._is_home_team_total(mkt_name_lower, home_name, away_name)
                if period_suffix:
                    if is_home:
                        period_home_total.setdefault(period_suffix, []).append(mkt)
                    else:
                        period_away_total.setdefault(period_suffix, []).append(mkt)
                else:
                    if is_home:
                        home_total_markets.append(mkt)
                    else:
                        away_total_markets.append(mkt)

            # Total / Over Under
            elif self._is_total_market(mkt_name_lower, mkt_type):
                if period_suffix:
                    period_total.setdefault(period_suffix, []).append(mkt)
                else:
                    total_markets.append(mkt)

        # ── Build full-game markets ──────────────────────────────
        self._add_moneyline(result, seen_keys, "h2h", moneyline_markets, home_name, away_name)
        self._add_spread(result, seen_keys, "spreads", spread_markets, home_name, away_name)
        self._add_total(result, seen_keys, "totals", total_markets)
        self._add_total(result, seen_keys, "team_total_home", home_total_markets)
        self._add_total(result, seen_keys, "team_total_away", away_total_markets)

        # ── Build period markets ─────────────────────────────────
        for suffix, mkts in period_moneyline.items():
            self._add_moneyline(result, seen_keys, f"h2h{suffix}", mkts, home_name, away_name)
        for suffix, mkts in period_spread.items():
            self._add_spread(result, seen_keys, f"spreads{suffix}", mkts, home_name, away_name)
        for suffix, mkts in period_total.items():
            self._add_total(result, seen_keys, f"totals{suffix}", mkts)
        for suffix, mkts in period_home_total.items():
            self._add_total(result, seen_keys, f"team_total_home{suffix}", mkts)
        for suffix, mkts in period_away_total.items():
            self._add_total(result, seen_keys, f"team_total_away{suffix}", mkts)

        # ── MMA special markets ──────────────────────────────────
        if gtd_market and "fight_to_go_distance" not in seen_keys:
            gtd_outcomes = self._parse_yes_no_outcomes(gtd_market)
            if gtd_outcomes:
                result.append(Market(key="fight_to_go_distance", outcomes=gtd_outcomes))
                seen_keys.add("fight_to_go_distance")

        if ou_rounds_market and "over_under_rounds" not in seen_keys:
            ou_outcomes = self._parse_total_outcomes(ou_rounds_market)
            if ou_outcomes:
                result.append(Market(key="over_under_rounds", outcomes=ou_outcomes))
                seen_keys.add("over_under_rounds")

        return result

    # ─── Market classification helpers ───────────────────────────

    @staticmethod
    def _is_moneyline_market(name_lower: str, mkt_type: str) -> bool:
        keywords = ("moneyline", "money line", "winner", "match result", "to win", "match winner")
        type_keywords = ("moneyline", "winner", "match_result", "1x2")
        return any(k in name_lower for k in keywords) or any(k in mkt_type for k in type_keywords)

    @staticmethod
    def _is_spread_market(name_lower: str, mkt_type: str) -> bool:
        keywords = ("spread", "handicap", "point spread", "puck line", "run line")
        type_keywords = ("spread", "handicap")
        return any(k in name_lower for k in keywords) or any(k in mkt_type for k in type_keywords)

    @staticmethod
    def _is_total_market(name_lower: str, mkt_type: str) -> bool:
        keywords = ("total", "over/under", "over / under")
        type_keywords = ("total", "over_under", "overunder")
        return any(k in name_lower for k in keywords) or any(k in mkt_type for k in type_keywords)

    @staticmethod
    def _is_team_total_market(name_lower: str, mkt_type: str, home: str, away: str) -> bool:
        """Check if this is a team-specific total market."""
        if "team total" in name_lower:
            return True
        # Check if market name contains a specific team name with total
        home_lower = home.lower()
        away_lower = away.lower()
        if ("total" in name_lower or "over/under" in name_lower):
            if home_lower in name_lower or away_lower in name_lower:
                return True
        return False

    @staticmethod
    def _is_home_team_total(name_lower: str, home: str, away: str) -> bool:
        """Determine whether a team total market is for the home or away team."""
        home_lower = home.lower()
        if home_lower in name_lower:
            return True
        if "home" in name_lower:
            return True
        return False

    # ─── Market building helpers ─────────────────────────────────

    def _add_moneyline(
        self,
        result: List[Market],
        seen: set,
        key: str,
        mkts: List[dict],
        home_name: str,
        away_name: str,
    ) -> None:
        if key in seen or not mkts:
            return
        # Take the first valid moneyline market
        for mkt in mkts:
            outcomes = self._parse_moneyline_outcomes(mkt, home_name, away_name)
            if outcomes:
                result.append(Market(key=key, outcomes=outcomes))
                seen.add(key)
                return

    def _add_spread(
        self,
        result: List[Market],
        seen: set,
        key: str,
        mkts: List[dict],
        home_name: str,
        away_name: str,
    ) -> None:
        if key in seen or not mkts:
            return
        best = self._pick_main_line(mkts)
        if best:
            outcomes = self._parse_spread_outcomes(best, home_name, away_name)
            if outcomes:
                result.append(Market(key=key, outcomes=outcomes))
                seen.add(key)

    def _add_total(
        self,
        result: List[Market],
        seen: set,
        key: str,
        mkts: List[dict],
    ) -> None:
        if key in seen or not mkts:
            return
        best = self._pick_main_line(mkts)
        if best:
            outcomes = self._parse_total_outcomes(best)
            if outcomes:
                result.append(Market(key=key, outcomes=outcomes))
                seen.add(key)

    # ─── Outcome parsing ─────────────────────────────────────────

    def _parse_moneyline_outcomes(
        self, mkt: dict, home_name: str, away_name: str
    ) -> List[Outcome]:
        """Parse moneyline/winner market outcomes."""
        outcomes_data = mkt.get("outcomes") or []
        result: List[Outcome] = []
        for out in outcomes_data:
            if (out.get("status") or "").lower() in ("suspended", "closed"):
                continue
            odds = out.get("odds")
            if not odds:
                continue
            try:
                decimal_odds = float(odds)
            except (ValueError, TypeError):
                continue
            if decimal_odds <= 1.0:
                continue

            american = decimal_to_american(decimal_odds)
            out_name = out.get("name", "")

            # Resolve team names
            resolved = resolve_team_name(out_name)
            if not resolved:
                resolved = out_name

            # Map Draw outcome
            out_name_lower = out_name.lower()
            if out_name_lower in ("draw", "tie", "x"):
                resolved = "Draw"

            result.append(Outcome(name=resolved, price=american))

        return result if len(result) >= 2 else []

    def _parse_spread_outcomes(
        self, mkt: dict, home_name: str, away_name: str
    ) -> List[Outcome]:
        """Parse spread/handicap market outcomes."""
        outcomes_data = mkt.get("outcomes") or []
        result: List[Outcome] = []
        for out in outcomes_data:
            if (out.get("status") or "").lower() in ("suspended", "closed"):
                continue
            odds = out.get("odds")
            if not odds:
                continue
            try:
                decimal_odds = float(odds)
            except (ValueError, TypeError):
                continue
            if decimal_odds <= 1.0:
                continue

            american = decimal_to_american(decimal_odds)
            out_name = out.get("name", "")

            # Extract point from outcome name
            point = _extract_line(out_name)

            # Resolve team name (strip trailing number)
            name_part = re.sub(r"\s*[+-]?\d+\.?\d*\s*$", "", out_name).strip()
            resolved = resolve_team_name(name_part) or name_part

            result.append(Outcome(name=resolved, price=american, point=point))

        return result if len(result) >= 2 else []

    def _parse_total_outcomes(self, mkt: dict) -> List[Outcome]:
        """Parse total (over/under) market outcomes."""
        outcomes_data = mkt.get("outcomes") or []
        result: List[Outcome] = []
        for out in outcomes_data:
            if (out.get("status") or "").lower() in ("suspended", "closed"):
                continue
            odds = out.get("odds")
            if not odds:
                continue
            try:
                decimal_odds = float(odds)
            except (ValueError, TypeError):
                continue
            if decimal_odds <= 1.0:
                continue

            american = decimal_to_american(decimal_odds)
            out_name = out.get("name", "")
            out_name_lower = out_name.lower()

            if "over" in out_name_lower:
                name = "Over"
            elif "under" in out_name_lower:
                name = "Under"
            else:
                name = out_name

            point = _extract_line(out_name)

            result.append(Outcome(name=name, price=american, point=point))

        return result if len(result) >= 2 else []

    def _parse_yes_no_outcomes(self, mkt: dict) -> List[Outcome]:
        """Parse a Yes/No market (e.g., fight to go the distance)."""
        outcomes_data = mkt.get("outcomes") or []
        result: List[Outcome] = []
        for out in outcomes_data:
            if (out.get("status") or "").lower() in ("suspended", "closed"):
                continue
            odds = out.get("odds")
            if not odds:
                continue
            try:
                decimal_odds = float(odds)
            except (ValueError, TypeError):
                continue
            if decimal_odds <= 1.0:
                continue

            american = decimal_to_american(decimal_odds)
            out_name = out.get("name", "")
            out_name_lower = out_name.lower()

            if "yes" in out_name_lower:
                name = "Yes"
            elif "no" in out_name_lower:
                name = "No"
            else:
                name = out_name

            result.append(Outcome(name=name, price=american))

        return result if len(result) >= 2 else []

    # ─── Line selection ──────────────────────────────────────────

    @staticmethod
    def _pick_main_line(markets: List[dict]) -> Optional[dict]:
        """Pick the market with odds closest to -110/-110 (1.909 decimal)."""
        target = 1.909
        best: Optional[dict] = None
        best_score = float("inf")

        for mkt in markets:
            outcomes = mkt.get("outcomes") or []
            if len(outcomes) < 2:
                continue

            total_dev = 0.0
            valid = True
            for out in outcomes:
                if (out.get("status") or "").lower() in ("suspended", "closed"):
                    valid = False
                    break
                try:
                    odds = float(out.get("odds", 0))
                except (ValueError, TypeError):
                    valid = False
                    break
                if odds <= 1.0:
                    valid = False
                    break
                total_dev += abs(odds - target)

            if not valid:
                continue

            score = total_dev / len(outcomes)
            if score < best_score:
                best_score = score
                best = mkt

        return best

    # ─── Cleanup ─────────────────────────────────────────────────

    async def close(self) -> None:
        """Clean up the HTTP client."""
        await self._client.aclose()
