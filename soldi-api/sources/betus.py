"""
BetUS sportsbook scraper.

Uses BetUS's public API to fetch odds for major sports.
No authentication required -- public odds are visible without login.

API pattern:
  https://api.betus.com.pa/betting/offerings/v1/events?sportId={id}

Sport IDs:
  Basketball=1, Football=2, Baseball=3, Hockey=4, MMA=10
"""

import asyncio
import logging
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

BASE_URL = "https://api.betus.com.pa/betting/offerings/v1"

# OddsScreen sport_key -> (BetUS sportId, leagueFilter or None)
BETUS_SPORT_MAP: Dict[str, Tuple[int, Optional[str]]] = {
    "basketball_nba": (1, "NBA"),
    "basketball_ncaab": (1, "NCAAB"),
    "icehockey_nhl": (4, "NHL"),
    "baseball_mlb": (3, "MLB"),
    "mma_mixed_martial_arts": (10, None),
}

# Keywords that indicate a futures/special market (skip these)
_FUTURES_KEYWORDS = frozenset([
    "wins", "champion", "coin toss", "futures", "conference",
    "division", "mvp", "award", "super bowl", "world series",
    "stanley cup", "nba finals", "pennant", "cy young", "heisman",
])

# Sports that support period markets (halves / quarters / periods)
_PERIOD_SPORTS = frozenset([
    "basketball_nba",
    "basketball_ncaab",
    "icehockey_nhl",
    "baseball_mlb",
])

# BetUS period identifiers -> market key suffix
_PERIOD_SUFFIX_MAP = {
    # Basketball
    "1H": "_h1",
    "2H": "_h2",
    "1Q": "_q1",
    "2Q": "_q2",
    "3Q": "_q3",
    "4Q": "_q4",
    # Hockey
    "1P": "_p1",
    "2P": "_p2",
    "3P": "_p3",
    # Baseball
    "F5": "_h1",   # First 5 innings treated as first half
}


class BetUSSource(DataSource):
    """Fetches odds from BetUS's public offerings API."""

    _api_sem = asyncio.Semaphore(4)

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json",
                "Referer": "https://www.betus.com.pa/",
                "Origin": "https://www.betus.com.pa",
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
        headers = {"x-requests-remaining": "unlimited"}

        if bookmakers and "betus" not in bookmakers:
            return [], headers

        sport_config = BETUS_SPORT_MAP.get(sport_key)
        if sport_config is None:
            return [], headers

        sport_id, league_filter = sport_config

        try:
            url = f"{BASE_URL}/events"
            params: Dict[str, str] = {
                "sportId": str(sport_id),
                "marketTypes": "GAME",
                "oddsFormat": "AMERICAN",
            }
            if league_filter:
                params["league"] = league_filter

            async with self._api_sem:
                response = await self._client.get(url, params=params)
                if response.status_code in (400, 429):
                    await asyncio.sleep(2.0)
                    response = await self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            events = self._parse_events(data, sport_key)
            logger.info("BetUS: %d events for %s", len(events), sport_key)
            return events, headers

        except Exception as e:
            logger.warning("BetUS failed for %s: %s", sport_key, e)
            return [], headers

    def _parse_events(self, data: dict, sport_key: str) -> List[OddsEvent]:
        """Parse BetUS API response into OddsEvent list."""
        events_list: List[OddsEvent] = []
        sport_title = get_sport_title(sport_key)
        is_mma = sport_key == "mma_mixed_martial_arts"

        raw_events = data if isinstance(data, list) else data.get("events", [])
        if not isinstance(raw_events, list):
            return []

        for ev in raw_events:
            try:
                event = self._parse_single_event(ev, sport_key, sport_title, is_mma)
                if event:
                    events_list.append(event)
            except Exception as e:
                logger.debug("BetUS: failed to parse event: %s", e)
                continue

        return events_list

    def _parse_single_event(
        self,
        ev: dict,
        sport_key: str,
        sport_title: str,
        is_mma: bool,
    ) -> Optional[OddsEvent]:
        """Parse a single event dict into an OddsEvent."""
        # Skip futures / specials
        event_desc = (ev.get("description") or ev.get("name") or "").lower()
        if any(kw in event_desc for kw in _FUTURES_KEYWORDS):
            return None

        # Extract competitors
        competitors = ev.get("competitors", [])
        if not isinstance(competitors, list) or len(competitors) < 2:
            # Try alternate structure
            home_name = ev.get("homeTeam") or ev.get("home", {}).get("name", "")
            away_name = ev.get("awayTeam") or ev.get("away", {}).get("name", "")
            if not home_name or not away_name:
                return None
        else:
            home_name = ""
            away_name = ""
            for comp in competitors:
                role = (comp.get("role") or comp.get("type") or "").upper()
                name = comp.get("name") or comp.get("description") or ""
                if role == "HOME" or comp.get("home", False):
                    home_name = name
                elif role == "AWAY" or comp.get("away", False):
                    away_name = name
            # Fallback: if no home/away role, use first two competitors
            if not home_name and not away_name and len(competitors) >= 2:
                home_name = competitors[0].get("name") or competitors[0].get("description") or ""
                away_name = competitors[1].get("name") or competitors[1].get("description") or ""

        if not home_name or not away_name:
            return None

        home_name = resolve_team_name(home_name)
        away_name = resolve_team_name(away_name)

        # Parse commence time
        commence_time = self._parse_commence_time(ev)
        if not commence_time:
            return None

        # Parse markets
        all_markets: List[Market] = []
        seen_keys: set = set()

        # Markets may be in different locations depending on API response shape
        market_groups = (
            ev.get("markets", [])
            or ev.get("displayGroups", [])
            or ev.get("offerings", [])
        )
        if isinstance(market_groups, dict):
            # If markets is a dict keyed by type, flatten
            market_groups = list(market_groups.values())
        if not isinstance(market_groups, list):
            market_groups = []

        # Flatten nested market groups
        flat_markets = []
        for mg in market_groups:
            if isinstance(mg, dict):
                inner = mg.get("markets", [])
                if isinstance(inner, list) and inner:
                    flat_markets.extend(inner)
                else:
                    flat_markets.append(mg)
            elif isinstance(mg, list):
                flat_markets.extend(mg)

        for mkt in flat_markets:
            if not isinstance(mkt, dict):
                continue
            parsed_markets = self._parse_market(
                mkt, home_name, away_name, sport_key, is_mma, seen_keys,
            )
            all_markets.extend(parsed_markets)

        if not all_markets:
            return None

        # Build event URL
        event_id_raw = ev.get("id") or ev.get("eventId") or ""
        event_url = f"https://www.betus.com.pa/sportsbook/" if not event_id_raw else None
        if event_id_raw:
            event_url = f"https://www.betus.com.pa/sportsbook/event/{event_id_raw}"

        cid = canonical_event_id(sport_key, home_name, away_name, commence_time)

        return OddsEvent(
            id=cid,
            sport_key=sport_key,
            sport_title=sport_title,
            commence_time=commence_time,
            home_team=home_name,
            away_team=away_name,
            bookmakers=[
                Bookmaker(
                    key="betus",
                    title="BetUS",
                    last_update=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    markets=all_markets,
                    event_url=event_url,
                )
            ],
        )

    def _parse_market(
        self,
        mkt: dict,
        home: str,
        away: str,
        sport_key: str,
        is_mma: bool,
        seen_keys: set,
    ) -> List[Market]:
        """Parse a single market dict, returning zero or more Market objects."""
        results: List[Market] = []
        desc = (
            mkt.get("description")
            or mkt.get("name")
            or mkt.get("type")
            or ""
        )
        desc_lower = desc.lower()
        desc_upper = desc.upper().replace(" ", "_")
        outcomes_raw = mkt.get("outcomes") or mkt.get("selections") or []
        if not isinstance(outcomes_raw, list) or len(outcomes_raw) < 2:
            return []

        # Determine period suffix
        period = mkt.get("period") or mkt.get("periodDescription") or ""
        if isinstance(period, dict):
            period = period.get("description") or period.get("id") or ""
        period = str(period).strip()
        suffix = _PERIOD_SUFFIX_MAP.get(period, "")

        # Only allow period markets for supported sports
        if suffix and sport_key not in _PERIOD_SPORTS:
            suffix = ""

        # Skip combo / parlay markets
        if " and " in desc_lower or " & " in desc_lower:
            return []

        # Skip alternate lines
        if "alternate" in desc_lower or "alt " in desc_lower:
            return []

        # --- MMA-specific markets ---
        if is_mma:
            if "go the distance" in desc_lower or "distance" in desc_lower:
                key = "fight_to_go_distance"
                if key not in seen_keys:
                    seen_keys.add(key)
                    parsed = self._parse_yes_no(outcomes_raw)
                    if parsed:
                        results.append(Market(key=key, outcomes=parsed))
                return results

            if "over/under rounds" in desc_lower or "total rounds" in desc_lower:
                key = "over_under_rounds"
                if key not in seen_keys:
                    seen_keys.add(key)
                    parsed = self._parse_total_outcomes(outcomes_raw)
                    if parsed:
                        results.append(Market(key=key, outcomes=parsed))
                return results

        # --- Detect team totals ---
        if " - " in desc and ("TOTAL" in desc_upper or "TEAM_TOTAL" in desc_upper):
            parts = desc.split(" - ", 1)
            team_part = parts[1].strip() if len(parts) > 1 else ""
            if team_part:
                team_lower = team_part.lower()
                home_lower = home.lower()
                away_lower = away.lower()
                team_total_key = None
                if team_lower == home_lower or home_lower in team_lower or team_lower in home_lower:
                    team_total_key = "team_total_home"
                elif team_lower == away_lower or away_lower in team_lower or team_lower in away_lower:
                    team_total_key = "team_total_away"

                if team_total_key:
                    full_key = team_total_key + suffix
                    if full_key not in seen_keys:
                        seen_keys.add(full_key)
                        parsed = self._parse_total_outcomes(outcomes_raw)
                        if parsed:
                            results.append(Market(key=full_key, outcomes=parsed))
                    return results

        # Also detect team totals via explicit market type field
        market_type = (mkt.get("marketType") or mkt.get("type") or "").upper()
        if "TEAM_TOTAL" in market_type or "TEAMTOTAL" in market_type:
            team_ref = mkt.get("teamRef") or mkt.get("team") or ""
            if isinstance(team_ref, dict):
                team_ref = team_ref.get("name", "")
            team_ref_lower = str(team_ref).lower()
            team_total_key = None
            if team_ref_lower and (home.lower() in team_ref_lower or team_ref_lower in home.lower()):
                team_total_key = "team_total_home"
            elif team_ref_lower and (away.lower() in team_ref_lower or team_ref_lower in away.lower()):
                team_total_key = "team_total_away"
            # Fallback: check outcome names
            if not team_total_key:
                for o in outcomes_raw:
                    o_name = (o.get("description") or o.get("name") or "").lower()
                    if home.lower() in o_name:
                        team_total_key = "team_total_home"
                        break
                    elif away.lower() in o_name:
                        team_total_key = "team_total_away"
                        break

            if team_total_key:
                full_key = team_total_key + suffix
                if full_key not in seen_keys:
                    seen_keys.add(full_key)
                    parsed = self._parse_total_outcomes(outcomes_raw)
                    if parsed:
                        results.append(Market(key=full_key, outcomes=parsed))
                return results

        # --- Standard market classification ---
        base = None
        if (
            "MONEYLINE" in desc_upper
            or "MONEY_LINE" in desc_upper
            or market_type in ("MONEYLINE", "MONEY_LINE", "H2H")
        ):
            base = "h2h"
        elif (
            "SPREAD" in desc_upper
            or "HANDICAP" in desc_upper
            or market_type in ("SPREAD", "POINT_SPREAD", "HANDICAP")
        ):
            base = "spreads"
        elif (
            "TOTAL" in desc_upper
            or "OVER_UNDER" in desc_upper
            or market_type in ("TOTAL", "OVER_UNDER", "TOTALS")
        ):
            base = "totals"

        if base is None:
            return []

        market_key = base + suffix
        if market_key in seen_keys:
            return []
        seen_keys.add(market_key)

        if base == "h2h":
            parsed = self._parse_moneyline(outcomes_raw, home, away)
            if parsed:
                results.append(Market(key=market_key, outcomes=parsed))
        elif base == "spreads":
            parsed = self._parse_spread_outcomes(outcomes_raw, home, away)
            if parsed:
                results.append(Market(key=market_key, outcomes=parsed))
        elif base == "totals":
            parsed = self._parse_total_outcomes(outcomes_raw)
            if parsed:
                results.append(Market(key=market_key, outcomes=parsed))

        return results

    # -- Outcome parsers -------------------------------------------------------

    @staticmethod
    def _parse_american_odds(raw) -> Optional[int]:
        """Convert raw odds value to American integer. Handles 'EVEN' -> 100."""
        if raw is None:
            return None
        s = str(raw).strip().upper()
        if s == "EVEN":
            return 100
        try:
            return int(float(s.replace("+", "")))
        except (ValueError, TypeError):
            return None

    def _parse_moneyline(
        self, outcomes: list, home: str, away: str,
    ) -> List[Outcome]:
        """Parse moneyline outcomes."""
        result = []
        for o in outcomes:
            odds = self._extract_odds(o)
            if odds is None:
                continue
            name = o.get("description") or o.get("name") or o.get("label") or ""
            if not name:
                continue
            result.append(Outcome(name=name, price=odds))
        return result if len(result) >= 2 else []

    def _parse_spread_outcomes(
        self, outcomes: list, home: str, away: str,
    ) -> List[Outcome]:
        """Parse spread/handicap outcomes."""
        result = []
        for o in outcomes:
            odds = self._extract_odds(o)
            point = self._extract_point(o)
            if odds is None or point is None:
                continue
            name = o.get("description") or o.get("name") or o.get("label") or ""
            if not name:
                continue
            result.append(Outcome(name=name, price=odds, point=point))
        return result if len(result) >= 2 else []

    def _parse_total_outcomes(self, outcomes: list) -> List[Outcome]:
        """Parse total (over/under) outcomes."""
        result = []
        for o in outcomes:
            odds = self._extract_odds(o)
            point = self._extract_point(o)
            if odds is None or point is None:
                continue
            desc = (o.get("description") or o.get("name") or o.get("label") or "").lower()
            if "over" in desc:
                name = "Over"
            elif "under" in desc:
                name = "Under"
            else:
                name = o.get("description") or o.get("name") or ""
            result.append(Outcome(name=name, price=odds, point=point))
        return result if len(result) >= 2 else []

    def _parse_yes_no(self, outcomes: list) -> List[Outcome]:
        """Parse a Yes/No market (e.g., fight to go the distance)."""
        result = []
        for o in outcomes:
            odds = self._extract_odds(o)
            if odds is None:
                continue
            desc = (o.get("description") or o.get("name") or o.get("label") or "").lower()
            if "yes" in desc:
                name = "Yes"
            elif "no" in desc:
                name = "No"
            else:
                name = o.get("description") or o.get("name") or ""
            result.append(Outcome(name=name, price=odds))
        return result if len(result) >= 2 else []

    def _extract_odds(self, outcome: dict) -> Optional[int]:
        """Extract American odds from an outcome dict, trying multiple field names."""
        # Try direct american odds field
        for field in ("americanOdds", "american", "odds", "price"):
            val = outcome.get(field)
            if val is not None:
                parsed = self._parse_american_odds(val)
                if parsed is not None:
                    return parsed

        # Try nested price object
        price_obj = outcome.get("price") or outcome.get("odds")
        if isinstance(price_obj, dict):
            for field in ("american", "americanOdds", "value"):
                val = price_obj.get(field)
                if val is not None:
                    parsed = self._parse_american_odds(val)
                    if parsed is not None:
                        return parsed

        return None

    @staticmethod
    def _extract_point(outcome: dict) -> Optional[float]:
        """Extract point/handicap/line from an outcome dict."""
        for field in ("point", "handicap", "line", "spread", "points"):
            val = outcome.get(field)
            if val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue

        # Try nested price object
        price_obj = outcome.get("price") or outcome.get("odds")
        if isinstance(price_obj, dict):
            for field in ("handicap", "point", "line"):
                val = price_obj.get(field)
                if val is not None:
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        continue

        return None

    @staticmethod
    def _parse_commence_time(ev: dict) -> Optional[str]:
        """Extract and format commence time from event data."""
        # Try ISO string fields
        for field in ("startTime", "commence_time", "date", "eventDate", "scheduledStart"):
            val = ev.get(field)
            if isinstance(val, str) and val:
                # Already ISO format
                if "T" in val:
                    return val if val.endswith("Z") or "+" in val else val + "Z"
                # Try parsing date-only
                try:
                    dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                    return dt.isoformat().replace("+00:00", "Z")
                except (ValueError, TypeError):
                    pass

        # Try epoch milliseconds
        for field in ("startTime", "startTimeMs", "commence_time_ms", "eventTime"):
            val = ev.get(field)
            if isinstance(val, (int, float)) and val > 1_000_000_000:
                # If > 10 billion, assume milliseconds
                if val > 10_000_000_000:
                    val = val / 1000
                dt = datetime.fromtimestamp(val, tz=timezone.utc)
                return dt.isoformat().replace("+00:00", "Z")

        return None

    async def close(self) -> None:
        """Clean up the HTTP client."""
        await self._client.aclose()
