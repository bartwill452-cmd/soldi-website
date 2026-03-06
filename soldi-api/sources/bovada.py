"""
Bovada sportsbook scraper.
Uses Bovada's public coupon API to fetch odds.
No authentication required.
"""

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome, PlayerProp
from sources.base import DataSource
from market_keys import detect_period_suffix, classify_base_market
from sources.sport_mapping import (
    BOVADA_SPORT_PATHS,
    canonical_event_id,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.bovada.lv/services/sports/event/coupon/events/A/description"

# Bovada period.id → market key suffix
_BOVADA_PERIOD_MAP = {
    "": "",           # No period field → full game (legacy)
    # Soccer
    "100": "",        # Regulation Time (soccer)
    "102": "_h1",     # First Half (soccer)
    "103": "_h2",     # Second Half (soccer)
    # Hockey (NHL)
    "185": "",        # Regulation Time (hockey)
    "186": "_p1",     # 1st Period (hockey)
    "187": "_p2",     # 2nd Period (hockey)
    "188": "_p3",     # 3rd Period (hockey)
    "1191": "",       # Game (hockey)
    # Basketball (NBA/NCAAB)
    "209": "",        # Game
    "211": "_h1",     # First Half
    "212": "_h2",     # Second Half
    "213": "_q1",     # 1st Quarter
    "214": "_q2",     # 2nd Quarter
    "215": "_q3",     # 3rd Quarter
    "216": "_q4",     # 4th Quarter
    "1195": "",       # Regulation Time → treat as full game
    # Tennis
    "245": "",        # Live Match (tennis)
    "246": "_s1",     # 1st Set (tennis)
    "247": "_s2",     # 2nd Set (tennis)
    # MMA/Boxing
    "12122": "",      # Bout (MMA/Boxing) → treat as full game
}


class BovadaSource(DataSource):
    """Fetches odds from Bovada's public coupon API."""

    # Limit concurrent Bovada requests to avoid 400/429 rate limits
    _api_sem = asyncio.Semaphore(4)

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
            },
        )
        # Cache: canonical_event_id → (bovada_link, event_url)
        self._event_links: Dict[str, Tuple[str, Optional[str]]] = {}

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        if bookmakers and "bovada" not in bookmakers:
            return [], {"x-requests-remaining": "unlimited"}

        sport_path = BOVADA_SPORT_PATHS.get(sport_key)
        if sport_path is None:
            return [], {"x-requests-remaining": "unlimited"}

        try:
            url = f"{BASE_URL}/{sport_path}"
            # Omit marketFilterId to get ALL markets (halves, quarters, props)
            params = {"lang": "en", "eventsLimit": "50"}
            async with self._api_sem:
                response = await self._client.get(url, params=params)
                if response.status_code in (400, 429):
                    # Rate limited — wait and retry once
                    await asyncio.sleep(2.0)
                    response = await self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if not data and sport_key.startswith("soccer"):
                logger.warning(
                    "Bovada: empty response for %s (HTTP %d, %d bytes, url=%s)",
                    sport_key, response.status_code, len(response.content), url,
                )

            events = self._parse(data, sport_key)
            logger.info(f"Bovada: {len(events)} events for {sport_key}")
            return events, {"x-requests-remaining": "unlimited"}

        except Exception as e:
            logger.warning(f"Bovada failed for {sport_key}: {e}")
            return [], {"x-requests-remaining": "unlimited"}

    def _parse(self, data: list, sport_key: str) -> List[OddsEvent]:
        events = []
        sport_title = get_sport_title(sport_key)

        if sport_key.startswith("soccer"):
            total_raw = sum(len(g.get("events", [])) for g in data)
            logger.info("Bovada parse %s: %d groups, %d raw events", sport_key, len(data), total_raw)

        for group in data:
            # MMA/Boxing: only include UFC events (skip OKTAGON, KSW, ONE, RIZIN, etc.)
            if sport_key == "mma_mixed_martial_arts":
                path_entries = group.get("path", [])
                if isinstance(path_entries, list):
                    tour_descs = [
                        p.get("description", "")
                        for p in path_entries
                        if isinstance(p, dict) and p.get("type") == "TOUR"
                    ]
                    if tour_descs and not any("UFC" in d.upper() for d in tour_descs):
                        continue  # Skip non-UFC MMA promotions

            for ev in group.get("events", []):
                competitors = ev.get("competitors", [])
                if len(competitors) < 2:
                    continue

                home = ""
                away = ""
                for comp in competitors:
                    if comp.get("home", False):
                        home = comp.get("name", "")
                    else:
                        away = comp.get("name", "")

                if not home or not away:
                    continue

                # Resolve name variants (e.g. "L.A. Clippers" → "Los Angeles Clippers")
                home = resolve_team_name(home)
                away = resolve_team_name(away)

                # Convert epoch ms to ISO string
                start_ms = ev.get("startTime", 0)
                commence_time = ""
                if start_ms:
                    dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
                    commence_time = dt.isoformat()

                # Parse markets from displayGroups
                bov_markets = []
                seen_keys = set()  # type: set
                for dg in ev.get("displayGroups", []):
                    for mkt in dg.get("markets", []):
                        desc = mkt.get("description", "")
                        outcomes_raw = mkt.get("outcomes", [])

                        # Use period field if available (more reliable than desc parsing)
                        period_info = mkt.get("period", {})
                        period_id = str(period_info.get("id", "")) if isinstance(period_info, dict) else ""
                        suffix = _BOVADA_PERIOD_MAP.get(period_id, "")

                        # If no period field, fall back to description-based detection
                        if not period_id:
                            cleaned_desc, suffix = detect_period_suffix(desc)
                        else:
                            # Strip period prefix from description for base classification
                            cleaned_desc, _dsuffix = detect_period_suffix(desc)

                        # Skip unknown periods (exotic markets)
                        if period_id and period_id not in _BOVADA_PERIOD_MAP:
                            continue

                        # Detect team totals: "Total Points - {Team Name}"
                        team_total_key = None
                        if " - " in desc and "TOTAL" in desc.upper():
                            parts = desc.split(" - ", 1)
                            team_part = parts[1].strip() if len(parts) > 1 else ""
                            if team_part:
                                team_lower = team_part.lower()
                                home_lower = home.lower()
                                away_lower = away.lower()
                                if team_lower == home_lower or home_lower in team_lower or team_lower in home_lower:
                                    team_total_key = "team_total_home"
                                elif team_lower == away_lower or away_lower in team_lower or team_lower in away_lower:
                                    team_total_key = "team_total_away"

                        if team_total_key:
                            tt_key = team_total_key + suffix
                            if tt_key not in seen_keys:
                                seen_keys.add(tt_key)
                                parsed = self._parse_total(outcomes_raw)
                                if parsed:
                                    bov_markets.append(Market(key=tt_key, outcomes=parsed))
                            continue

                        # Skip combo / parlay markets (e.g. "Winner and O/U 125.5 Points",
                        # "Point Spread -4.5 and O/U 131.5 Points") — these are NOT
                        # regular moneyline / spread / total markets.
                        if " and " in desc.lower() or " & " in desc.lower():
                            continue

                        desc_lower = desc.lower()

                        # MMA-specific markets: fight to go the distance
                        if "go the distance" in desc_lower:
                            market_key = "fight_to_go_distance"
                            if market_key not in seen_keys:
                                seen_keys.add(market_key)
                                parsed = self._parse_yes_no(outcomes_raw)
                                if parsed:
                                    bov_markets.append(Market(key=market_key, outcomes=parsed))
                            continue

                        # Soccer-specific markets: detect before general classification
                        if "both teams to score" in desc_lower or "btts" in desc_lower:
                            market_key = "btts"
                            if market_key not in seen_keys:
                                seen_keys.add(market_key)
                                parsed = self._parse_yes_no(outcomes_raw)
                                if parsed:
                                    bov_markets.append(Market(key=market_key, outcomes=parsed))
                            continue

                        if "draw no bet" in desc_lower:
                            market_key = "draw_no_bet" + suffix
                            if market_key not in seen_keys:
                                seen_keys.add(market_key)
                                parsed = self._parse_moneyline(outcomes_raw, home, away)
                                if parsed:
                                    bov_markets.append(Market(key=market_key, outcomes=parsed))
                            continue

                        if "double chance" in desc_lower:
                            market_key = "double_chance"
                            if market_key not in seen_keys:
                                seen_keys.add(market_key)
                                parsed = self._parse_moneyline(outcomes_raw, home, away)
                                if parsed:
                                    bov_markets.append(Market(key=market_key, outcomes=parsed))
                            continue

                        # Classify the base market type
                        base = None
                        cleaned_upper = cleaned_desc.upper().replace(" ", "_")
                        if cleaned_desc in ("Moneyline",) or "MONEYLINE" in cleaned_upper or "MONEY_LINE" in cleaned_upper:
                            # Check if this is a 3-way moneyline (soccer: has 3+ outcomes including Draw/Tie)
                            is_3way = len(outcomes_raw) >= 3 and any(
                                "draw" in o.get("description", "").lower() or "tie" in o.get("description", "").lower()
                                for o in outcomes_raw
                            )
                            base = "h2h_3way" if is_3way else "h2h"
                        elif "MATCH_RESULT" in cleaned_upper or "MATCH_WINNER" in cleaned_upper:
                            is_3way = len(outcomes_raw) >= 3 and any(
                                "draw" in o.get("description", "").lower() or "tie" in o.get("description", "").lower()
                                for o in outcomes_raw
                            )
                            base = "h2h_3way" if is_3way else "h2h"
                        elif cleaned_desc in ("Point Spread",) or "SPREAD" in cleaned_upper or "HANDICAP" in cleaned_upper:
                            # Skip alternate/alt lines
                            if "ALTERNATE" in cleaned_upper or "ALT_" in cleaned_upper:
                                continue
                            base = "spreads"
                        elif cleaned_desc in ("Total",) or "TOTAL" in cleaned_upper or "OVER_UNDER" in cleaned_upper:
                            # Skip alternate totals
                            if "ALTERNATE" in cleaned_upper or "ALT_" in cleaned_upper:
                                continue
                            base = "totals"

                        if base is None:
                            # Try generic classification
                            base = classify_base_market(cleaned_desc)

                        if base is None:
                            continue

                        market_key = base + suffix
                        if market_key in seen_keys:
                            continue
                        seen_keys.add(market_key)

                        if base in ("h2h", "h2h_3way"):
                            parsed = self._parse_moneyline(outcomes_raw, home, away)
                            if parsed:
                                bov_markets.append(Market(key=market_key, outcomes=parsed))
                        elif base == "spreads":
                            parsed = self._parse_spread(outcomes_raw, home, away)
                            if parsed:
                                bov_markets.append(Market(key=market_key, outcomes=parsed))
                        elif base == "totals":
                            parsed = self._parse_total(outcomes_raw)
                            if parsed:
                                bov_markets.append(Market(key=market_key, outcomes=parsed))

                if not bov_markets:
                    continue

                # Build event deep-link URL from Bovada's link field
                event_url = None
                link = ev.get("link")
                if link:
                    event_url = f"https://www.bovada.lv{link}"

                cid = canonical_event_id(sport_key, home, away, commence_time)
                # Cache link for player props lookup
                if link:
                    self._event_links[cid] = (link, event_url)
                events.append(OddsEvent(
                    id=cid,
                    sport_key=sport_key,
                    sport_title=sport_title,
                    commence_time=commence_time,
                    home_team=home,
                    away_team=away,
                    bookmakers=[
                        Bookmaker(key="bovada", title="Bovada", markets=bov_markets, event_url=event_url)
                    ],
                ))

        return events

    @staticmethod
    def _parse_american(raw) -> Optional[int]:
        """Convert Bovada american odds to int. Handles 'EVEN' → 100."""
        if raw is None:
            return None
        s = str(raw).strip().upper()
        if s == "EVEN":
            return 100
        try:
            return int(s.replace("+", ""))
        except (ValueError, TypeError):
            return None

    def _parse_moneyline(self, outcomes: list, home: str, away: str) -> List[Outcome]:
        result = []
        for o in outcomes:
            price = o.get("price", {})
            odds = self._parse_american(price.get("american"))
            if odds is None:
                continue
            name = o.get("description", "")
            result.append(Outcome(name=name, price=odds))
        return result if len(result) >= 2 else []

    def _parse_spread(self, outcomes: list, home: str, away: str) -> List[Outcome]:
        result = []
        for o in outcomes:
            price = o.get("price", {})
            odds = self._parse_american(price.get("american"))
            handicap = price.get("handicap")
            if odds is None or handicap is None:
                continue
            try:
                point = float(handicap)
            except (ValueError, TypeError):
                continue
            name = o.get("description", "")
            result.append(Outcome(name=name, price=odds, point=point))
        return result if len(result) >= 2 else []

    def _parse_yes_no(self, outcomes: list) -> List[Outcome]:
        """Parse a Yes/No market (e.g., Both Teams to Score)."""
        result = []
        for o in outcomes:
            price_data = o.get("price", {})
            odds = self._parse_american(price_data.get("american"))
            if odds is None:
                continue
            desc = o.get("description", "")
            if "yes" in desc.lower():
                name = "Yes"
            elif "no" in desc.lower():
                name = "No"
            else:
                name = desc
            result.append(Outcome(name=name, price=odds))
        return result if len(result) >= 2 else []

    def _parse_total(self, outcomes: list) -> List[Outcome]:
        result = []
        for o in outcomes:
            price = o.get("price", {})
            odds = self._parse_american(price.get("american"))
            handicap = price.get("handicap")
            if odds is None or handicap is None:
                continue
            try:
                point = float(handicap)
            except (ValueError, TypeError):
                continue
            desc = o.get("description", "")
            name = "Over" if "over" in desc.lower() else "Under" if "under" in desc.lower() else desc
            result.append(Outcome(name=name, price=odds, point=point))
        return result if len(result) >= 2 else []

    # Bovada O/U player prop description patterns: "Total Points - Player Name (TEAM)"
    # Team suffix is optional (some players listed without team abbreviation)
    _OU_PROP_PATTERNS = [
        (re.compile(r"^Total Points - (.+?)(?:\s*\(\w+\))?$"), "points"),
        (re.compile(r"^Total Rebounds - (.+?)(?:\s*\(\w+\))?$"), "rebounds"),
        (re.compile(r"^Total Assists - (.+?)(?:\s*\(\w+\))?$"), "assists"),
        (re.compile(r"^Total Made 3 Points? Shots? - (.+?)(?:\s*\(\w+\))?$"), "threes"),
        (re.compile(r"^Total Points,? Rebounds and Assists - (.+?)(?:\s*\(\w+\))?$"), "pts_reb_ast"),
        (re.compile(r"^Total Points and Rebounds - (.+?)(?:\s*\(\w+\))?$"), "pts_reb"),
        (re.compile(r"^Total Points and Assists - (.+?)(?:\s*\(\w+\))?$"), "pts_ast"),
        (re.compile(r"^Total Rebounds and Assists - (.+?)(?:\s*\(\w+\))?$"), "reb_ast"),
        (re.compile(r"^Total Steals - (.+?)(?:\s*\(\w+\))?$"), "steals"),
        (re.compile(r"^Total Blocks - (.+?)(?:\s*\(\w+\))?$"), "blocks"),
    ]

    # Bovada milestone description → stat_type (legacy fallback)
    _MILESTONE_PATTERNS = [
        (re.compile(r"^Points Milestones - (.+?)(?:\s*\(\w+\))?$"), "points"),
        (re.compile(r"^Rebounds Milestones - (.+?)(?:\s*\(\w+\))?$"), "rebounds"),
        (re.compile(r"^Assists Milestones - (.+?)(?:\s*\(\w+\))?$"), "assists"),
        (re.compile(r"^Total Made Threes Milestones - (.+?)(?:\s*\(\w+\))?$"), "threes"),
    ]
    # Outcome description → threshold
    _THRESHOLD_RE = re.compile(r"(\d+)\+")

    async def get_player_props(self, sport_key: str, event_id: str) -> List[PlayerProp]:
        """Fetch player props from Bovada's single-event endpoint."""
        link_data = self._event_links.get(event_id)
        if not link_data:
            return []

        bovada_link, event_url = link_data

        try:
            url = f"{BASE_URL}{bovada_link}"
            params = {"lang": "en"}
            response = await self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.warning(f"Bovada player props failed for {event_id}: {e}")
            return []

        # Collect team names to exclude team totals from player props
        team_names = set()  # type: set
        for group in data:
            for ev in group.get("events", []):
                for comp in ev.get("competitors", []):
                    name = comp.get("name", "")
                    if name:
                        team_names.add(name.lower())

        props: List[PlayerProp] = []
        for group in data:
            for ev in group.get("events", []):
                for dg in ev.get("displayGroups", []):
                    for mkt in dg.get("markets", []):
                        desc = mkt.get("description", "")
                        outcomes = mkt.get("outcomes", [])

                        # Try O/U prop first (may have multiple lines per player)
                        ou_stat, ou_player = self._match_ou_prop(desc)
                        # Skip team totals (e.g. "Total Points - Indiana Pacers")
                        if ou_stat and ou_player.lower() in team_names:
                            continue
                        if ou_stat and len(outcomes) >= 2:
                            for outcome in outcomes:
                                out_desc = outcome.get("description", "")
                                price_data = outcome.get("price", {})
                                handicap = price_data.get("handicap")
                                price = self._parse_american(price_data.get("american"))
                                if handicap is None or price is None:
                                    continue
                                try:
                                    line = float(handicap)
                                except (ValueError, TypeError):
                                    continue
                                if "over" in out_desc.lower():
                                    ou_desc = "Over"
                                elif "under" in out_desc.lower():
                                    ou_desc = "Under"
                                else:
                                    continue
                                props.append(PlayerProp(
                                    player_name=ou_player,
                                    stat_type=ou_stat,
                                    line=line,
                                    price=price,
                                    description=ou_desc,
                                    bookmaker_key="bovada",
                                    bookmaker_title="Bovada",
                                    event_url=event_url,
                                ))
                            continue

                        # Fallback: milestone-style props
                        stat_type, player_name = self._match_milestone(desc)
                        if not stat_type:
                            continue
                        for outcome in outcomes:
                            out_desc = outcome.get("description", "")
                            m = self._THRESHOLD_RE.search(out_desc)
                            if not m:
                                continue
                            threshold = float(m.group(1))
                            price_data = outcome.get("price", {})
                            price = self._parse_american(price_data.get("american"))
                            if price is None:
                                continue
                            props.append(PlayerProp(
                                player_name=player_name,
                                stat_type=stat_type,
                                line=threshold,
                                price=price,
                                bookmaker_key="bovada",
                                bookmaker_title="Bovada",
                                event_url=event_url,
                            ))

        return props

    def _match_ou_prop(self, description: str) -> Tuple[Optional[str], str]:
        """Match a Bovada O/U player prop description to (stat_type, player_name)."""
        for pattern, stat_type in self._OU_PROP_PATTERNS:
            m = pattern.match(description)
            if m:
                return stat_type, m.group(1).strip()
        return None, ""

    def _match_milestone(self, description: str) -> Tuple[Optional[str], str]:
        """Match a Bovada milestone market description to (stat_type, player_name)."""
        for pattern, stat_type in self._MILESTONE_PATTERNS:
            m = pattern.match(description)
            if m:
                return stat_type, m.group(1).strip()
        return None, ""

    async def close(self) -> None:
        await self._client.aclose()
