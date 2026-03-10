"""
BetRivers sportsbook scraper.
Uses the Kambi API (which powers BetRivers/Rush Street Interactive).
No authentication required.
"""

import asyncio
import logging
import re
import time
from typing import Dict, List, Optional, Tuple

import httpx

from models import Bookmaker, Market, OddsEvent, Outcome, PlayerProp
from sources.base import DataSource
from market_keys import detect_period_suffix, classify_base_market
from sources.sport_mapping import (
    KAMBI_SPORT_PATHS,
    canonical_event_id,
    decimal_to_american,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

# Kambi API base (rsiuspa = Rush Street Interactive US PA)
BASE_URL = "https://eu-offering-api.kambicdn.com/offering/v2018/rsiuspa"

# Minimum seconds between Kambi API requests (across all sports).
# Kambi CDN allows ~2-3 req/s sustained; 0.4s gives us 2.5 req/s with margin.
_MIN_REQUEST_INTERVAL = 0.4

# Maximum retries for 429 responses
_MAX_RETRIES = 3

# Base backoff seconds for 429 retries (doubles each attempt)
_RETRY_BACKOFF_BASE = 3.0


class BetRiversSource(DataSource):
    """Fetches odds from BetRivers via the Kambi API."""

    # Kambi criterion labels → stat_type for player O/U props
    # Labels vary by event: some use "player X over/under", others "X by the player"
    _PLAYER_PROP_LABELS = {
        # Format: "player X over/under" (older Kambi events)
        "player points over/under": "points",
        "player rebounds over/under": "rebounds",
        "player assists over/under": "assists",
        "player three pointers made over/under": "threes",
        "player points + rebounds + assists over/under": "pts_reb_ast",
        "player points + rebounds over/under": "pts_reb",
        "player points + assists over/under": "pts_ast",
        "player steals over/under": "steals",
        "player blocks over/under": "blocks",
        # Format: "X by the player" (newer Kambi events)
        "points scored by the player": "points",
        "rebounds by the player": "rebounds",
        "assists by the player": "assists",
        "3-point field goals made by the player": "threes",
        "points, rebounds & assists by the player": "pts_reb_ast",
        "points & rebounds by the player": "pts_reb",
        "points & assists by the player": "pts_ast",
        "rebounds & assists by the player": "reb_ast",
        "steals by the player": "steals",
        "blocks by the player": "blocks",
        "steals & blocks by the player": "stl_blk",
    }

    def __init__(self):
        self._client = httpx.AsyncClient(
            timeout=25.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
            },
        )
        # Cache: canonical_event_id → (kambi_event_id, event_url)
        self._event_ids: Dict[str, Tuple[int, Optional[str]]] = {}
        # Global request throttle: serialize all Kambi requests to avoid 429s
        self._request_lock = asyncio.Lock()
        self._last_request_time: float = 0.0

    async def _throttled_get(self, url: str, params: dict) -> httpx.Response:
        """Make a GET request with global rate-limiting and 429 retry logic.

        Ensures at least _MIN_REQUEST_INTERVAL seconds between requests and
        retries with exponential backoff on 429 responses.
        """
        for attempt in range(_MAX_RETRIES + 1):
            async with self._request_lock:
                # Enforce minimum interval between requests
                now = time.monotonic()
                elapsed = now - self._last_request_time
                if elapsed < _MIN_REQUEST_INTERVAL:
                    await asyncio.sleep(_MIN_REQUEST_INTERVAL - elapsed)
                self._last_request_time = time.monotonic()

            response = await self._client.get(url, params=params)

            if response.status_code == 429:
                if attempt < _MAX_RETRIES:
                    backoff = _RETRY_BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        "BetRivers 429 rate-limited on %s (attempt %d/%d), "
                        "retrying in %.1fs",
                        url.split("/")[-1], attempt + 1, _MAX_RETRIES, backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                else:
                    logger.warning(
                        "BetRivers 429 rate-limited on %s — exhausted %d retries",
                        url.split("/")[-1], _MAX_RETRIES,
                    )
            return response

        return response  # Return last response (will be 429)

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        if bookmakers and "betrivers" not in bookmakers:
            return [], {"x-requests-remaining": "unlimited"}

        sport_path = KAMBI_SPORT_PATHS.get(sport_key)
        if sport_path is None:
            return [], {"x-requests-remaining": "unlimited"}

        try:
            # Step 1: Get event list from listView
            list_url = f"{BASE_URL}/listView/{sport_path}.json"
            params = {"lang": "en_US", "market": "US"}
            response = await self._throttled_get(list_url, params)
            response.raise_for_status()
            data = response.json()

            kambi_events = data.get("events", [])
            if not kambi_events:
                return [], {"x-requests-remaining": "unlimited"}

            # Step 2: Fetch full bet offers per event with bounded concurrency.
            # We use a semaphore to allow up to 4 concurrent requests while
            # the throttled_get still enforces the minimum interval between
            # requests. This is ~4x faster than fully sequential.
            sem = asyncio.Semaphore(4)
            sport_title = get_sport_title(sport_key)

            async def _fetch_and_parse(ev):
                async with sem:
                    result = await self._fetch_event_offers(ev)
                    if result is not None:
                        return self._parse_event(result, sport_key, sport_title)
                    return None

            parsed_results = await asyncio.gather(
                *[_fetch_and_parse(ev) for ev in kambi_events]
            )
            events = [e for e in parsed_results if e is not None]

            logger.info(f"BetRivers: {len(events)} events for {sport_key}")
            return events, {"x-requests-remaining": "unlimited"}

        except Exception as e:
            logger.warning(f"BetRivers failed for {sport_key}: {type(e).__name__}: {e}")
            return [], {"x-requests-remaining": "unlimited"}

    async def _fetch_event_offers(self, kambi_event: dict) -> Optional[dict]:
        """Fetch full bet offers for a single event with throttling."""
        event_info = kambi_event.get("event", {})
        event_id = event_info.get("id")
        if not event_id:
            return None

        url = f"{BASE_URL}/betoffer/event/{event_id}.json"
        params = {"lang": "en_US", "market": "US"}
        try:
            response = await self._throttled_get(url, params)
            if response.status_code != 200:
                return None
            data = response.json()
            return {"event": event_info, "betOffers": data.get("betOffers", [])}
        except Exception:
            return None

    def _parse_event(self, data: dict, sport_key: str, sport_title: str) -> Optional[OddsEvent]:
        event_info = data["event"]
        offers = data["betOffers"]

        # Parse team names from event name: "Team A @ Team B" or "Team A - Team B"
        name = event_info.get("name", "")
        home = event_info.get("homeName", "")
        away = event_info.get("awayName", "")

        if not home or not away:
            if " @ " in name:
                parts = name.split(" @ ")
                away = parts[0].strip()
                home = parts[1].strip()
            elif " - " in name:
                parts = name.split(" - ")
                home = parts[0].strip()
                away = parts[1].strip()

        if not home or not away:
            return None

        # Resolve Kambi abbreviated names (e.g. "OKC Thunder" → "Oklahoma City Thunder")
        home = resolve_team_name(home, sport_key=sport_key)
        away = resolve_team_name(away, sport_key=sport_key)

        commence_time = event_info.get("start", "")

        # Parse bet offers into markets (with period detection)
        br_markets = []
        seen_keys = set()  # type: set
        for offer in offers:
            label = offer.get("criterion", {}).get("label", "")
            outcomes_raw = offer.get("outcomes", [])

            # Detect period suffix from criterion label
            cleaned_label, suffix = detect_period_suffix(label)
            # Strip "- Inc. OT and Shootout" suffix (hockey)
            # Note: don't strip "Regular Time" when it's the full label (soccer 3-way ML)
            cleaned_label = re.sub(
                r"\s*-?\s*Inc\.?\s*OT\s*(?:and|&)\s*Shootout\s*$",
                "", cleaned_label, flags=re.IGNORECASE,
            ).strip()
            # Strip " - Regular Time" suffix only when preceded by something
            if cleaned_label.lower() != "regular time":
                cleaned_label = re.sub(
                    r"\s*-\s*Regular\s*Time\s*$",
                    "", cleaned_label, flags=re.IGNORECASE,
                ).strip()

            # Classify the base market type
            base = None
            team_total_side = ""
            label_lower = cleaned_label.lower()

            # MMA-specific markets
            orig_label_lower = label.lower()
            if "go the distance" in label_lower or "go the distance" in orig_label_lower:
                base = "fight_to_go_distance"
            elif "total rounds" in label_lower or "total rounds" in orig_label_lower:
                base = "total_rounds"
            # Soccer-specific markets: check first before general classification
            # Use the original label for detection since detect_period_suffix
            # may strip some Kambi soccer labels to empty string
            elif "both teams to score" in label_lower or "both teams to score" in orig_label_lower:
                base = "btts"
            elif "double chance" in label_lower or "double chance" in orig_label_lower:
                base = "double_chance"
            elif "draw no bet" in label_lower or "tie no bet" in label_lower \
                    or "draw no bet" in orig_label_lower or "tie no bet" in orig_label_lower:
                base = "draw_no_bet"
            elif cleaned_label == "Moneyline" or "moneyline" in label_lower:
                base = "h2h"
            elif ("full time" in label_lower or "match result" in label_lower
                  or "1x2" in label_lower or cleaned_label == "Regular Time"):
                # Soccer 3-way moneyline (US: "Regular Time", GB: "Full Time")
                base = "h2h_3way"
            elif cleaned_label == "Half Time" or orig_label_lower == "half time":
                # Kambi 1st half 3-way (not a period suffix, it's the market name)
                base = "h2h_3way"
                suffix = "_h1"
            elif orig_label_lower == "2nd half" and cleaned_label == "" and suffix == "_h2":
                # Kambi 2nd half 3-way: period detection strips to empty, restore base
                base = "h2h_3way"
            elif (cleaned_label == "Point Spread"
                  or "spread" in label_lower
                  or "puck line" in label_lower
                  or "run line" in label_lower
                  or "handicap" in label_lower):
                # Skip 2-way handicap labels that are really draw no bet
                if "2-way" in label_lower or "2 way" in label_lower:
                    continue
                # Skip alternate/alt lines — these are not mainline spreads
                if "alternate" in label_lower or "alt " in label_lower:
                    continue
                base = "spreads"
            elif "total" in label_lower:
                # Skip alternate total lines
                if "alternate" in label_lower or "alt " in label_lower:
                    continue
                # Check if this is a team total
                home_lower = home.lower()
                away_lower = away.lower()
                # Kambi team totals: "Home Team Total Points" or "Away Team Total"
                is_team_total = False
                for word in home_lower.split():
                    if len(word) > 2 and word in label_lower:
                        is_team_total = True
                        team_total_side = "home"
                        break
                if not is_team_total:
                    for word in away_lower.split():
                        if len(word) > 2 and word in label_lower:
                            is_team_total = True
                            team_total_side = "away"
                            break
                if not is_team_total and "home" in label_lower:
                    is_team_total = True
                    team_total_side = "home"
                elif not is_team_total and "away" in label_lower:
                    is_team_total = True
                    team_total_side = "away"

                if is_team_total:
                    base = "team_total_" + team_total_side
                else:
                    base = "totals"

            if base is None:
                base = classify_base_market(cleaned_label)

            if base is None:
                continue

            market_key = base + suffix
            if market_key in seen_keys:
                continue
            seen_keys.add(market_key)

            if base == "h2h":
                parsed = self._parse_moneyline(outcomes_raw)
                if parsed:
                    br_markets.append(Market(key=market_key, outcomes=parsed))
            elif base == "h2h_3way":
                # 3-way result: home/draw/away (Kambi uses 1/X/2 labels)
                parsed = self._parse_moneyline_3way(outcomes_raw, home, away)
                if parsed and len(parsed) >= 3:
                    br_markets.append(Market(key=market_key, outcomes=parsed))
            elif base == "draw_no_bet":
                # Draw No Bet / Tie No Bet: 2-way (home/away)
                # Kambi uses 1/2 labels with OT_ONE/OT_TWO types
                parsed = self._parse_two_way_1x2(outcomes_raw, home, away)
                if parsed:
                    br_markets.append(Market(key=market_key, outcomes=parsed))
            elif base == "btts":
                # Both Teams to Score: Yes/No
                parsed = self._parse_yes_no(outcomes_raw)
                if parsed:
                    br_markets.append(Market(key=market_key, outcomes=parsed))
            elif base == "fight_to_go_distance":
                # MMA: Will the fight go the distance? Yes/No
                parsed = self._parse_yes_no(outcomes_raw)
                if parsed:
                    br_markets.append(Market(key=market_key, outcomes=parsed))
            elif base == "total_rounds":
                # MMA: Total rounds (Over/Under)
                parsed = self._parse_total(outcomes_raw)
                if parsed:
                    br_markets.append(Market(key=market_key, outcomes=parsed))
            elif base == "double_chance":
                # Double Chance: 3 outcomes (1X = Home/Draw, 12 = Home/Away, X2 = Draw/Away)
                parsed = self._parse_double_chance(outcomes_raw, home, away)
                if parsed:
                    br_markets.append(Market(key=market_key, outcomes=parsed))
            elif base == "spreads":
                parsed = self._parse_spread(outcomes_raw)
                if parsed:
                    # Skip draw-no-bet lines disguised as spreads (±0.5 points)
                    if any(abs(o.point) == 0.5 for o in parsed if o.point is not None):
                        continue
                    br_markets.append(Market(key=market_key, outcomes=parsed))
            elif base.startswith("team_total_"):
                parsed = self._parse_total(outcomes_raw)
                if parsed:
                    br_markets.append(Market(key=market_key, outcomes=parsed))
            elif base == "totals":
                parsed = self._parse_total(outcomes_raw)
                if parsed:
                    br_markets.append(Market(key=market_key, outcomes=parsed))

        if not br_markets:
            return None

        # Build event deep-link URL from Kambi event ID
        kambi_id = event_info.get("id")
        event_url = f"https://www.betrivers.com/sports/event/{kambi_id}" if kambi_id else None

        cid = canonical_event_id(sport_key, home, away, commence_time)
        # Cache for player props lookup
        if kambi_id:
            self._event_ids[cid] = (kambi_id, event_url)
        return OddsEvent(
            id=cid,
            sport_key=sport_key,
            sport_title=sport_title,
            commence_time=commence_time,
            home_team=home,
            away_team=away,
            bookmakers=[
                Bookmaker(key="betrivers", title="BetRivers", markets=br_markets, event_url=event_url)
            ],
        )

    def _milliodds_to_american(self, milliodds: int) -> int:
        """Convert Kambi milliodds (e.g. 1910 = 1.91 decimal) to American."""
        decimal_odds = milliodds / 1000.0
        return decimal_to_american(decimal_odds)

    def _parse_moneyline(self, outcomes: list) -> List[Outcome]:
        result = []
        for o in outcomes:
            milliodds = o.get("odds")
            if milliodds is None:
                continue
            name = resolve_team_name(o.get("label", ""))
            price = self._milliodds_to_american(milliodds)
            result.append(Outcome(name=name, price=price))
        return result if len(result) >= 2 else []

    def _parse_spread(self, outcomes: list) -> List[Outcome]:
        result = []
        for o in outcomes:
            milliodds = o.get("odds")
            line = o.get("line")
            if milliodds is None or line is None:
                continue
            name = resolve_team_name(o.get("label", ""))
            price = self._milliodds_to_american(milliodds)
            point = line / 1000.0  # Kambi uses milliunits
            result.append(Outcome(name=name, price=price, point=point))
        return result if len(result) >= 2 else []

    def _parse_yes_no(self, outcomes: list) -> List[Outcome]:
        """Parse a Yes/No market (e.g., BTTS)."""
        result = []
        for o in outcomes:
            milliodds = o.get("odds")
            if milliodds is None:
                continue
            label = o.get("label", "")
            o_type = o.get("type", "")
            if o_type == "OT_YES" or "yes" in label.lower():
                name = "Yes"
            elif o_type == "OT_NO" or "no" in label.lower():
                name = "No"
            else:
                name = label
            price = self._milliodds_to_american(milliodds)
            result.append(Outcome(name=name, price=price))
        return result if len(result) >= 2 else []

    def _parse_two_way_1x2(self, outcomes: list, home: str, away: str) -> List[Outcome]:
        """Parse 2-way market with Kambi 1/2 labels (e.g., Tie No Bet)."""
        result = []
        for o in outcomes:
            milliodds = o.get("odds")
            if milliodds is None:
                continue
            o_type = o.get("type", "")
            label = o.get("label", "")
            if o_type == "OT_ONE" or label == "1":
                name = home
            elif o_type == "OT_TWO" or label == "2":
                name = away
            else:
                name = resolve_team_name(label)
            price = self._milliodds_to_american(milliodds)
            result.append(Outcome(name=name, price=price))
        return result if len(result) >= 2 else []

    def _parse_moneyline_3way(self, outcomes: list, home: str, away: str) -> List[Outcome]:
        """Parse 3-way moneyline (Kambi uses 1/X/2 labels with OT_ONE/OT_CROSS/OT_TWO types)."""
        result = []
        for o in outcomes:
            milliodds = o.get("odds")
            if milliodds is None:
                continue
            o_type = o.get("type", "")
            label = o.get("label", "")
            if o_type == "OT_ONE" or label == "1":
                name = home
            elif o_type == "OT_CROSS" or label.upper() == "X":
                name = "Draw"
            elif o_type == "OT_TWO" or label == "2":
                name = away
            else:
                name = resolve_team_name(label)
            price = self._milliodds_to_american(milliodds)
            result.append(Outcome(name=name, price=price))
        return result if len(result) >= 3 else []

    def _parse_double_chance(self, outcomes: list, home: str, away: str) -> List[Outcome]:
        """Parse double chance market (1X, 12, X2) with human-readable names."""
        # Kambi double chance labels: "1X" = Home or Draw, "12" = Home or Away, "X2" = Draw or Away
        _DC_MAP = {
            "1x": f"{home} or Draw",
            "12": f"{home} or {away}",
            "x2": f"Draw or {away}",
        }
        result = []
        for o in outcomes:
            milliodds = o.get("odds")
            if milliodds is None:
                continue
            raw_label = o.get("label", "").strip().lower()
            name = _DC_MAP.get(raw_label, o.get("label", raw_label))
            price = self._milliodds_to_american(milliodds)
            result.append(Outcome(name=name, price=price))
        return result if len(result) >= 2 else []

    def _parse_total(self, outcomes: list) -> List[Outcome]:
        result = []
        for o in outcomes:
            milliodds = o.get("odds")
            line = o.get("line")
            if milliodds is None or line is None:
                continue
            label = o.get("label", "")
            name = "Over" if "over" in label.lower() else "Under" if "under" in label.lower() else label
            price = self._milliodds_to_american(milliodds)
            point = line / 1000.0
            result.append(Outcome(name=name, price=price, point=point))
        return result if len(result) >= 2 else []

    async def get_player_props(self, sport_key: str, event_id: str) -> List[PlayerProp]:
        """Fetch player props from Kambi's event detail endpoint."""
        cached = self._event_ids.get(event_id)
        if not cached:
            return []

        kambi_id, event_url = cached

        try:
            url = f"{BASE_URL}/betoffer/event/{kambi_id}.json"
            params = {"lang": "en_US", "market": "US"}
            response = await self._throttled_get(url, params)
            if response.status_code != 200:
                return []
            data = response.json()
        except Exception as e:
            logger.warning(f"BetRivers player props failed for {event_id}: {e}")
            return []

        props: List[PlayerProp] = []
        for offer in data.get("betOffers", []):
            offer_type_id = offer.get("betOfferType", {}).get("id")
            if offer_type_id != 127:  # Player Occurrence Line
                continue

            label = offer.get("criterion", {}).get("label", "").lower()
            stat_type = self._PLAYER_PROP_LABELS.get(label)
            if not stat_type:
                continue

            outcomes = offer.get("outcomes", [])
            # Capture both Over and Under outcomes
            for o in outcomes:
                o_type = o.get("type", "")
                if o_type == "OT_OVER":
                    ou_desc = "Over"
                elif o_type == "OT_UNDER":
                    ou_desc = "Under"
                elif o_type == "OT_YES":
                    ou_desc = "Over"
                elif o_type == "OT_NO":
                    ou_desc = "Under"
                else:
                    continue
                player = o.get("participant", "")
                milliodds = o.get("odds")
                line = o.get("line")
                if not player or milliodds is None or line is None:
                    continue

                price = self._milliodds_to_american(milliodds)
                threshold = line / 1000.0

                props.append(PlayerProp(
                    player_name=player,
                    stat_type=stat_type,
                    line=threshold,
                    price=price,
                    description=ou_desc,
                    bookmaker_key="betrivers",
                    bookmaker_title="BetRivers",
                    event_url=event_url,
                ))

        return props

    async def close(self) -> None:
        await self._client.aclose()
