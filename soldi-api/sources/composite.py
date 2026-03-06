import asyncio
import logging
import math
import re
import statistics
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from market_keys import reclassify_tennis_totals
from models import Bookmaker, Market, OddsEvent, Outcome, PlayerProp
from sources.base import DataSource
from sources.sport_mapping import (
    normalize_team_name, resolve_team_name, _truncate_fractional_seconds,
)

logger = logging.getLogger(__name__)

# Sports that use halves (not quarters) — filter out quarter-period markets
HALVES_ONLY_SPORTS = frozenset({"basketball_ncaab", "basketball_wnba", "basketball_euroleague"})

# Market key suffixes that are invalid for halves-only sports
_INVALID_PERIOD_SUFFIXES = ("_q1", "_q2", "_q3", "_q4", "_i1")


def _reclassify_tennis_totals(events: List[OddsEvent]) -> None:
    """Split tennis 'totals' markets into 'totals_sets' and 'totals_games'.

    Tennis has two distinct over/under markets:
    - Total sets (O/U 2.5) - whether the match goes to 3 sets or ends in 2
    - Total games (O/U 22.5) - total games played across all sets

    Uses heuristic: point <= 5.5 → total sets; point > 5.5 → total games.
    Modifies events in-place.
    """
    for event in events:
        for bm in event.bookmakers:
            new_markets = []  # type: List[Market]
            for mkt in bm.markets:
                if mkt.key == "totals" and mkt.outcomes:
                    # Get the point value from outcomes
                    point = None
                    for o in mkt.outcomes:
                        if o.point is not None:
                            point = o.point
                            break
                    new_key = reclassify_tennis_totals(mkt.key, point)
                    new_markets.append(
                        Market(key=new_key, outcomes=mkt.outcomes)
                    )
                else:
                    new_markets.append(mkt)
            bm.markets = new_markets


def _filter_invalid_period_markets(
    events: List[OddsEvent], sport_key: str
) -> List[OddsEvent]:
    """Remove quarter/inning markets from sports that only have halves.

    For NCAAB, WNBA, and Euroleague basketball, bookmakers sometimes include
    quarter markets (e.g. spreads_q1, totals_q3) that don't exist in those
    sports (they play halves). This strips those invalid markets out.
    """
    if sport_key not in HALVES_ONLY_SPORTS:
        return events

    for event in events:
        for bookmaker in event.bookmakers:
            bookmaker.markets = [
                m for m in bookmaker.markets
                if not m.key.endswith(_INVALID_PERIOD_SUFFIXES)
            ]

    return events


# Keywords in team names that indicate a futures/championship market
_FUTURES_KEYWORDS = frozenset([
    "wins", "champion", "coin toss", "futures", "conference team",
    "division", "mvp", "award", "super bowl", "world series",
    "stanley cup", "nba finals", "pennant", "cy young", "heisman",
])


def _is_womens_event(event: OddsEvent) -> bool:
    """Return True if the event looks like a women's basketball game.

    Some sources (Hard Rock, DraftKings, etc.) mix women's games into the
    men's college basketball feed.  These are identifiable by "(W)" in the
    team name, or "Women" / "Wmns" tags.
    """
    combined = event.home_team + " " + event.away_team
    if "(W)" in combined:
        return True
    lower = combined.lower()
    if "women" in lower or "(wmns)" in lower or "wncaa" in lower:
        return True
    return False


def _is_prop_or_summary_event(event: OddsEvent) -> bool:
    """Return True if the event is a prop market (Corners, Bookings) or
    an aggregated summary row (e.g. 'Away Teams (5 Games) @ Home Teams (5 Games)').
    """
    combined = event.home_team + " " + event.away_team
    # Prop-market events have suffixes like "(Corners)", "(Bookings)"
    if "(corners)" in combined.lower() or "(bookings)" in combined.lower():
        return True
    # Aggregated summary rows from some scrapers
    if "away teams" in combined.lower() and "home teams" in combined.lower():
        return True
    return False


def _is_futures_event(event: OddsEvent) -> bool:
    """Return True if the event looks like a futures/championship market."""
    combined = (event.home_team + " " + event.away_team).lower()
    return any(kw in combined for kw in _FUTURES_KEYWORDS)


def _is_stale_event(event: OddsEvent, max_age_hours: int = 12) -> bool:
    """Return True if the event's commence_time is more than max_age_hours in the past."""
    try:
        ct = event.commence_time
        if ct.endswith("Z"):
            dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
        elif "+" in ct[10:] or ct.count("-") > 2:
            dt = datetime.fromisoformat(ct)
        else:
            dt = datetime.fromisoformat(ct)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        return dt < cutoff
    except Exception:
        return False


def _normalize_display_names(events: List[OddsEvent]) -> None:
    """Resolve team name aliases on display names for cleaner UI.

    Sources may pass unresolved names (abbreviations, mascot suffixes, etc.)
    through to OddsEvent.home_team / away_team.  This normalises them
    using the central alias table so the dashboard shows clean names.

    Also normalizes outcome names within all bookmaker h2h markets so they
    match the event-level home/away team names (different sources may use
    different name forms for the same team, e.g. "LA Clippers" vs
    "Los Angeles Clippers").
    """
    for event in events:
        sport_key = event.sport_key if hasattr(event, "sport_key") else ""
        if not sport_key and ":" in event.id:
            sport_key = event.id.split(":")[0]
        event.home_team = resolve_team_name(event.home_team, sport_key=sport_key)
        event.away_team = resolve_team_name(event.away_team, sport_key=sport_key)

        # Normalize outcome names in h2h markets to match event-level team names.
        # Uses exact normalized match first, then substring match as fallback
        # (handles cases like "New Orleans" vs "New Orleans Pelicans").
        home_norm = normalize_team_name(event.home_team)
        away_norm = normalize_team_name(event.away_team)
        for bm in event.bookmakers:
            for mkt in bm.markets:
                if not mkt.key.startswith("h2h"):
                    continue
                for outcome in mkt.outcomes:
                    if outcome.name == event.home_team or outcome.name == event.away_team:
                        continue  # already matches
                    o_norm = normalize_team_name(outcome.name)
                    # Exact normalized match
                    if o_norm == home_norm:
                        outcome.name = event.home_team
                    elif o_norm == away_norm:
                        outcome.name = event.away_team
                    # Substring match (e.g. "neworleans" in "neworleanspelicans")
                    elif len(o_norm) >= 4 and (o_norm in home_norm or home_norm in o_norm):
                        outcome.name = event.home_team
                    elif len(o_norm) >= 4 and (o_norm in away_norm or away_norm in o_norm):
                        outcome.name = event.away_team


# Bookmakers whose backend doesn't provide totals — derive from consensus
_DERIVE_TOTALS_BOOKS = frozenset(["buckeye"])


def _fill_missing_totals(events: List[OddsEvent]) -> None:
    """For bookmakers in _DERIVE_TOTALS_BOOKS that have spreads but no totals,
    derive the totals market from the consensus of other bookmakers.

    Uses the median total-point line from all other books that have totals for
    the same event, with standard -110/-110 juice (actual juice isn't available).
    Applies to all period variants (totals, totals_h1, totals_h2, etc.).
    """
    for event in events:
        # Collect total-point lines from all non-derived bookmakers
        # Group by market key suffix (game, h1, h2, etc.)
        consensus = {}  # type: Dict[str, List[float]]
        for bm in event.bookmakers:
            if bm.key in _DERIVE_TOTALS_BOOKS:
                continue
            for m in bm.markets:
                if not m.key.startswith("totals"):
                    continue
                for o in m.outcomes:
                    if o.point is not None and o.point > 0:
                        consensus.setdefault(m.key, []).append(o.point)
                        break  # one point per market per bookmaker

        if not consensus:
            continue

        for bm in event.bookmakers:
            if bm.key not in _DERIVE_TOTALS_BOOKS:
                continue

            existing_total_keys = {m.key for m in bm.markets if m.key.startswith("totals")}

            for market_key, points in consensus.items():
                if market_key in existing_total_keys:
                    continue  # already has this totals market
                if len(points) < 2:
                    continue  # need at least 2 books for consensus

                median_point = round(statistics.median(points) * 2) / 2  # round to nearest 0.5
                bm.markets.append(Market(
                    key=market_key,
                    outcomes=[
                        Outcome(name="Over", price=-110, point=median_point),
                        Outcome(name="Under", price=-110, point=median_point),
                    ],
                ))
                logger.debug(
                    "Derived %s for %s on %s: %.1f (from %d books)",
                    market_key, bm.key, event.id, median_point, len(points),
                )


# Standard deviation of game margin by sport (for spread→ML conversion)
_SPORT_SIGMA = {
    "basketball_nba": 12.0,
    "basketball_ncaab": 11.0,
    "americanfootball_nfl": 13.5,
    "americanfootball_ncaaf": 16.0,
    "baseball_mlb": 1.5,
    "icehockey_nhl": 1.5,
}  # type: Dict[str, float]


def _prob_to_american(prob):
    # type: (float) -> int
    """Convert win probability (0-1) to American odds with standard vig."""
    if prob <= 0.01:
        return 5000
    if prob >= 0.99:
        return -5000
    if prob >= 0.5:
        return -round(100 * prob / (1 - prob))
    else:
        return round(100 * (1 - prob) / prob)


def _fill_missing_moneylines(events: List[OddsEvent], sport_key: str) -> None:
    """For any bookmaker that has spreads but no h2h, derive moneyline from spread.

    Uses the normal distribution relationship:
        win_probability = Φ(spread / σ)
    where σ is the sport-specific standard deviation of game margin.
    """
    sigma = _SPORT_SIGMA.get(sport_key)
    if sigma is None:
        return  # unsupported sport

    for event in events:
        for bm in event.bookmakers:
            h2h_keys = {m.key for m in bm.markets if m.key.startswith("h2h")}
            spread_keys = {m.key for m in bm.markets if m.key.startswith("spreads")}

            for sp_key in spread_keys:
                # Determine corresponding h2h key
                suffix = sp_key[len("spreads"):]  # e.g., "" or "_h1"
                h2h_key = "h2h" + suffix

                if h2h_key in h2h_keys:
                    continue  # already has moneyline for this period

                # Find the spread market
                sp_market = None
                for m in bm.markets:
                    if m.key == sp_key:
                        sp_market = m
                        break
                if not sp_market or len(sp_market.outcomes) < 2:
                    continue

                # Get the spread point from the home team (second outcome)
                # Outcome order: [away, home] — home's point tells us direction
                away_out = sp_market.outcomes[0]
                home_out = sp_market.outcomes[1]
                home_point = home_out.point
                if home_point is None:
                    continue

                spread_abs = abs(home_point)
                if spread_abs == 0:
                    # Pick-em
                    bm.markets.append(Market(
                        key=h2h_key,
                        outcomes=[
                            Outcome(name=away_out.name, price=-110),
                            Outcome(name=home_out.name, price=-110),
                        ],
                    ))
                    continue

                # Normal CDF: probability favorite wins
                fav_prob = 0.5 * (1.0 + math.erf(spread_abs / (sigma * math.sqrt(2))))
                dog_prob = 1.0 - fav_prob

                # Add ~4.5% vig
                fav_prob_vig = min(0.99, fav_prob * 1.023)
                dog_prob_vig = min(0.99, dog_prob * 1.023)

                fav_odds = _prob_to_american(fav_prob_vig)
                dog_odds = _prob_to_american(dog_prob_vig)

                # Negative point = favored
                if home_point < 0:
                    home_price = fav_odds
                    away_price = dog_odds
                else:
                    home_price = dog_odds
                    away_price = fav_odds

                bm.markets.append(Market(
                    key=h2h_key,
                    outcomes=[
                        Outcome(name=away_out.name, price=away_price),
                        Outcome(name=home_out.name, price=home_price),
                    ],
                ))
                logger.debug(
                    "Derived %s for %s on %s from spread %.1f",
                    h2h_key, bm.key, event.id, home_point,
                )


def _fuzzy_merge_by_date(all_events: Dict[str, OddsEvent]) -> Dict[str, OddsEvent]:
    """Merge events that have the same teams but dates ±1 day apart.

    Different sources sometimes report the same game with commence times that
    differ by exactly 24 hours (e.g., FanDuel says Feb 26 00:30Z, Kalshi says
    Feb 25 00:30Z for the same game). This prevents them from merging on
    canonical ID alone. This pass finds those near-duplicates and merges them.

    Strategy: group events by team-key (sport + sorted teams, no date).
    Within each group, if exactly 2 events differ by ≤1 day, merge into the
    one that has more bookmakers (or the earlier date as tiebreaker).
    """
    # Build team-key → list of event IDs
    team_groups = {}  # type: Dict[str, List[str]]
    for eid, ev in all_events.items():
        # Extract team key: everything before the last ":date" segment
        parts = eid.rsplit(":", 1)
        if len(parts) == 2:
            team_key = parts[0]
        else:
            team_key = eid
        team_groups.setdefault(team_key, []).append(eid)

    merged_count = 0
    for team_key, eids in team_groups.items():
        if len(eids) < 2:
            continue

        # Check all pairs for date proximity
        events_list = [(eid, all_events[eid]) for eid in eids if eid in all_events]
        for i in range(len(events_list)):
            for j in range(i + 1, len(events_list)):
                eid_a, ev_a = events_list[i]
                eid_b, ev_b = events_list[j]

                if eid_a not in all_events or eid_b not in all_events:
                    continue  # Already merged away

                # Check if dates are within 1 day (or one side has no date)
                try:
                    ct_a = ev_a.commence_time
                    ct_b = ev_b.commence_time

                    # If one event has no commence_time, merge into the
                    # one that DOES have a valid time (same teams already
                    # matched via team_key, so these are the same game).
                    if not ct_a and not ct_b:
                        continue  # Neither has a date — skip
                    elif not ct_a or not ct_b:
                        # One side missing date → auto-merge into the one
                        # with a valid date (it has better metadata).
                        if ct_a:
                            winner, loser = ev_a, ev_b
                            winner_id, loser_id = eid_a, eid_b
                        else:
                            winner, loser = ev_b, ev_a
                            winner_id, loser_id = eid_b, eid_a

                        # Merge loser's bookmakers into winner
                        existing_keys = {b.key for b in winner.bookmakers}
                        for bm in loser.bookmakers:
                            if bm.key not in existing_keys:
                                winner.bookmakers.append(bm)
                        if loser.score_data and not winner.score_data:
                            winner.score_data = loser.score_data
                        del all_events[loser_id]
                        merged_count += 1
                        logger.info(
                            "Fuzzy merge (empty date): %s + %s → %s (%d bookmakers)",
                            winner_id, loser_id, winner_id, len(winner.bookmakers),
                        )
                        continue

                    ct_a_clean = _truncate_fractional_seconds(ct_a)
                    ct_b_clean = _truncate_fractional_seconds(ct_b)
                    dt_a = datetime.fromisoformat(ct_a_clean.replace("Z", "+00:00"))
                    dt_b = datetime.fromisoformat(ct_b_clean.replace("Z", "+00:00"))
                    diff = abs((dt_a - dt_b).total_seconds())

                    if diff > 129600:  # More than 36 hours apart
                        continue
                except Exception:
                    continue

                # These are the same game — merge into the one with more bookmakers
                # (that source likely has the correct date). Tie-break: earlier date.
                if len(ev_a.bookmakers) > len(ev_b.bookmakers):
                    winner, loser = ev_a, ev_b
                    winner_id, loser_id = eid_a, eid_b
                elif len(ev_b.bookmakers) > len(ev_a.bookmakers):
                    winner, loser = ev_b, ev_a
                    winner_id, loser_id = eid_b, eid_a
                else:
                    # Same number of bookmakers — prefer earlier date
                    if dt_a <= dt_b:
                        winner, loser = ev_a, ev_b
                        winner_id, loser_id = eid_a, eid_b
                    else:
                        winner, loser = ev_b, ev_a
                        winner_id, loser_id = eid_b, eid_a

                # Merge loser's bookmakers into winner
                existing_keys = {b.key for b in winner.bookmakers}
                for bm in loser.bookmakers:
                    if bm.key not in existing_keys:
                        winner.bookmakers.append(bm)

                # Merge score_data
                if loser.score_data and not winner.score_data:
                    winner.score_data = loser.score_data

                # Remove the loser event
                del all_events[loser_id]
                merged_count += 1
                logger.info(
                    "Fuzzy merge: %s + %s → %s (%d bookmakers)",
                    winner_id, loser_id, winner_id, len(winner.bookmakers),
                )

    if merged_count:
        logger.info("Fuzzy date merge: consolidated %d duplicate events", merged_count)

    return all_events


def _fuzzy_merge_by_team_name(all_events: Dict[str, OddsEvent]) -> Dict[str, OddsEvent]:
    """Merge events on the same date where team names are similar but not identical.

    Safety net for cases where team name normalization doesn't fully converge.
    Uses substring matching and last-name matching (for MMA fighters).
    """
    # Group events by (sport_key, date)
    date_groups = {}  # type: Dict[Tuple[str, str], List[str]]
    for eid in all_events:
        parts = eid.rsplit(":", 1)
        if len(parts) != 2:
            continue
        date = parts[1]
        sport = all_events[eid].sport_key
        date_groups.setdefault((sport, date), []).append(eid)

    merged_count = 0
    for (sport, date), eids in date_groups.items():
        if len(eids) < 2:
            continue

        # Compare each pair
        for i in range(len(eids)):
            for j in range(i + 1, len(eids)):
                eid_a = eids[i]
                eid_b = eids[j]
                if eid_a not in all_events or eid_b not in all_events:
                    continue

                ev_a = all_events[eid_a]
                ev_b = all_events[eid_b]

                if not _teams_match_fuzzy(ev_a, ev_b, sport):
                    continue

                # Merge into the event with more bookmakers
                if len(ev_a.bookmakers) >= len(ev_b.bookmakers):
                    winner, loser = ev_a, ev_b
                    winner_id, loser_id = eid_a, eid_b
                else:
                    winner, loser = ev_b, ev_a
                    winner_id, loser_id = eid_b, eid_a

                existing_keys = {b.key for b in winner.bookmakers}
                for bm in loser.bookmakers:
                    if bm.key not in existing_keys:
                        winner.bookmakers.append(bm)

                if loser.score_data and not winner.score_data:
                    winner.score_data = loser.score_data

                del all_events[loser_id]
                merged_count += 1
                logger.info(
                    "Fuzzy team merge: %s + %s → %s (%d bookmakers)",
                    winner_id, loser_id, winner_id, len(winner.bookmakers),
                )

    if merged_count:
        logger.info("Fuzzy team merge: consolidated %d duplicate events", merged_count)

    return all_events


def _teams_match_fuzzy(ev_a: OddsEvent, ev_b: OddsEvent, sport: str) -> bool:
    """Check if two events likely represent the same matchup via fuzzy name matching.

    Uses normalized substring matching and last-name matching for MMA.
    """
    home_a = normalize_team_name(ev_a.home_team)
    away_a = normalize_team_name(ev_a.away_team)
    home_b = normalize_team_name(ev_b.home_team)
    away_b = normalize_team_name(ev_b.away_team)

    # Quick exact check — if normalized names match, it's the same event
    teams_a = sorted([home_a, away_a])
    teams_b = sorted([home_b, away_b])
    if teams_a == teams_b:
        return True

    is_combat = "mma" in sport or "boxing" in sport

    # Substring matching: one team name contains the other
    def _name_matches(n1: str, n2: str) -> bool:
        if not n1 or not n2:
            return False
        if n1 == n2:
            return True
        # One is a substring of the other (min length 4 to avoid false positives)
        if len(n1) >= 4 and len(n2) >= 4:
            if n1 in n2 or n2 in n1:
                return True
        # For MMA/boxing: prefix match handles transliteration differences
        # e.g. "nurgozhaev" vs "nurgozhay" share prefix "nurgozha" (8 chars)
        if is_combat and len(n1) >= 6 and len(n2) >= 6:
            min_len = min(len(n1), len(n2))
            # If 80%+ of the shorter name is a shared prefix, consider them matching
            shared = 0
            for c1, c2 in zip(n1, n2):
                if c1 == c2:
                    shared += 1
                else:
                    break
            if shared >= max(6, int(min_len * 0.8)):
                return True
        return False

    # Check if both team pairs match (in either order)
    if (_name_matches(teams_a[0], teams_b[0]) and _name_matches(teams_a[1], teams_b[1])):
        return True
    if (_name_matches(teams_a[0], teams_b[1]) and _name_matches(teams_a[1], teams_b[0])):
        return True

    # For MMA/boxing: also match by last name (last word) of fighters
    if is_combat:
        def _extract_last_name(original_name: str) -> str:
            """Extract last name from fighter's original (pre-normalized) name."""
            parts = original_name.strip().split()
            if len(parts) >= 2:
                return normalize_team_name(parts[-1])
            return normalize_team_name(original_name)

        last_a = sorted([_extract_last_name(ev_a.home_team), _extract_last_name(ev_a.away_team)])
        last_b = sorted([_extract_last_name(ev_b.home_team), _extract_last_name(ev_b.away_team)])
        if last_a[0] and last_b[0] and len(last_a[0]) >= 3 and len(last_b[0]) >= 3:
            if last_a == last_b:
                return True

    return False


class CompositeSource(DataSource):
    """Combines multiple data sources, merging bookmaker data by event ID."""

    def __init__(self, sources: List[DataSource]):
        self._sources = sources

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        all_events: Dict[str, OddsEvent] = {}
        merged_headers: Dict[str, str] = {"x-requests-remaining": "unknown"}

        import time as _time

        async def _fetch_source(source: DataSource) -> Optional[Tuple[List[OddsEvent], Dict[str, str]]]:
            t0 = _time.time()
            name = source.__class__.__name__
            try:
                result = await asyncio.wait_for(
                    source.get_odds(
                        sport_key, regions, markets, bookmakers, odds_format
                    ),
                    timeout=12.0,
                )
                return result
            except asyncio.TimeoutError:
                logger.warning(f"Source {name} timed out for {sport_key} (12s)")
                return None
            except Exception as e:
                logger.warning(f"Source {name} failed for {sport_key}: {type(e).__name__}: {e}")
                return None

        # Fetch all sources in parallel (each with 12s timeout)
        results = await asyncio.gather(
            *[_fetch_source(s) for s in self._sources],
            return_exceptions=False,
        )

        # Log per-source timing summary
        source_timings = []
        for source, result in zip(self._sources, results):
            name = source.__class__.__name__.replace("Source", "")
            n = len(result[0]) if result else 0
            source_timings.append(f"{name}:{n}")
        logger.info(f"Composite {sport_key}: {' '.join(source_timings)}")

        for result in results:
            if result is None:
                continue
            events, headers = result

            if "x-requests-remaining" in headers:
                merged_headers["x-requests-remaining"] = headers["x-requests-remaining"]

            for event in events:
                if event.id in all_events:
                    # Merge: append new bookmakers from secondary sources
                    existing = all_events[event.id]
                    existing_keys = {b.key for b in existing.bookmakers}
                    for bm in event.bookmakers:
                        if bm.key not in existing_keys:
                            existing.bookmakers.append(bm)
                    # Merge score_data (prefer first non-None)
                    if event.score_data and not existing.score_data:
                        existing.score_data = event.score_data
                else:
                    all_events[event.id] = event

        # Fuzzy merge: consolidate events with same teams but dates ±1 day apart
        all_events = _fuzzy_merge_by_date(all_events)

        # Fuzzy merge: consolidate events with similar team names on same date
        all_events = _fuzzy_merge_by_team_name(all_events)

        # Filter out completed events (status="post"), futures, stale, women's games,
        # prop-market events (Corners/Bookings), and aggregated summary rows
        result = [
            ev for ev in all_events.values()
            if not (ev.score_data and ev.score_data.status == "post")
            and not _is_futures_event(ev)
            and not _is_stale_event(ev)
            and not _is_womens_event(ev)
            and not _is_prop_or_summary_event(ev)
        ]

        # Normalize display names through alias resolution
        _normalize_display_names(result)

        # Sort by commence_time so events appear in chronological order
        # Use proper datetime parsing to handle mixed ISO formats
        # (e.g., "Z", ".000Z", "+00:00")
        def _sort_key(ev: OddsEvent) -> datetime:
            ct = ev.commence_time
            if not ct:
                return datetime.max.replace(tzinfo=timezone.utc)
            try:
                ct = _truncate_fractional_seconds(ct)
                if ct.endswith("Z"):
                    return datetime.fromisoformat(ct.replace("Z", "+00:00"))
                dt = datetime.fromisoformat(ct)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                return datetime.max.replace(tzinfo=timezone.utc)

        result.sort(key=_sort_key)

        # Normalize commence_time format to consistent ISO 8601 with Z suffix
        for ev in result:
            if ev.commence_time:
                try:
                    dt = _sort_key(ev)
                    if dt != datetime.max.replace(tzinfo=timezone.utc):
                        ev.commence_time = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    pass

        # Remove quarter/inning markets for halves-only sports (NCAAB, WNBA, etc.)
        result = _filter_invalid_period_markets(result, sport_key)

        # For tennis, split "totals" into "totals_sets" and "totals_games"
        if "tennis" in sport_key:
            _reclassify_tennis_totals(result)

        # Derive missing totals for bookmakers that don't provide them
        _fill_missing_totals(result)

        # Derive missing moneylines from spreads (e.g., Pinnacle NCAAB)
        _fill_missing_moneylines(result, sport_key)

        # NOTE: No longer enriching game events with championship futures.
        # Futures prices (e.g. +1011, +9900) are championship win probabilities
        # and are confusing when shown alongside actual game moneylines.
        # Polymarket game-level markets still merge in normally via event IDs.

        return result, merged_headers

    async def _enrich_with_futures(
        self, events: List[OddsEvent], sport_key: str
    ) -> None:
        """Add prediction-market championship futures odds to game events.

        For sources like Polymarket that don't have game-level markets but
        DO have championship futures, attach each team's championship odds
        to the corresponding game events.
        """
        for source in self._sources:
            try:
                futures_data = await source.get_team_futures(sport_key)
                if not futures_data:
                    continue

                bm_key = futures_data["bookmaker_key"]
                bm_title = futures_data["bookmaker_title"]
                teams = futures_data["teams"]
                event_url = futures_data.get("event_url")

                enriched_count = 0
                for event in events:
                    # Skip if this bookmaker already has data for this event
                    existing_keys = {b.key for b in event.bookmakers}
                    if bm_key in existing_keys:
                        continue

                    # Look up both teams' championship odds
                    home_norm = normalize_team_name(event.home_team)
                    away_norm = normalize_team_name(event.away_team)

                    home_data = teams.get(home_norm)
                    away_data = teams.get(away_norm)

                    # Need odds for at least one team to be useful
                    if not home_data and not away_data:
                        continue

                    home_price = home_data["price"] if home_data else 0
                    away_price = away_data["price"] if away_data else 0

                    # Compute combined liquidity from both teams' markets
                    home_liq = (home_data or {}).get("liquidity")
                    away_liq = (away_data or {}).get("liquidity")
                    total_liq = None  # type: Optional[float]
                    if home_liq or away_liq:
                        total_liq = (home_liq or 0) + (away_liq or 0)

                    h2h_market = Market(
                        key="h2h",
                        outcomes=[
                            Outcome(name=event.home_team, price=home_price),
                            Outcome(name=event.away_team, price=away_price),
                        ],
                        liquidity=total_liq,
                    )

                    event.bookmakers.append(
                        Bookmaker(
                            key=bm_key,
                            title=bm_title,
                            markets=[h2h_market],
                            event_url=event_url,
                        )
                    )
                    enriched_count += 1

                if enriched_count:
                    logger.info(
                        f"{bm_title}: enriched {enriched_count} events with "
                        f"championship futures for {sport_key}"
                    )

            except Exception as e:
                logger.warning(
                    f"Futures enrichment from {source.__class__.__name__} failed: {e}"
                )

    async def get_player_props(self, sport_key: str, event_id: str) -> List[PlayerProp]:
        """Collect player props from all sources in parallel."""
        all_props: List[PlayerProp] = []
        source_counts: List[str] = []

        async def _fetch_props(source: DataSource) -> Tuple[str, Optional[List[PlayerProp]]]:
            try:
                props = await source.get_player_props(sport_key, event_id)
                return source.__class__.__name__, props
            except Exception as e:
                logger.warning(f"Props from {source.__class__.__name__} failed: {e}")
                return source.__class__.__name__, None

        results = await asyncio.gather(
            *[_fetch_props(s) for s in self._sources],
            return_exceptions=False,
        )

        for name, props in results:
            if props:
                all_props.extend(props)
                source_counts.append(f"{name}={len(props)}")
            elif props is None:
                source_counts.append(f"{name}=ERR")

        if source_counts:
            logger.info(f"Props for {event_id}: {', '.join(source_counts)} (total={len(all_props)})")
        return all_props

    async def close(self) -> None:
        for source in self._sources:
            await source.close()
