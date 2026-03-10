"""
Stake.us / Stake.com sportsbook scraper.

Uses Playwright to bypass Cloudflare, then makes GraphQL API calls
via in-page fetch() to Stake.com's sportsbook API.

Note: Stake.us (US sweepstakes) has its sportsbook disabled.
We scrape from stake.com (international) which shares the same API.
Odds are displayed as "StakeUS" in the frontend to match branding.

Architecture:
  1. Launch headless Chrome with stealth to bypass Cloudflare
  2. Navigate to stake.com/sports to establish session cookies
  3. Resolve sport slugs to UUIDs via slugSport query
  4. Fetch fixtures via sport(sportId) -> fixtureList() -> groups()
  5. Parse fixture data and convert decimal odds to American format
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from models import Bookmaker, Market, OddsEvent, Outcome
from sources.base import DataSource
from sources.sport_mapping import (
    canonical_event_id,
    decimal_to_american,
    get_sport_title,
    resolve_team_name,
)

logger = logging.getLogger(__name__)

SITE_URL = "https://stake.com"
GQL_PATH = "/_api/graphql"

# ── Sport key → Stake sport slug ──────────────────────────────────────
STAKE_SPORT_SLUGS: Dict[str, str] = {
    "basketball_nba": "basketball",
    "basketball_ncaab": "basketball",
    "icehockey_nhl": "ice-hockey",
    "baseball_mlb": "baseball",
    "mma_mixed_martial_arts": "mma",
}

# ── League name filters (Stake groups many leagues under one sport) ───
LEAGUE_FILTERS: Dict[str, List[str]] = {
    "basketball_nba": ["nba"],
    "basketball_ncaab": ["ncaa", "college", "ncaab", "march madness"],
    "icehockey_nhl": ["nhl"],
    "baseball_mlb": ["mlb", "major league"],
    "mma_mixed_martial_arts": ["ufc"],
}

# ── Market group names to request from Stake API ──────────────────────
# Each group is requested separately via the groups() field on fixtures
MARKET_GROUPS = ["winner", "totals", "handicap"]

# ── Market group → canonical market key ───────────────────────────────
MARKET_GROUP_MAP: Dict[str, str] = {
    "winner": "h2h",
    "moneyline": "h2h",
    "1x2": "h2h",
    "handicap": "spreads",
    "spread": "spreads",
    "point spread": "spreads",
    "total": "totals",
    "over/under": "totals",
    "total points": "totals",
    "total goals": "totals",
    "total rounds": "totals_rounds",
    "totals": "totals",
}

# ── Period name → market suffix ───────────────────────────────────────
PERIOD_SUFFIX_MAP: Dict[str, str] = {
    "1st half": "_h1",
    "2nd half": "_h2",
    "1st quarter": "_q1",
    "2nd quarter": "_q2",
    "3rd quarter": "_q3",
    "4th quarter": "_q4",
    "1st period": "_p1",
    "2nd period": "_p2",
    "3rd period": "_p3",
    "1st inning": "_i1",
    "first 5 innings": "_f5",
    "first 7 innings": "_f7",
}

# ── GraphQL: Resolve sport slug → UUID ────────────────────────────────
GQL_SPORT_ID = """
query SlugSport($slug: String!) {
  slugSport(slug: $slug) {
    id
    name
  }
}
"""

# ── GraphQL: Fetch upcoming fixtures with market data ─────────────────
# Uses the correct nested structure: sport -> fixtureList -> groups
GQL_FIXTURES = """
query SportFixtureList(
  $sportId: String!,
  $type: SportSearchEnum!,
  $limit: Int!,
  $offset: Int!,
  $groups: [String!]!
) {
  sport(sportId: $sportId) {
    name
    fixtureList(type: $type, limit: $limit, offset: $offset) {
      id
      extId
      status
      slug
      data {
        ... on SportFixtureDataMatch {
          startTime
          competitors {
            name
            extId
            abbreviation
          }
        }
      }
      tournament {
        name
        slug
        category {
          name
          slug
          sport {
            name
            slug
          }
        }
      }
      groups(groups: $groups, status: [active, suspended, deactivated]) {
        name
        markets {
          id
          name
          status
          extId
          specifiers
          outcomes {
            id
            active
            name
            odds
          }
        }
      }
    }
  }
}
"""


class StakeUSSource(DataSource):
    """Fetches odds from Stake.com via Playwright + in-page GraphQL calls.

    Uses Playwright to bypass Cloudflare, then executes GraphQL queries
    from within the browser context. Runs as a background prefetch loop.
    """

    def __init__(self):
        self._browser = None
        self._context = None
        self._page = None
        self._pw = None
        self._lock = asyncio.Lock()
        # Prefetched odds cache: { sport_key: [OddsEvent, ...] }
        self._cache: Dict[str, List[OddsEvent]] = {}
        self._prefetch_task = None
        self._consecutive_zero_cycles: int = 0
        # Cache of sport slug → UUID (resolved once, reused)
        self._sport_ids: Dict[str, str] = {}

    def start_prefetch(self) -> None:
        """Start background prefetch loop (call after event loop is running)."""
        self._prefetch_task = asyncio.ensure_future(self._prefetch_loop())

    async def get_odds(
        self,
        sport_key: str,
        regions: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
        bookmakers: Optional[List[str]] = None,
        odds_format: str = "american",
    ) -> Tuple[List[OddsEvent], Dict[str, str]]:
        if bookmakers and "stakeus" not in bookmakers:
            return [], {"x-requests-remaining": "unlimited"}
        if sport_key not in STAKE_SPORT_SLUGS:
            return [], {"x-requests-remaining": "unlimited"}
        return self._cache.get(sport_key, []), {"x-requests-remaining": "unlimited"}

    # ------------------------------------------------------------------
    # Background prefetch loop
    # ------------------------------------------------------------------

    async def _prefetch_loop(self) -> None:
        """Continuously fetch odds for all supported sports."""
        await asyncio.sleep(12)  # Stagger behind other Playwright scrapers
        logger.info("StakeUS: Starting background prefetch loop")
        cycle = 0

        while True:
            cycle += 1
            total_events = 0

            async with self._lock:
                try:
                    await self._ensure_browser()
                    if self._page is None:
                        logger.warning("StakeUS: No browser page, skipping cycle %d", cycle)
                        await asyncio.sleep(30)
                        continue

                    # Resolve sport UUIDs on first cycle
                    if not self._sport_ids:
                        await self._resolve_sport_ids()

                    for sport_key, sport_slug in STAKE_SPORT_SLUGS.items():
                        try:
                            sport_id = self._sport_ids.get(sport_slug)
                            if not sport_id:
                                if cycle <= 2:
                                    logger.info(
                                        "StakeUS: No UUID for %s (slug=%s), skipping",
                                        sport_key, sport_slug,
                                    )
                                continue

                            events = await self._fetch_sport(sport_key, sport_slug, sport_id)
                            if events:
                                self._cache[sport_key] = events
                                total_events += len(events)
                        except Exception as exc:
                            logger.warning("StakeUS: Error fetching %s: %s", sport_key, exc)
                        await asyncio.sleep(1)  # Small delay between sports

                except Exception as exc:
                    logger.warning("StakeUS: Prefetch cycle %d error: %s", cycle, exc)

            if total_events == 0:
                self._consecutive_zero_cycles += 1
            else:
                self._consecutive_zero_cycles = 0

            # Restart browser after 5 consecutive zero-event cycles
            if self._consecutive_zero_cycles >= 5:
                logger.warning(
                    "StakeUS: %d consecutive zero-event cycles — restarting browser",
                    self._consecutive_zero_cycles,
                )
                await self._close_browser()
                self._sport_ids.clear()  # Re-resolve UUIDs after restart
                self._consecutive_zero_cycles = 0
                await asyncio.sleep(10)

            logger.info(
                "StakeUS: Prefetch cycle #%d complete (%d total events)",
                cycle, total_events,
            )
            await asyncio.sleep(30)  # Refresh every 30 seconds

    # ------------------------------------------------------------------
    # Browser management
    # ------------------------------------------------------------------

    async def _ensure_browser(self) -> None:
        """Launch Playwright browser with stealth mode to bypass Cloudflare."""
        if self._page is not None:
            return

        try:
            from playwright.async_api import async_playwright

            self._pw = await async_playwright().start()

            launch_args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
            try:
                self._browser = await self._pw.chromium.launch(
                    headless=True, channel="chrome", args=launch_args,
                )
                logger.info("StakeUS: Launched system Chrome")
            except Exception:
                self._browser = await self._pw.chromium.launch(
                    headless=True, args=launch_args,
                )
                logger.info("StakeUS: Launched bundled Chromium (fallback)")

            self._context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )

            # Apply stealth evasions
            try:
                from playwright_stealth import Stealth
                stealth = Stealth()
                await stealth.apply_stealth_async(self._context)
            except ImportError:
                await self._context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    window.chrome = {runtime: {}};
                """)

            self._page = await self._context.new_page()
            logger.info("StakeUS: Playwright browser launched (stealth mode)")

            # Navigate to sportsbook to establish CF session cookies
            try:
                logger.info("StakeUS: Loading sportsbook to establish session")
                await self._page.goto(
                    f"{SITE_URL}/sports/basketball",
                    timeout=45000,
                    wait_until="domcontentloaded",
                )
                # Wait for CF challenge to resolve and page to load
                await asyncio.sleep(8)
                title = await self._page.title()
                url = self._page.url
                logger.info("StakeUS: Session established (title: %r, url: %s)", title, url)
            except Exception as e:
                logger.warning("StakeUS: Session setup failed: %s", e)
                # Page may still have valid CF cookies even if timeout occurred

        except Exception as e:
            logger.warning("StakeUS: Failed to launch browser: %s", e)
            self._page = None

    async def _close_browser(self) -> None:
        """Shut down browser and clean up."""
        try:
            if self._page:
                await self._page.close()
        except Exception:
            pass
        try:
            if self._context:
                await self._context.close()
        except Exception:
            pass
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        try:
            if self._pw:
                await self._pw.stop()
        except Exception:
            pass
        self._page = None
        self._context = None
        self._browser = None
        self._pw = None

    # ------------------------------------------------------------------
    # GraphQL API calls (via in-page fetch)
    # ------------------------------------------------------------------

    async def _gql_call(self, query: str, variables: dict) -> Optional[dict]:
        """Execute a GraphQL query via page.evaluate(fetch(...))."""
        if self._page is None:
            return None

        try:
            result = await self._page.evaluate(
                """async ([query, variables, url]) => {
                    try {
                        const r = await fetch(url, {
                            method: "POST",
                            headers: {
                                "Content-Type": "application/json",
                                "Accept": "*/*",
                                "x-language": "en",
                            },
                            body: JSON.stringify({query, variables}),
                            credentials: "include",
                        });
                        if (!r.ok) return {__error: r.status, __statusText: r.statusText};
                        const json = await r.json();
                        return json;
                    } catch(e) {
                        return {__error: e.message};
                    }
                }""",
                [query, variables, f"{SITE_URL}{GQL_PATH}"],
            )

            if isinstance(result, dict) and "__error" in result:
                err = result["__error"]
                if err == 403:
                    logger.warning("StakeUS: GraphQL 403 — CF block, restarting browser")
                    await self._close_browser()
                    return None
                logger.warning("StakeUS: GraphQL error: %s (status: %s)", err, result.get("__statusText", ""))
                return None

            return result
        except Exception as exc:
            logger.warning("StakeUS: GraphQL call failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Resolve sport slugs → UUIDs
    # ------------------------------------------------------------------

    async def _resolve_sport_ids(self) -> None:
        """Resolve all sport slugs to UUIDs via the slugSport query."""
        unique_slugs = set(STAKE_SPORT_SLUGS.values())
        for slug in unique_slugs:
            try:
                data = await self._gql_call(GQL_SPORT_ID, {"slug": slug})
                if not data:
                    logger.warning("StakeUS: Failed to resolve sport slug %r", slug)
                    continue

                # Check for GQL errors
                if data.get("errors"):
                    logger.warning(
                        "StakeUS: GQL errors resolving %r: %s",
                        slug, data["errors"],
                    )
                    continue

                sport_data = (data.get("data") or {}).get("slugSport")
                if sport_data and sport_data.get("id"):
                    self._sport_ids[slug] = sport_data["id"]
                    logger.info(
                        "StakeUS: Resolved %r → %s (name: %s)",
                        slug, sport_data["id"], sport_data.get("name"),
                    )
                else:
                    logger.warning("StakeUS: No sport found for slug %r", slug)

                await asyncio.sleep(0.5)
            except Exception as exc:
                logger.warning("StakeUS: Error resolving %r: %s", slug, exc)

        logger.info("StakeUS: Resolved %d/%d sport IDs", len(self._sport_ids), len(unique_slugs))

    # ------------------------------------------------------------------
    # Fetch & parse for one sport
    # ------------------------------------------------------------------

    async def _fetch_sport(
        self, sport_key: str, sport_slug: str, sport_id: str
    ) -> List[OddsEvent]:
        """Fetch and parse fixtures for one sport."""
        variables = {
            "sportId": sport_id,
            "type": "upcoming",
            "limit": 100,
            "offset": 0,
            "groups": MARKET_GROUPS,
        }
        data = await self._gql_call(GQL_FIXTURES, variables)
        if not data:
            logger.info("StakeUS: No GQL data for %s (id=%s)", sport_key, sport_id[:12])
            return []

        # Check for GQL errors
        errors = data.get("errors")
        if errors:
            logger.warning("StakeUS: GQL errors for %s: %s", sport_key, errors[:2])

        sport_node = (data.get("data") or {}).get("sport")
        if not sport_node:
            logger.info("StakeUS: No sport node for %s", sport_key)
            return []

        fixtures = sport_node.get("fixtureList") or []
        logger.info(
            "StakeUS: %d raw fixtures for %s (slug=%s)",
            len(fixtures), sport_key, sport_slug,
        )

        if not fixtures:
            return []

        # Log first fixture for debugging
        sample = fixtures[0]
        t = sample.get("tournament") or {}
        t_name = t.get("name", "?")
        c_name = ((t.get("category") or {}).get("name") or "?")
        status = sample.get("status", "?")
        n_groups = len(sample.get("groups") or [])
        logger.info(
            "StakeUS: Sample — tournament=%r, category=%r, status=%r, groups=%d",
            t_name, c_name, status, n_groups,
        )

        # Filter to relevant league
        league_keywords = LEAGUE_FILTERS.get(sport_key, [])
        filtered = []
        skipped_leagues = set()
        for fix in fixtures:
            if not fix or not fix.get("data"):
                continue

            tournament = fix.get("tournament") or {}
            tournament_name = (tournament.get("name") or "").lower()
            category_name = ((tournament.get("category") or {}).get("name") or "").lower()

            if league_keywords:
                combined = f"{tournament_name} {category_name}"
                if not any(kw in combined for kw in league_keywords):
                    skipped_leagues.add(tournament_name)
                    continue

            status = (fix.get("status") or "").lower()
            if status in ("live", "in_progress", "ended", "closed", "cancelled"):
                continue

            filtered.append(fix)

        if skipped_leagues:
            logger.info(
                "StakeUS: %s — filtered out %d leagues (e.g. %s)",
                sport_key, len(skipped_leagues), list(skipped_leagues)[:3],
            )

        # Parse fixtures
        events = []
        seen_ids = set()
        for fix in filtered:
            try:
                event = self._parse_fixture(fix, sport_key)
                if event and event.id not in seen_ids:
                    seen_ids.add(event.id)
                    events.append(event)
            except Exception as exc:
                logger.debug("StakeUS: skip fixture: %s", exc)

        logger.info("StakeUS: %d events for %s", len(events), sport_key)
        return events

    # ------------------------------------------------------------------
    # Fixture parsing
    # ------------------------------------------------------------------

    def _parse_fixture(
        self, fix: Dict[str, Any], sport_key: str
    ) -> Optional[OddsEvent]:
        """Parse a single Stake fixture into an OddsEvent."""
        match_data = fix.get("data") or {}
        competitors = match_data.get("competitors") or []
        if len(competitors) < 2:
            return None

        # Stake uses [away, home] order (first competitor is away)
        away_raw = competitors[0].get("name", "Unknown")
        home_raw = competitors[1].get("name", "Unknown")

        home_team = resolve_team_name(home_raw, sport_key)
        away_team = resolve_team_name(away_raw, sport_key)

        start_time = match_data.get("startTime")
        if not start_time:
            return None

        # Normalise start time
        if isinstance(start_time, (int, float)):
            if start_time > 1e12:
                start_time = start_time / 1000
            commence_time = datetime.fromtimestamp(start_time, tz=timezone.utc).isoformat()
        else:
            commence_time = str(start_time)

        commence_date = commence_time[:10]
        event_id = canonical_event_id(sport_key, home_team, away_team, commence_date)

        # Parse markets from groups
        groups = fix.get("groups") or []
        parsed_markets = self._parse_groups(groups, home_team, away_team, sport_key)

        if not parsed_markets:
            return None

        # Build event URL
        fixture_slug = fix.get("slug", "")
        sport_slug_url = (
            ((fix.get("tournament") or {}).get("category") or {}).get("sport") or {}
        ).get("slug", "")
        event_url = (
            f"{SITE_URL}/sports/{sport_slug_url}/{fixture_slug}"
            if sport_slug_url and fixture_slug else None
        )

        bookmaker = Bookmaker(
            key="stakeus",
            title="Stake.us",
            last_update=datetime.now(timezone.utc).isoformat(),
            markets=parsed_markets,
            event_url=event_url,
        )

        return OddsEvent(
            id=event_id,
            sport_key=sport_key,
            sport_title=get_sport_title(sport_key),
            commence_time=commence_time,
            home_team=home_team,
            away_team=away_team,
            bookmakers=[bookmaker],
        )

    def _parse_groups(
        self,
        groups: List[Dict[str, Any]],
        home_team: str,
        away_team: str,
        sport_key: str,
    ) -> List[Market]:
        """Parse Stake group/market data into Market models."""
        market_map: Dict[str, List[Outcome]] = {}

        for group in groups:
            group_name = (group.get("name") or "").lower().strip()

            for mkt in (group.get("markets") or []):
                status = (mkt.get("status") or "").lower()
                if status not in ("active", "open", ""):
                    continue

                market_name = (mkt.get("name") or "").lower().strip()

                # Determine base market key from group name first, then market name
                base_key = MARKET_GROUP_MAP.get(group_name)
                if not base_key:
                    base_key = MARKET_GROUP_MAP.get(market_name)
                if not base_key:
                    if any(k in market_name for k in ("winner", "moneyline", "money line")):
                        base_key = "h2h"
                    elif any(k in market_name for k in ("handicap", "spread")):
                        base_key = "spreads"
                    elif any(k in market_name for k in ("total", "over")):
                        base_key = "totals"
                    else:
                        continue

                # Detect period suffix from market name
                period_suffix = ""
                for period_label, suffix in PERIOD_SUFFIX_MAP.items():
                    if period_label in market_name:
                        period_suffix = suffix
                        break

                market_key = base_key + period_suffix
                specifiers = mkt.get("specifiers") or ""

                # Parse outcomes
                parsed: List[Outcome] = []
                for oc in (mkt.get("outcomes") or []):
                    if not oc.get("active", True):
                        continue

                    odds_decimal = oc.get("odds")
                    if not odds_decimal:
                        continue

                    # Handle string odds
                    if isinstance(odds_decimal, str):
                        try:
                            odds_decimal = float(odds_decimal)
                        except ValueError:
                            continue

                    if odds_decimal <= 1.0:
                        continue

                    american_odds = decimal_to_american(odds_decimal)
                    if -99 < american_odds < 99 and american_odds != 0:
                        continue

                    outcome_name = oc.get("name", "Unknown")
                    point_val = None

                    if base_key == "spreads":
                        point_val = self._extract_handicap(
                            specifiers, outcome_name, home_team, away_team
                        )
                    elif base_key == "totals":
                        point_val = self._extract_total(specifiers)

                    # Resolve team names
                    resolved = resolve_team_name(outcome_name, sport_key)
                    if base_key == "h2h":
                        if resolved.lower() == home_team.lower():
                            resolved = home_team
                        elif resolved.lower() == away_team.lower():
                            resolved = away_team
                    elif base_key == "totals":
                        name_lower = outcome_name.lower()
                        if "over" in name_lower:
                            resolved = "Over"
                        elif "under" in name_lower:
                            resolved = "Under"

                    parsed.append(Outcome(name=resolved, price=american_odds, point=point_val))

                if parsed:
                    market_map.setdefault(market_key, []).extend(parsed)

        # Deduplicate and build Market objects
        result: List[Market] = []
        for key, outcomes in market_map.items():
            seen = set()
            deduped = []
            for oc in outcomes:
                if oc.name not in seen:
                    seen.add(oc.name)
                    deduped.append(oc)

            max_outcomes = 3 if key.startswith("h2h") else 2
            result.append(Market(
                key=key,
                last_update=datetime.now(timezone.utc).isoformat(),
                outcomes=deduped[:max_outcomes],
            ))

        return result

    @staticmethod
    def _extract_handicap(
        specifiers: str, outcome_name: str,
        home_team: str, away_team: str,
    ) -> Optional[float]:
        """Extract handicap point value from specifiers string."""
        try:
            for part in specifiers.replace(";", ",").split(","):
                part = part.strip()
                key_lower = part.split("=")[0].lower().strip()
                if key_lower in ("hcp", "handicap"):
                    val = float(part.split("=")[-1])
                    resolved = resolve_team_name(outcome_name, "")
                    if resolved.lower() == away_team.lower():
                        val = -val
                    return val
        except (ValueError, IndexError):
            pass
        return None

    @staticmethod
    def _extract_total(specifiers: str) -> Optional[float]:
        """Extract total point value from specifiers string."""
        try:
            for part in specifiers.replace(";", ",").split(","):
                part = part.strip()
                key_lower = part.split("=")[0].lower().strip()
                if key_lower in ("total", "over", "ou", "points"):
                    return float(part.split("=")[-1])
        except (ValueError, IndexError):
            pass
        return None

    async def close(self) -> None:
        if self._prefetch_task:
            self._prefetch_task.cancel()
        await self._close_browser()
