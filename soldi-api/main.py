import asyncio
import time
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, List

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import line_history
from auth import api_key_header, api_key_query, verify_api_key
from cache import TTLCache
from config import Settings
from sources import (
    Bet105Source,
    BetOnlineSource,
    BetRiversSource,
    BetUSSource,
    BookmakerSource,
    BuckeyeSource,
    CaesarsSource,
    CompositeSource,
    DataSource,
    DraftKingsSource,
    FanDuelSource,
    HardRockBetSource,
    KalshiSource,
    NovigSource,
    PinnacleSource,
    ProphetXSource,
    StakeUSSource,
)
from sources.sport_mapping import resolve_team_name, canonical_event_id

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("soldi-api")
# Suppress httpx request logging
logging.getLogger("httpx").setLevel(logging.WARNING)

settings = Settings()
cache = TTLCache(default_ttl=settings.cache_ttl_seconds)

# Will be initialized in lifespan
data_source: Optional[DataSource] = None
source_count: int = 0
_refresh_task: Optional[asyncio.Task] = None

# All sport keys to keep warm in the background refresh loop
_ACTIVE_SPORTS: List[str] = [
    "basketball_nba",
    "basketball_ncaab",
    "icehockey_nhl",
    "baseball_mlb",
    "mma_mixed_martial_arts",
]

# Pause (seconds) AFTER each refresh cycle completes before starting the next.
# Set to 0 for continuous refresh — new cycle starts immediately after previous
# one finishes.  Individual per-source caches handle their own rate-limiting.
_REFRESH_PAUSE = 0



async def _background_refresh_loop() -> None:
    """Continuously refresh odds for ALL active sports in parallel.

    All sports run concurrently.  Each sport has a 15s timeout.
    Playwright sources serve from prefetch cache (~0ms).
    HTTP sources hit APIs directly (~0.2-9s depending on source).
    With 0s pause, a new cycle starts immediately — updates every ~5-10s.
    """
    # Short initial delay — Playwright sources need a moment to warm up
    await asyncio.sleep(2)
    logger.info(
        "Background refresh loop started — %d sports, ALL concurrent",
        len(_ACTIVE_SPORTS),
    )
    cycle = 0
    while True:
        cycle += 1
        t0 = time.time()

        async def _refresh_with_timeout(sport_key: str) -> bool:
            try:
                await asyncio.wait_for(
                    _refresh_one_sport(sport_key), timeout=60.0,
                )
                return True
            except asyncio.TimeoutError:
                logger.warning("Refresh %s timed out (60s)", sport_key)
                return False
            except Exception as exc:
                logger.warning("Refresh %s error: %s", sport_key, exc)
                return False

        results = await asyncio.gather(
            *[_refresh_with_timeout(sk) for sk in _ACTIVE_SPORTS],
        )
        refreshed = sum(1 for r in results if r)

        elapsed = time.time() - t0
        logger.info(
            "Refresh cycle #%d: %d/%d sports in %.1fs — next in %ds",
            cycle, refreshed, len(_ACTIVE_SPORTS), elapsed, _REFRESH_PAUSE,
        )
        await asyncio.sleep(_REFRESH_PAUSE)


async def _refresh_one_sport(sport_key: str) -> None:
    """Fetch + cache one sport.

    Only updates cache if we got at least as many events as the previous cycle.
    This prevents slow-source timeouts from temporarily removing events.
    """
    cache_key = f"{sport_key}:us:h2h::american"
    try:
        events, headers = await data_source.get_odds(
            sport_key=sport_key,
            regions=["us"],
            markets=["h2h", "spreads", "totals"],
            bookmakers=None,
            odds_format="american",
        )
        events_data = [e.model_dump(exclude_none=True) for e in events]

        # Only update cache if we got events (or there's no previous data).
        # This prevents source timeouts from temporarily removing events.
        prev = cache.get(cache_key)
        if events_data or prev is None:
            cache.set(cache_key, (events_data, headers))

        # Record line history
        if events:
            line_history.record_snapshots(events, sport_key)
    except Exception as exc:
        logger.warning("Refresh %s failed: %s", sport_key, exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global data_source, source_count

    sources = []

    # Parse disabled scrapers from env var (comma-separated, case-insensitive)
    disabled = set()
    if settings.disabled_scrapers:
        disabled = {s.strip().lower() for s in settings.disabled_scrapers.split(",") if s.strip()}
        logger.info("Disabled scrapers: %s", disabled)

    def is_enabled(name: str) -> bool:
        return name.lower() not in disabled

    # --- HTTP-only scrapers (lightweight, no Playwright) ---

    if is_enabled("fanduel"):
        logger.info("Initializing FanDuel scraper")
        sources.append(FanDuelSource())

    if is_enabled("pinnacle"):
        logger.info("Initializing Pinnacle scraper")
        sources.append(PinnacleSource())

    if is_enabled("betrivers"):
        logger.info("Initializing BetRivers (Kambi) scraper")
        sources.append(BetRiversSource())

    if is_enabled("kalshi"):
        logger.info("Initializing Kalshi prediction market")
        sources.append(KalshiSource())

    if is_enabled("prophetx"):
        logger.info("Initializing ProphetX exchange scraper")
        sources.append(ProphetXSource())

    if is_enabled("novig"):
        logger.info("Initializing Novig exchange scraper")
        sources.append(NovigSource())

    stakeus = None
    if is_enabled("stakeus"):
        logger.info("Initializing Stake.us scraper (Playwright)")
        stakeus = StakeUSSource()
        sources.append(stakeus)

    if is_enabled("hardrock"):
        logger.info("Initializing Hard Rock Bet scraper")
        hardrock = HardRockBetSource()
        sources.append(hardrock)

    # --- HTTP-only wrappers for scrapers that CAN run without Playwright ---
    # These are initialized in HTTP-only mode (no Chromium browser) when
    # the Playwright version is disabled.  They use direct API calls instead.

    if not is_enabled("caesars"):
        # Caesars Playwright is disabled; try HTTP-only API mode instead
        if "caesars_http" not in disabled:
            logger.info("Initializing Caesars scraper (HTTP-only mode)")
            caesars_http = CaesarsSource(http_only=True)
            sources.append(caesars_http)
            caesars_http.start_prefetch()

    if not is_enabled("buckeye"):
        # Buckeye Playwright is disabled; try direct httpx auth instead
        if "buckeye_http" not in disabled:
            logger.info("Initializing Buckeye scraper (HTTP-only mode)")
            buckeye_http = BuckeyeSource(http_only=True)
            sources.append(buckeye_http)

    if not is_enabled("betonline"):
        # BetOnline Playwright is disabled; try direct HTTP API instead
        if "betonline_http" not in disabled:
            logger.info("Initializing BetOnline scraper (HTTP-only mode)")
            betonline_http = BetOnlineSource(http_only=True)
            sources.append(betonline_http)
            betonline_http.start_prefetch()

    # --- Playwright-based scrapers (require Chromium, ~150-300MB RAM each) ---

    betonline = None
    if is_enabled("betonline"):
        logger.info("Initializing BetOnline scraper")
        betonline = BetOnlineSource()
        sources.append(betonline)

    bet105 = None
    if is_enabled("bet105"):
        logger.info("Initializing Bet105 scraper (ppm.bet105.ag)")
        bet105 = Bet105Source()
        sources.append(bet105)

    dk = None
    if is_enabled("draftkings"):
        logger.info("Initializing DraftKings scraper (Playwright)")
        dk = DraftKingsSource()
        sources.append(dk)

    caesars = None
    if is_enabled("caesars"):
        logger.info("Initializing Caesars scraper")
        caesars = CaesarsSource()
        sources.append(caesars)

    buckeye = None
    if is_enabled("buckeye"):
        logger.info("Initializing Buckeye scraper")
        buckeye = BuckeyeSource()
        sources.append(buckeye)

    betus = None
    if is_enabled("betus"):
        logger.info("Initializing BetUS scraper (Playwright)")
        betus = BetUSSource()
        sources.append(betus)

    # Bookmaker.eu (requires login credentials + Playwright)
    bookmaker = None
    if is_enabled("bookmaker") and settings.bookmaker_username and settings.bookmaker_password:
        logger.info("Initializing Bookmaker.eu scraper")
        bookmaker = BookmakerSource(
            username=settings.bookmaker_username,
            password=settings.bookmaker_password,
        )
        sources.append(bookmaker)
    elif not is_enabled("bookmaker"):
        logger.info("Bookmaker.eu: disabled via DISABLED_SCRAPERS")
    else:
        logger.info("Bookmaker.eu: No credentials configured, skipping")

    data_source = CompositeSource(sources)
    source_count = len(sources)
    logger.info(f"SoldiAPI started with {source_count} data source(s)")

    # Start Playwright background prefetch tasks (only for enabled sources)
    if betonline is not None:
        betonline.start_prefetch()
    if bet105 is not None:
        bet105.start_prefetch()
    if dk is not None:
        dk.start_prefetch()
    if caesars is not None:
        caesars.start_prefetch()
    if bookmaker is not None:
        bookmaker.start_prefetch()
    if betus is not None:
        betus.start_prefetch()
    if stakeus is not None:
        stakeus.start_prefetch()

    # Initialize line history database
    line_history.init_db(settings.line_history_db)
    line_history.purge_old_snapshots(settings.line_history_retention_days)

    # Start continuous background refresh loop (keeps cache always warm)
    global _refresh_task
    _refresh_task = asyncio.create_task(_background_refresh_loop())
    logger.info("Background refresh task created: %s", _refresh_task)

    yield

    # Cancel background tasks
    if _refresh_task and not _refresh_task.done():
        _refresh_task.cancel()
    await data_source.close()
    logger.info("SoldiAPI shut down")


app = FastAPI(
    title="SoldiAPI",
    version="2.0.0",
    description="Odds aggregation API for SoldiOdds",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
    expose_headers=["x-requests-remaining", "x-requests-used"],
)

router = APIRouter(prefix="/api/v1")


def get_verified_api_key(
    query_key: Optional[str] = Depends(api_key_query),
    header_key: Optional[str] = Depends(api_key_header),
) -> str:
    return verify_api_key(settings.soldi_api_key, query_key, header_key)


@router.get("/sports/{sport_key}/odds")
async def get_odds(
    sport_key: str,
    background_tasks: BackgroundTasks,
    _api_key: str = Depends(get_verified_api_key),
    regions: str = Query(default="us"),
    markets: str = Query(default="h2h"),
    bookmakers: str = Query(default=""),
    oddsFormat: str = Query(default="american"),
    includeLinks: bool = Query(default=False),
):
    # The background refresh loop uses the standard key format.
    # Always serve from that cache regardless of what the frontend requested.
    bg_cache_key = f"{sport_key}:us:h2h::american"
    cached = cache.get(bg_cache_key)

    if cached is not None:
        events_data, cached_headers = cached
        response = JSONResponse(content=events_data)
        response.headers["x-requests-remaining"] = cached_headers.get(
            "x-requests-remaining", "cached"
        )
        return response

    # Cache miss — return empty instead of blocking on a full source fetch.
    # The background refresh loop will populate the cache shortly.
    logger.info(f"Cache miss for {sport_key} — returning empty (warming up)")
    return JSONResponse(content=[])


@router.get("/sports/{sport_key}/events/{event_id}/player-props")
async def get_player_props(
    sport_key: str,
    event_id: str,
    _api_key: str = Depends(get_verified_api_key),
):
    cache_key = f"props:{sport_key}:{event_id}"
    cached = cache.get(cache_key)
    if cached is not None:
        return JSONResponse(content=cached)

    props = await data_source.get_player_props(sport_key, event_id)
    props_data = [p.model_dump() for p in props]

    # Only cache non-empty results — empty results may be caused by
    # transient auth failures (e.g. Buckeye CF cookie expiry) and we
    # don't want to serve stale empties for the full TTL window.
    if props_data:
        cache.set(cache_key, props_data)
    logger.info(f"Fetched {len(props_data)} player props for {event_id}")
    return JSONResponse(content=props_data)


@router.get("/sports/{sport_key}/events/{event_id}/line-history")
async def get_event_line_history(
    sport_key: str,
    event_id: str,
    _api_key: str = Depends(get_verified_api_key),
    market: Optional[str] = Query(default=None),
    bookmaker: Optional[str] = Query(default=None),
):
    cache_key = f"line_history:{event_id}:{market}:{bookmaker}"
    cached = cache.get(cache_key)
    if cached is not None:
        return JSONResponse(content=cached)

    snapshots = await asyncio.to_thread(
        line_history.get_line_history,
        event_id=event_id,
        market_key=market,
        bookmaker_key=bookmaker,
    )

    # Short cache (15s) since line history changes less frequently
    cache.set(cache_key, snapshots, ttl=15)
    return JSONResponse(content=snapshots)


app.include_router(router)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "2.0.0",
        "sources": source_count if data_source else 0,
        "cache": cache.stats(),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )
