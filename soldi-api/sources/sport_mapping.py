"""
Central mapping between OddsScreen sport keys and external API identifiers.
Also provides odds conversion and team name normalization utilities.
"""

import re
import unicodedata
from typing import Optional, Tuple

# ── DraftKings event group IDs ──────────────────────────────────────────────
# Discovered via browser devtools on sportsbook.draftkings.com
# Format: OddsScreen sport_key -> DraftKings eventGroupId
DRAFTKINGS_EVENT_GROUPS = {
    "basketball_nba": 42648,
    "americanfootball_nfl": 88808,
    "icehockey_nhl": 42133,
    "baseball_mlb": 84240,
}

# ── FanDuel custom page IDs ─────────────────────────────────────────────────
# Used as customPageId param on sbapi.*.sportsbook.fanduel.com
FANDUEL_PAGE_IDS = {
    "basketball_nba": "nba",
    "americanfootball_nfl": "nfl",
    "icehockey_nhl": "nhl",
    "baseball_mlb": "mlb",
    "basketball_ncaab": "ncaab",
    "americanfootball_ncaaf": "ncaaf",
    # NOTE: soccer, ufc, tennis, boxing page IDs return 404 as of Feb 2026.
    # FanDuel's content-managed-page API only supports the above sport slugs.
}

# ── FanDuel sport-level event type IDs ────────────────────────────────────
# For sports that don't have customPageId, FanDuel uses page=SPORT&eventTypeId=
FANDUEL_EVENT_TYPE_IDS = {
    "mma_mixed_martial_arts": 26420387,
    "boxing_boxing": 6,
    "tennis_atp": 2,
    "tennis_wta": 2,
}

# ── ESPN sport/league paths ─────────────────────────────────────────────────
# Used as {sport}/{league} in ESPN API URLs
ESPN_SPORT_LEAGUES = {
    "basketball_nba": ("basketball", "nba"),
    "americanfootball_nfl": ("football", "nfl"),
    "icehockey_nhl": ("hockey", "nhl"),
    "baseball_mlb": ("baseball", "mlb"),
    "basketball_ncaab": ("basketball", "mens-college-basketball"),
    "americanfootball_ncaaf": ("football", "college-football"),
    "soccer_epl": ("soccer", "eng.1"),
    "soccer_spain_la_liga": ("soccer", "esp.1"),
    "soccer_germany_bundesliga": ("soccer", "ger.1"),
    "soccer_italy_serie_a": ("soccer", "ita.1"),
    "soccer_france_ligue_one": ("soccer", "fra.1"),
    "soccer_uefa_champs_league": ("soccer", "uefa.champions"),
    "mma_mixed_martial_arts": ("mma", "ufc"),
    "boxing_boxing": ("boxing", "boxing"),
    "tennis_atp": ("tennis", "atp"),
    "tennis_wta": ("tennis", "wta"),
}

# ── ESPN provider ID → sportsbook key mapping ───────────────────────────────
ESPN_PROVIDER_TO_BOOK = {
    38: "williamhill_us",   # Caesars
    40: "betmgm",
    41: "draftkings",
    45: "fanduel",
    58: "espnbet",          # ESPN BET
    100: "draftkings",      # DraftKings (current ID)
    # 200 = DraftKings Live Odds (skip, duplicate of 100)
    1001: "bet365",
    1003: "betmgm",         # alternate ID
}

# ── Pinnacle league IDs (Arcadia guest API) ──────────────────────────────────
# Discovered via https://guest.api.arcadia.pinnacle.com/0.1/leagues/{id}/matchups
PINNACLE_LEAGUE_IDS = {
    "basketball_nba": 487,
    "americanfootball_nfl": 889,
    "icehockey_nhl": 1456,
    "baseball_mlb": 246,
    "basketball_ncaab": 493,
    "soccer_epl": 1980,
    "soccer_spain_la_liga": 2196,
    "soccer_germany_bundesliga": 1842,
    "soccer_italy_serie_a": 2436,
    "soccer_france_ligue_one": 2036,
    "soccer_uefa_champs_league": 2627,
    "mma_mixed_martial_arts": 1624,
    "boxing_boxing": 197047,
}

# Pinnacle sport IDs for sports with many dynamic leagues (e.g. tennis
# has a separate league per tournament).  The scraper auto-discovers
# active leagues from the /sports/{id}/leagues endpoint.
PINNACLE_SPORT_IDS = {
    "tennis_atp": 33,   # Tennis — filter for ATP/Challenger
    "tennis_wta": 33,   # Tennis — filter for WTA
}

# ── Bovada sport paths ────────────────────────────────────────────────────────
# Used in bovada.lv/services/sports/event/coupon/events/A/description/{path}
BOVADA_SPORT_PATHS = {
    "basketball_nba": "basketball/nba",
    "basketball_ncaab": "basketball/college-basketball",
    "americanfootball_nfl": "football/nfl",
    "americanfootball_ncaaf": "football/college-football",
    "icehockey_nhl": "hockey/nhl",
    "baseball_mlb": "baseball/mlb",
    "soccer_epl": "soccer/europe/england/premier-league",
    "soccer_spain_la_liga": "soccer/europe/spain/la-liga",
    "soccer_germany_bundesliga": "soccer/europe/germany/1-bundesliga",
    "soccer_italy_serie_a": "soccer/europe/italy/serie-a",
    "soccer_france_ligue_one": "soccer/europe/france/ligue-1",
    "soccer_usa_mls": "soccer/north-america/united-states/mls",
    "soccer_uefa_champs_league": "soccer/international-club/uefa-champions-league",
    "tennis_atp": "tennis",
    "tennis_wta": "tennis",
    "mma_mixed_martial_arts": "ufc-mma",
    "boxing_boxing": "boxing",
}

# ── Kambi (BetRivers) sport paths ────────────────────────────────────────────
# Used in eu-offering-api.kambicdn.com/offering/v2018/rsiuspa/listView/{path}
KAMBI_SPORT_PATHS = {
    "basketball_nba": "basketball/nba",
    "americanfootball_nfl": "american_football/nfl",
    "americanfootball_ncaaf": "american_football/ncaaf",
    "icehockey_nhl": "ice_hockey/nhl",
    "baseball_mlb": "baseball/mlb",
    "basketball_ncaab": "basketball/ncaab",
    "soccer_epl": "football/england/premier_league",
    "soccer_spain_la_liga": "football/spain/la_liga",
    "soccer_germany_bundesliga": "football/germany/bundesliga",
    "soccer_italy_serie_a": "football/italy/serie_a",
    "soccer_france_ligue_one": "football/france/ligue_1",
    "soccer_usa_mls": "football/usa/mls",
    "soccer_uefa_champs_league": "football/champions_league",
    "mma_mixed_martial_arts": "ufc_mma",
    "boxing_boxing": "boxing",
    "tennis_atp": "tennis/atp",
    "tennis_wta": "tennis/wta",
}

# ── OddsScreen sport_key → display title ────────────────────────────────────
SPORT_TITLES = {
    "basketball_nba": "NBA",
    "americanfootball_nfl": "NFL",
    "icehockey_nhl": "NHL",
    "baseball_mlb": "MLB",
    "basketball_ncaab": "NCAAB",
    "americanfootball_ncaaf": "NCAAF",
    "soccer_epl": "English Premier League",
    "soccer_spain_la_liga": "La Liga",
    "soccer_germany_bundesliga": "Bundesliga",
    "soccer_italy_serie_a": "Serie A",
    "soccer_france_ligue_one": "Ligue 1",
    "soccer_uefa_champs_league": "UEFA Champions League",
    "mma_mixed_martial_arts": "UFC",
    "boxing_boxing": "Boxing",
    "tennis_atp": "ATP",
    "tennis_wta": "WTA",
}


def get_sport_title(sport_key: str) -> str:
    """Get display title for a sport key."""
    if sport_key in SPORT_TITLES:
        return SPORT_TITLES[sport_key]
    # Fallback: derive from key
    parts = sport_key.split("_", 1)
    return parts[-1].upper() if len(parts) > 1 else sport_key.upper()


def decimal_to_american(decimal_odds: float) -> int:
    """Convert decimal odds to American format."""
    if decimal_odds <= 1.0:
        return -10000  # edge case
    if decimal_odds >= 2.0:
        return round((decimal_odds - 1) * 100)
    else:
        return round(-100 / (decimal_odds - 1))


def american_to_decimal(american_odds: int) -> float:
    """Convert American odds to decimal format."""
    if american_odds > 0:
        return 1 + (american_odds / 100)
    else:
        return 1 + (100 / abs(american_odds))


def normalize_team_name(name: str) -> str:
    """
    Normalize team names for cross-source matching.
    'Los Angeles Lakers' -> 'losangeleslakers'
    'LA Lakers' -> 'lalakers'
    'Louisville (#21)' -> 'louisville'
    """
    # Strip ranking indicators like (#21), (21), #21
    name = re.sub(r"\s*\(?#?\d{1,3}\)?\s*$", "", name)
    name = re.sub(r"^\s*\(?#?\d{1,3}\)?\s+", "", name)
    # Remove accents
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    # Lowercase, strip punctuation and whitespace
    name = re.sub(r"[^a-z0-9]", "", name.lower())
    return name


# ── Known college mascot names (stripped during pre-normalization) ──
_COLLEGE_MASCOTS = frozenset({
    "49ers", "aces", "aggies", "anteaters", "aztecs", "badgers", "banana slugs",
    "beach", "bears", "bengals", "big green", "big red", "billikens", "bison",
    "black bears", "black knights", "blazers", "blue demons", "blue hens",
    "blue hose", "blue jays", "blue raiders", "bobcats", "boilermakers",
    "bonnies", "braves", "broncos", "bruins", "buckeyes", "bulldogs", "bulls",
    "cadets", "camels", "cardinals", "catamounts", "cavaliers", "chanticleers",
    "colonels", "colonials", "commodores", "cornhuskers", "cougars",
    "cowboys", "crimson tide", "crusaders", "cyclones", "dons", "dukes",
    "eagles", "explorers", "falcons", "fighting camels", "fighting hawks",
    "fighting illini", "fighting irish", "flames", "flyers", "friars", "gaels",
    "gamecocks", "gators", "golden bears", "golden eagles", "golden flashes",
    "golden grizzlies", "golden hurricane", "golden panthers", "governors",
    "great danes", "green wave", "greyhounds", "grizzlies", "hawks",
    "highlanders", "hilltoppers", "hokies", "hoosiers", "hornets", "hurricanes",
    "huskies", "jaguars", "jayhawks", "kangaroos", "knights",
    "lakers", "leopards", "lions", "lobos", "longhorns", "lumberjacks",
    "mastodons", "matadors", "mean green", "midshipmen", "miners",
    "minutemen", "monarchs", "mountain hawks", "mountaineers", "musketeers",
    "nittany lions", "norse", "ospreys", "owls", "paladins", "panthers",
    "patriots", "peacocks", "pelicans", "penguins", "phoenixes", "pilots",
    "pioneers", "pirates", "pride", "purple aces", "purple eagles", "quakers",
    "racers", "raiders", "ramblers", "rams", "rattlers", "razorbacks",
    "red foxes", "red raiders", "red storm", "redhawks", "retrievers",
    "river hawks", "roadrunners", "rockets", "roos", "royals",
    "running rebels", "salukis", "seahawks", "seawolves", "seminoles",
    "shockers", "skyhawks", "sooners", "spartans", "spiders", "stags",
    "terrapins", "terriers", "texans", "thundering herd", "tigers", "titans",
    "toreros", "trojans", "tribe", "tritons", "utes", "vandals", "vikings",
    "volunteers", "vulcans", "warriors", "wasps", "waves", "wildcats",
    "wolf pack", "wolfpack", "wolverines", "wolves", "warhawks",
    "yellow jackets", "zips",
})


def _pre_normalize_name(name: str) -> str:
    """Pre-normalize names before alias lookup.

    Handles common patterns that prevent alias matches:
    - 'St.' / 'St ' → 'Saint ' (for college names)
    - Strips 'University' / 'College' suffixes
    - Strips known college mascot names
    - Strips MMA suffixes like 'Jr.', 'Jr', 'III', 'II'
    """
    # Normalize St. / St → Saint (only before a word, not mid-word)
    name = re.sub(r"\bSt\.?\s+", "Saint ", name, flags=re.IGNORECASE)
    # Strip MMA generational suffixes
    name = re.sub(r"\s+(?:Jr\.?|Sr\.?|III|II|IV)$", "", name, flags=re.IGNORECASE)
    # Strip known mascot names at end of team name
    lower = name.lower().strip()
    for mascot in sorted(_COLLEGE_MASCOTS, key=len, reverse=True):
        if lower.endswith(" " + mascot):
            candidate = name[:-(len(mascot))].rstrip()
            if len(candidate) >= 2:  # Don't strip if nothing left
                name = candidate
                break
    return name.strip()


def canonical_event_id(sport_key: str, home: str, away: str, commence_date: str) -> str:
    """
    Generate a stable event ID for matching events across sources.
    Teams are sorted alphabetically so order doesn't matter.
    Date is normalised to US-Eastern so that the same game reported in
    UTC vs ET doesn't produce two different IDs.
    Aliases are resolved before normalization so variant team names converge.
    """
    home_resolved = resolve_team_name(home, sport_key=sport_key)
    away_resolved = resolve_team_name(away, sport_key=sport_key)
    teams = sorted([normalize_team_name(home_resolved), normalize_team_name(away_resolved)])
    date = _normalize_date_to_et(commence_date)
    return f"{sport_key}:{teams[0]}:{teams[1]}:{date}"


def _truncate_fractional_seconds(raw: str) -> str:
    """Truncate fractional seconds to at most 6 digits (Python 3.9 limit).

    DraftKings returns 7-digit fractional seconds like '2026-03-01T00:30:00.0000000Z'
    which Python 3.9's datetime.fromisoformat() cannot parse (max 6 digits).
    """
    m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)(.*)", raw)
    if m:
        base, frac, rest = m.groups()
        frac = frac[:6]  # Truncate to microsecond precision
        return f"{base}.{frac}{rest}"
    return raw


def _normalize_date_to_et(commence_date: str) -> str:
    """Convert an ISO-8601 datetime string to a YYYY-MM-DD date in US/Eastern."""
    from datetime import datetime, timezone, timedelta
    try:
        raw = _truncate_fractional_seconds(commence_date.strip())
        # Try parsing ISO format (handles Z and +HH:MM offsets)
        if raw.endswith("Z"):
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        elif "+" in raw[10:] or raw.count("-") > 2:
            dt = datetime.fromisoformat(raw)
        else:
            # Bare date or no timezone — treat as UTC
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        # Convert to US Eastern (UTC-5 standard, UTC-4 DST)
        # Simple approximation: EST = UTC-5 (good enough for date bucketing)
        eastern = dt.astimezone(timezone(timedelta(hours=-5)))
        return eastern.strftime("%Y-%m-%d")
    except Exception:
        return commence_date[:10]


# ── Common team name aliases for cross-source matching ──────────────────────
# Maps variant names to canonical names
TEAM_ALIASES = {
    # ── NBA (Kambi abbreviations + common variants) ──
    "atl hawks": "Atlanta Hawks",
    "bos celtics": "Boston Celtics",
    "bkn nets": "Brooklyn Nets",
    "cha hornets": "Charlotte Hornets",
    "chi bulls": "Chicago Bulls",
    "cle cavaliers": "Cleveland Cavaliers",
    "dal mavericks": "Dallas Mavericks",
    "den nuggets": "Denver Nuggets",
    "det pistons": "Detroit Pistons",
    "gs warriors": "Golden State Warriors",
    "golden st warriors": "Golden State Warriors",
    "hou rockets": "Houston Rockets",
    "ind pacers": "Indiana Pacers",
    "la clippers": "Los Angeles Clippers",
    "l.a. clippers": "Los Angeles Clippers",
    "la lakers": "Los Angeles Lakers",
    "l.a. lakers": "Los Angeles Lakers",
    "mem grizzlies": "Memphis Grizzlies",
    "mia heat": "Miami Heat",
    "mil bucks": "Milwaukee Bucks",
    "min timberwolves": "Minnesota Timberwolves",
    "no pelicans": "New Orleans Pelicans",
    "ny knicks": "New York Knicks",
    "okc thunder": "Oklahoma City Thunder",
    "orl magic": "Orlando Magic",
    "phi 76ers": "Philadelphia 76ers",
    "pho suns": "Phoenix Suns",
    "por trail blazers": "Portland Trail Blazers",
    "sac kings": "Sacramento Kings",
    "sa spurs": "San Antonio Spurs",
    "tor raptors": "Toronto Raptors",
    "uta jazz": "Utah Jazz",
    "was wizards": "Washington Wizards",
    # ── NFL (Kambi abbreviations + common variants) ──
    "ari cardinals": "Arizona Cardinals",
    "atl falcons": "Atlanta Falcons",
    "bal ravens": "Baltimore Ravens",
    "buf bills": "Buffalo Bills",
    "car panthers": "Carolina Panthers",
    "chi bears": "Chicago Bears",
    "cin bengals": "Cincinnati Bengals",
    "cle browns": "Cleveland Browns",
    "dal cowboys": "Dallas Cowboys",
    "den broncos": "Denver Broncos",
    "det lions": "Detroit Lions",
    "gb packers": "Green Bay Packers",
    "hou texans": "Houston Texans",
    "ind colts": "Indianapolis Colts",
    "jac jaguars": "Jacksonville Jaguars",
    "kc chiefs": "Kansas City Chiefs",
    "lv raiders": "Las Vegas Raiders",
    "la chargers": "Los Angeles Chargers",
    "la rams": "Los Angeles Rams",
    "mia dolphins": "Miami Dolphins",
    "min vikings": "Minnesota Vikings",
    "ne patriots": "New England Patriots",
    "no saints": "New Orleans Saints",
    "ny giants": "New York Giants",
    "ny jets": "New York Jets",
    "phi eagles": "Philadelphia Eagles",
    "pit steelers": "Pittsburgh Steelers",
    "sf 49ers": "San Francisco 49ers",
    "sea seahawks": "Seattle Seahawks",
    "tb buccaneers": "Tampa Bay Buccaneers",
    "ten titans": "Tennessee Titans",
    "was commanders": "Washington Commanders",
    # ── NHL (Kambi abbreviations + common variants) ──
    "ana ducks": "Anaheim Ducks",
    "bos bruins": "Boston Bruins",
    "buf sabres": "Buffalo Sabres",
    "cgy flames": "Calgary Flames",
    "car hurricanes": "Carolina Hurricanes",
    "chi blackhawks": "Chicago Blackhawks",
    "col avalanche": "Colorado Avalanche",
    "cbj blue jackets": "Columbus Blue Jackets",
    "dal stars": "Dallas Stars",
    "det red wings": "Detroit Red Wings",
    "edm oilers": "Edmonton Oilers",
    "fla panthers": "Florida Panthers",
    "la kings": "Los Angeles Kings",
    "min wild": "Minnesota Wild",
    "mtl canadiens": "Montreal Canadiens",
    "nsh predators": "Nashville Predators",
    "nj devils": "New Jersey Devils",
    "ny islanders": "New York Islanders",
    "ny rangers": "New York Rangers",
    "ott senators": "Ottawa Senators",
    "phi flyers": "Philadelphia Flyers",
    "pit penguins": "Pittsburgh Penguins",
    "sj sharks": "San Jose Sharks",
    "sea kraken": "Seattle Kraken",
    "stl blues": "St. Louis Blues",
    "tb lightning": "Tampa Bay Lightning",
    "tor maple leafs": "Toronto Maple Leafs",
    "van canucks": "Vancouver Canucks",
    "vgs golden knights": "Vegas Golden Knights",
    "wsh capitals": "Washington Capitals",
    "wpg jets": "Winnipeg Jets",
    "uta mammoth": "Utah Hockey Club",
    # ── MLB (Kambi abbreviations) ──
    "ari diamondbacks": "Arizona Diamondbacks",
    "atl braves": "Atlanta Braves",
    "bal orioles": "Baltimore Orioles",
    "bos red sox": "Boston Red Sox",
    "chi cubs": "Chicago Cubs",
    "chi white sox": "Chicago White Sox",
    "cin reds": "Cincinnati Reds",
    "cle guardians": "Cleveland Guardians",
    "col rockies": "Colorado Rockies",
    "det tigers": "Detroit Tigers",
    "hou astros": "Houston Astros",
    "kc royals": "Kansas City Royals",
    "la angels": "Los Angeles Angels",
    "la dodgers": "Los Angeles Dodgers",
    "mia marlins": "Miami Marlins",
    "mil brewers": "Milwaukee Brewers",
    "min twins": "Minnesota Twins",
    "ny mets": "New York Mets",
    "ny yankees": "New York Yankees",
    "oak athletics": "Oakland Athletics",
    "phi phillies": "Philadelphia Phillies",
    "pit pirates": "Pittsburgh Pirates",
    "sd padres": "San Diego Padres",
    "sf giants": "San Francisco Giants",
    "sea mariners": "Seattle Mariners",
    "stl cardinals": "St. Louis Cardinals",
    "tb rays": "Tampa Bay Rays",
    "tex rangers": "Texas Rangers",
    "tor blue jays": "Toronto Blue Jays",
    "was nationals": "Washington Nationals",
    # ── College teams (common abbreviation / name variants) ──
    # Southland / smaller conferences
    "texas a&m-cc": "Texas A&M Corpus Christi",
    "texas a&m corpus": "Texas A&M Corpus Christi",
    "texas a&m corpus christi": "Texas A&M Corpus Christi",
    "tamu-cc": "Texas A&M Corpus Christi",
    "southeastern louisiana": "SE Louisiana",
    "se louisiana": "SE Louisiana",
    "grambling state": "Grambling",
    "grambling": "Grambling",
    "mississippi valley state": "Mississippi Valley State",
    "mississippi val. st.": "Mississippi Valley State",
    "miss valley st": "Mississippi Valley State",
    "miss valley state": "Mississippi Valley State",
    "stephen f. austin": "Stephen F. Austin",
    "stephen f austin": "Stephen F. Austin",
    "sfa": "Stephen F. Austin",
    "ut rio grande valley": "UT Rio Grande Valley",
    "utrgv": "UT Rio Grande Valley",
    "mcneese state": "McNeese State",
    "mcneese": "McNeese State",
    "nicholls": "Nicholls State",
    "nicholls colonels": "Nicholls State",
    "northwestern state": "Northwestern State",
    "northwestern st": "Northwestern State",
    "northwestern st.": "Northwestern State",
    "nw state": "Northwestern State",
    "nw state demons": "Northwestern State",
    "houston christian": "Houston Christian",
    "houston chr": "Houston Christian",
    "east texas a&m": "East Texas A&M",
    "etamu": "East Texas A&M",
    "incarnate word": "Incarnate Word",
    "uiw": "Incarnate Word",
    "new orleans": "New Orleans",
    "uno": "New Orleans",
    # FanDuel / Buckeye / Kalshi abbreviated names → canonical
    "gw revolutionaries": "George Washington",
    "george washington revolutionaries": "George Washington",
    "george washington": "George Washington",
    "geo washington": "George Washington",
    "geo. washington": "George Washington",
    "wv mountaineers": "West Virginia",
    "west virginia mountaineers": "West Virginia",
    "san jose st": "San Jose State",
    "san jose st.": "San Jose State",
    "san jose state spartans": "San Jose State",
    "miami (oh)": "Miami Ohio",
    "miami (ohio)": "Miami Ohio",
    "miami oh": "Miami Ohio",
    "miami ohio": "Miami Ohio",
    "miami redhawks": "Miami Ohio",
    "miami oh redhawks": "Miami Ohio",
    # Miami (FL) variants
    "miami florida": "Miami",
    "miami (fl)": "Miami",
    "miami hurricanes": "Miami",
    # St / State abbreviation variants (Buckeye/Kalshi style)
    "florida st": "Florida State",
    "florida st.": "Florida State",
    "florida state seminoles": "Florida State",
    "morgan st.": "Morgan State",
    "morgan st": "Morgan State",
    "morgan state": "Morgan State",
    "morgan state bears": "Morgan State",
    "south carolina st.": "South Carolina State",
    "south carolina st": "South Carolina State",
    "south carolina state": "South Carolina State",
    "south carolina state bulldogs": "South Carolina State",
    "north carolina st.": "North Carolina State",
    "north carolina st": "North Carolina State",
    "morehead st.": "Morehead State",
    "morehead st": "Morehead State",
    "morehead state": "Morehead State",
    "morehead state eagles": "Morehead State",
    "georgia st.": "Georgia State",
    "georgia st": "Georgia State",
    "georgia state": "Georgia State",
    "georgia state panthers": "Georgia State",
    "arkansas st.": "Arkansas State",
    "arkansas st": "Arkansas State",
    "arkansas state": "Arkansas State",
    "arkansas state red wolves": "Arkansas State",
    "east tennessee st.": "East Tennessee State",
    "east tennessee st": "East Tennessee State",
    "east tennessee state": "East Tennessee State",
    "east tennessee state buccaneers": "East Tennessee State",
    "etsu": "East Tennessee State",
    "wright st.": "Wright State",
    "wright st": "Wright State",
    "wright state": "Wright State",
    "wright state raiders": "Wright State",
    "cleveland st.": "Cleveland State",
    "cleveland st": "Cleveland State",
    "cleveland state": "Cleveland State",
    "cleveland state vikings": "Cleveland State",
    "illinois st.": "Illinois State",
    "illinois st": "Illinois State",
    "illinois state": "Illinois State",
    "illinois state redbirds": "Illinois State",
    "indiana st.": "Indiana State",
    "indiana st": "Indiana State",
    "indiana state": "Indiana State",
    "indiana state sycamores": "Indiana State",
    "youngstown st.": "Youngstown State",
    "youngstown st": "Youngstown State",
    "youngstown state": "Youngstown State",
    "youngstown state penguins": "Youngstown State",
    "ohio st.": "Ohio State",  # ensure coverage
    "mississippi st.": "Mississippi State",
    "oregon st.": "Oregon State",
    "san diego st.": "San Diego State",
    "san diego st": "San Diego State",
    "washington st.": "Washington State",
    "utah st.": "Utah State",
    "kansas st.": "Kansas State",
    # Southern Miss / Southern Mississippi
    "southern miss": "Southern Mississippi",
    "southern mississippi": "Southern Mississippi",
    "southern miss golden eagles": "Southern Mississippi",
    "southern mississippi golden eagles": "Southern Mississippi",
    # UIC / Illinois-Chicago
    "uic": "Illinois-Chicago",
    "illinois chicago": "Illinois-Chicago",
    "illinois-chicago": "Illinois-Chicago",
    "illinois-chicago flames": "Illinois-Chicago",
    # Saint / St variants
    "st louis": "Saint Louis",
    "st. louis": "Saint Louis",
    "saint louis": "Saint Louis",
    "saint louis billikens": "Saint Louis",
    "st louis billikens": "Saint Louis",
    "st. francis (pa)": "Saint Francis",
    "st francis (pa)": "Saint Francis",
    "saint francis (pa)": "Saint Francis",
    "saint francis": "Saint Francis",
    "st. francis": "Saint Francis",
    "st. francis pa": "Saint Francis",
    "st francis pa": "Saint Francis",
    "saint francis pa": "Saint Francis",
    # La Salle variants
    "la salle": "La Salle",
    "la salle explorers": "La Salle",
    # Louisiana-Lafayette / UL Lafayette
    "ul - lafayette": "Louisiana-Lafayette",
    "ul-lafayette": "Louisiana-Lafayette",
    "ul lafayette": "Louisiana-Lafayette",
    "louisiana-lafayette": "Louisiana-Lafayette",
    "louisiana lafayette": "Louisiana-Lafayette",
    "louisiana ragin' cajuns": "Louisiana-Lafayette",
    "louisiana": "Louisiana-Lafayette",
    "louisiana cajuns": "Louisiana-Lafayette",
    # Louisiana-Monroe
    "louisiana-monroe": "Louisiana-Monroe",
    "louisiana-monroe warhawks": "Louisiana-Monroe",
    "ul monroe": "Louisiana-Monroe",
    "ulm": "Louisiana-Monroe",
    # Polymarket truncated / mascot names
    "marshall thundering": "Marshall",
    "marshall thundering herd": "Marshall",
    "bowling": "Bowling Green",
    "bowling green": "Bowling Green",
    "bowling green falcons": "Bowling Green",
    "kent state golden flashes": "Kent State",
    "kent state": "Kent State",
    "boston": "Boston College",
    "boston college": "Boston College",
    "boston college eagles": "Boston College",
    "boston university": "Boston University",
    "boston university terriers": "Boston University",
    "queens (nc) royals": "Queens University",
    "queens university": "Queens University",
    "queens nc": "Queens University",
    "detroit mercy": "Detroit Mercy",
    "detroit titans": "Detroit Mercy",
    "detroit mercy titans": "Detroit Mercy",
    "robert morris": "Robert Morris",
    "robert morris colonials": "Robert Morris",
    "purdue fort wayne": "Purdue Fort Wayne",
    "purdue fort wayne mastodons": "Purdue Fort Wayne",
    "northern kentucky": "Northern Kentucky",
    "northern kentucky norse": "Northern Kentucky",
    "holy cross": "Holy Cross",
    "holy cross crusaders": "Holy Cross",
    "lehigh mountain": "Lehigh",
    "lehigh": "Lehigh",
    "lehigh mountain hawks": "Lehigh",
    "lipscomb bisons": "Lipscomb",
    "lipscomb": "Lipscomb",
    "south dakota coyotes": "South Dakota",
    "south dakota": "South Dakota",
    "omaha": "Omaha",
    "omaha mavericks": "Omaha",
    "tulsa golden hurricane": "Tulsa",
    "tulsa": "Tulsa",
    "valparaiso beacons": "Valparaiso",
    "valparaiso": "Valparaiso",
    # IU Indy / IUPUI
    "iu indy": "IU Indianapolis",
    "iu indianapolis": "IU Indianapolis",
    "iupui": "IU Indianapolis",
    # New Haven (DII but some books carry it)
    "new haven": "New Haven",
    "new haven chargers": "New Haven",
    # Stonehill
    "stonehill": "Stonehill",
    "stonehill skyhawks": "Stonehill",
    # Various Polymarket long names
    "maine black": "Maine",
    "maine black bears": "Maine",
    "maine": "Maine",
    "albany great danes": "Albany",
    "albany": "Albany",
    "delaware fightin' blue hens": "Delaware",
    "delaware": "Delaware",
    "stony brook seawolves": "Stony Brook",
    "stony brook": "Stony Brook",
    "massachusetts-lowell river": "UMass Lowell",
    "umass lowell": "UMass Lowell",
    "massachusetts lowell": "UMass Lowell",
    "mass lowell": "UMass Lowell",
    "umass lowell river hawks": "UMass Lowell",
    "campbell": "Campbell",
    "campbell fighting camels": "Campbell",
    "drexel dragons": "Drexel",
    "drexel": "Drexel",
    "uncg": "UNC Greensboro",
    "unc greensboro": "UNC Greensboro",
    "chattanooga mocs": "Chattanooga",
    "chattanooga": "Chattanooga",
    "chicago state": "Chicago State",
    "liu sharks": "LIU",
    "liu": "LIU",
    "le moyne": "Le Moyne",
    "fairleigh dickinson": "Fairleigh Dickinson",
    "charleston southern buccaneers": "Charleston Southern",
    "charleston southern": "Charleston Southern",
    "vmi keydets": "VMI",
    "vmi": "VMI",
    "south dakota state jackrabbits": "South Dakota State",
    "portland state vikings": "Portland State",
    "portland state": "Portland State",
    "idaho vandals": "Idaho",
    "idaho": "Idaho",
    "sam houston bearkats": "Sam Houston",
    "sam houston": "Sam Houston",
    "florida international": "FIU",
    "fiu": "FIU",
    "presbyterian blue hose": "Presbyterian",
    "presbyterian": "Presbyterian",
    "california-san diego": "UC San Diego",
    "uc san diego": "UC San Diego",
    "bakersfield": "Cal State Bakersfield",
    "cal state bakersfield": "Cal State Bakersfield",
    "south alabama": "South Alabama",
    "south alabama jaguars": "South Alabama",
    "north alabama": "North Alabama",
    "bellarmine": "Bellarmine",
    "bellarmine knights": "Bellarmine",
    "central connecticut state": "Central Connecticut",
    "central connecticut": "Central Connecticut",
    "mercyhurst": "Mercyhurst",
    "mercyhurst lakers": "Mercyhurst",
    "winthrop": "Winthrop",
    "usc upstate": "USC Upstate",
    "high point": "High Point",
    "oakland": "Oakland",
    "grand canyon": "Grand Canyon",
    "grand canyon antelopes": "Grand Canyon",
    "pepperdine": "Pepperdine",
    "pepperdine waves": "Pepperdine",
    "seattle": "Seattle",
    "seattle redhawks": "Seattle",
    "kansas city": "Kansas City",
    "kansas city roos": "Kansas City",
    # Common state abbreviations
    "penn st": "Penn State",
    "penn st.": "Penn State",
    "penn state nittany lions": "Penn State",
    "ohio st": "Ohio State",
    "ohio st.": "Ohio State",
    "ohio state buckeyes": "Ohio State",
    "michigan st": "Michigan State",
    "michigan st.": "Michigan State",
    "mich state": "Michigan State",
    "mich st": "Michigan State",
    "oregon st": "Oregon State",
    "oregon st.": "Oregon State",
    "oklahoma st": "Oklahoma State",
    "oklahoma st.": "Oklahoma State",
    "ok state": "Oklahoma State",
    "colorado st": "Colorado State",
    "colorado st.": "Colorado State",
    "arizona st": "Arizona State",
    "arizona st.": "Arizona State",
    "asu": "Arizona State",
    "boise st": "Boise State",
    "boise st.": "Boise State",
    "fresno st": "Fresno State",
    "fresno st.": "Fresno State",
    "iowa st": "Iowa State",
    "iowa st.": "Iowa State",
    "kansas st": "Kansas State",
    "kansas st.": "Kansas State",
    "k-state": "Kansas State",
    "mississippi st": "Mississippi State",
    "mississippi st.": "Mississippi State",
    "miss state": "Mississippi State",
    "miss st": "Mississippi State",
    "nc state": "North Carolina State",
    "n.c. state": "North Carolina State",
    "north carolina state": "North Carolina State",
    "nc state wolfpack": "North Carolina State",
    "washington st": "Washington State",
    "washington st.": "Washington State",
    "wazzu": "Washington State",
    "ball st": "Ball State",
    "ball st.": "Ball State",
    "boise state broncos": "Boise State",
    "utah st": "Utah State",
    "utah st.": "Utah State",
    "kent st": "Kent State",
    "kent st.": "Kent State",
    # Common school name variants
    "uconn": "Connecticut",
    "uconn huskies": "Connecticut",
    "connecticut huskies": "Connecticut",
    "ole miss": "Mississippi",
    "ole miss rebels": "Mississippi",
    "umass": "Massachusetts",
    "u mass": "Massachusetts",
    "massachusetts minutemen": "Massachusetts",
    "pitt": "Pittsburgh",
    "pitt panthers": "Pittsburgh",
    "pittsburgh panthers": "Pittsburgh",
    "smu": "SMU",
    "smu mustangs": "SMU",
    "southern methodist": "SMU",
    "tcu": "TCU",
    "tcu horned frogs": "TCU",
    "ucf": "UCF",
    "ucf knights": "UCF",
    "central florida": "UCF",
    "usc": "USC",
    "usc trojans": "USC",
    "southern california": "USC",
    "southern cal": "USC",
    "lsu": "LSU",
    "lsu tigers": "LSU",
    "louisiana state": "LSU",
    "vcu": "VCU",
    "virginia commonwealth": "VCU",
    "va commonwealth": "VCU",
    "vcu rams": "VCU",
    "unlv": "UNLV",
    "unlv rebels": "UNLV",
    "utep": "UTEP",
    "ut el paso": "UTEP",
    "utsa": "UTSA",
    "ut san antonio": "UTSA",
    # Directional / regional school variants
    "n carolina": "North Carolina",
    "unc": "North Carolina",
    "unc tar heels": "North Carolina",
    "s carolina": "South Carolina",
    "n dakota st": "North Dakota State",
    "n dakota state": "North Dakota State",
    "north dakota st": "North Dakota State",
    "s dakota st": "South Dakota State",
    "s dakota state": "South Dakota State",
    "south dakota st": "South Dakota State",
    "e michigan": "Eastern Michigan",
    "eastern michigan": "Eastern Michigan",
    "e mich": "Eastern Michigan",
    "w michigan": "Western Michigan",
    "western michigan": "Western Michigan",
    "w mich": "Western Michigan",
    "n illinois": "Northern Illinois",
    "northern illinois": "Northern Illinois",
    "niu": "Northern Illinois",
    "c michigan": "Central Michigan",
    "central michigan": "Central Michigan",
    "s illinois": "Southern Illinois",
    "southern illinois": "Southern Illinois",
    "siu": "Southern Illinois",
    "e carolina": "East Carolina",
    "east carolina": "East Carolina",
    "ecu": "East Carolina",
    "w virginia": "West Virginia",
    "w. virginia": "West Virginia",
    "wvu": "West Virginia",
    "n iowa": "Northern Iowa",
    "northern iowa": "Northern Iowa",
    "uni": "Northern Iowa",
    "s florida": "South Florida",
    "south florida": "South Florida",
    "usf": "South Florida",
    "c florida": "Central Florida",
    # Additional common variants
    "st. john's": "St Johns",
    "saint john's": "St Johns",
    "st john's": "St Johns",
    "st johns": "St Johns",
    "st. joseph's": "Saint Josephs",
    "saint joseph's": "Saint Josephs",
    "st josephs": "Saint Josephs",
    "st joseph's": "Saint Josephs",
    "st. josephs": "Saint Josephs",
    "saint josephs": "Saint Josephs",
    "st. bonaventure": "St Bonaventure",
    "saint bonaventure": "St Bonaventure",
    "st bonaventure": "St Bonaventure",
    "st. peter's": "Saint Peters",
    "saint peter's": "Saint Peters",
    "st peters": "Saint Peters",
    "st peter's": "Saint Peters",
    "loyola chicago": "Loyola Chicago",
    "loyola-chicago": "Loyola Chicago",
    "loyola (chi)": "Loyola Chicago",
    "loyola marymount": "Loyola Marymount",
    "loyola (md)": "Loyola Maryland",
    "loyola maryland": "Loyola Maryland",
    # Utah HC / expansion
    "utah hockey club": "Utah Hockey Club",
    "utah hc": "Utah Hockey Club",
    # ── Polymarket full mascot names → canonical school names ──
    # These ensure Polymarket's "School Mascot" names resolve to the same
    # canonical names used by FanDuel, ESPN, Pinnacle, etc.
    "louisville cardinals": "Louisville",
    "north carolina tar heels": "North Carolina",
    "duke blue devils": "Duke",
    "kentucky wildcats": "Kentucky",
    "kansas jayhawks": "Kansas",
    "gonzaga bulldogs": "Gonzaga",
    "houston cougars": "Houston",
    "purdue boilermakers": "Purdue",
    "tennessee volunteers": "Tennessee",
    "alabama crimson tide": "Alabama",
    "auburn tigers": "Auburn",
    "florida gators": "Florida",
    "michigan wolverines": "Michigan",
    "michigan state spartans": "Michigan State",
    "ohio state buckeyes": "Ohio State",
    "indiana hoosiers": "Indiana",
    "iowa hawkeyes": "Iowa",
    "iowa state cyclones": "Iowa State",
    "illinois fighting illini": "Illinois",
    "wisconsin badgers": "Wisconsin",
    "marquette golden eagles": "Marquette",
    "creighton bluejays": "Creighton",
    "villanova wildcats": "Villanova",
    "xavier musketeers": "Xavier",
    "uconn huskies": "Connecticut",
    "connecticut huskies": "Connecticut",
    "arizona wildcats": "Arizona",
    "arizona state sun devils": "Arizona State",
    "baylor bears": "Baylor",
    "texas tech red raiders": "Texas Tech",
    "texas longhorns": "Texas",
    "oklahoma sooners": "Oklahoma",
    "oklahoma state cowboys": "Oklahoma State",
    "arkansas razorbacks": "Arkansas",
    "oregon ducks": "Oregon",
    "colorado buffaloes": "Colorado",
    "utah utes": "Utah",
    "cincinnati bearcats": "Cincinnati",
    "memphis tigers": "Memphis",
    "st johns red storm": "St Johns",
    "st. john's red storm": "St Johns",
    "seton hall pirates": "Seton Hall",
    "providence friars": "Providence",
    "georgetown hoyas": "Georgetown",
    "butler bulldogs": "Butler",
    "dayton flyers": "Dayton",
    "san diego state aztecs": "San Diego State",
    "nevada wolf pack": "Nevada",
    "boise state broncos": "Boise State",
    "new mexico lobos": "New Mexico",
    "clemson tigers": "Clemson",
    "virginia cavaliers": "Virginia",
    "virginia tech hokies": "Virginia Tech",
    "wake forest demon deacons": "Wake Forest",
    "nc state wolfpack": "North Carolina State",
    "syracuse orange": "Syracuse",
    "notre dame fighting irish": "Notre Dame",
    "pittsburgh panthers": "Pittsburgh",
    "miami hurricanes": "Miami",
    "florida state seminoles": "Florida State",
    "georgia bulldogs": "Georgia",
    "georgia tech yellow jackets": "Georgia Tech",
    "south carolina gamecocks": "South Carolina",
    "vanderbilt commodores": "Vanderbilt",
    "mississippi state bulldogs": "Mississippi State",
    "ole miss rebels": "Mississippi",
    "lsu tigers": "LSU",
    "texas a&m aggies": "Texas A&M",
    "missouri tigers": "Missouri",
    "minnesota golden gophers": "Minnesota",
    "northwestern wildcats": "Northwestern",
    "nebraska cornhuskers": "Nebraska",
    "penn state nittany lions": "Penn State",
    "maryland terrapins": "Maryland",
    "rutgers scarlet knights": "Rutgers",
    "michigan state spartans": "Michigan State",
    "usc trojans": "USC",
    "ucla bruins": "UCLA",
    "washington huskies": "Washington",
    "stanford cardinal": "Stanford",
    "cal golden bears": "California",
    "california golden bears": "California",
    "kansas state wildcats": "Kansas State",
    "west virginia mountaineers": "West Virginia",
    "tcu horned frogs": "TCU",
    "smu mustangs": "SMU",
    "ucf knights": "UCF",
    "byu cougars": "BYU",
    "colorado state rams": "Colorado State",
    "san jose state spartans": "San Jose State",
    "fresno state bulldogs": "Fresno State",
    "unlv rebels": "UNLV",
    "wyoming cowboys": "Wyoming",
    "hawaii rainbow warriors": "Hawaii",
    "air force falcons": "Air Force",
    "army black knights": "Army",
    "navy midshipmen": "Navy",
    # ── Polymarket NBA nicknames (no city prefix) ──
    "hawks": "Atlanta Hawks",
    "celtics": "Boston Celtics",
    "nets": "Brooklyn Nets",
    "hornets": "Charlotte Hornets",
    "bulls": "Chicago Bulls",
    "cavaliers": "Cleveland Cavaliers",
    "mavericks": "Dallas Mavericks",
    "nuggets": "Denver Nuggets",
    "pistons": "Detroit Pistons",
    "warriors": "Golden State Warriors",
    "rockets": "Houston Rockets",
    "pacers": "Indiana Pacers",
    "clippers": "Los Angeles Clippers",
    "la clippers": "Los Angeles Clippers",
    "lakers": "Los Angeles Lakers",
    "grizzlies": "Memphis Grizzlies",
    "heat": "Miami Heat",
    "bucks": "Milwaukee Bucks",
    "timberwolves": "Minnesota Timberwolves",
    "pelicans": "New Orleans Pelicans",
    "knicks": "New York Knicks",
    "thunder": "Oklahoma City Thunder",
    "magic": "Orlando Magic",
    "76ers": "Philadelphia 76ers",
    "suns": "Phoenix Suns",
    "trail blazers": "Portland Trail Blazers",
    "blazers": "Portland Trail Blazers",
    # "kings" removed — ambiguous across NBA/NHL; use SPORT_TEAM_ALIASES
    "spurs": "San Antonio Spurs",
    "raptors": "Toronto Raptors",
    "jazz": "Utah Jazz",
    "wizards": "Washington Wizards",
    # ── Polymarket NHL nicknames (no city prefix) ──
    "golden knights": "Vegas Golden Knights",
    "ducks": "Anaheim Ducks",
    "coyotes": "Arizona Coyotes",
    "bruins": "Boston Bruins",
    "sabres": "Buffalo Sabres",
    "flames": "Calgary Flames",
    "hurricanes": "Carolina Hurricanes",
    "blackhawks": "Chicago Blackhawks",
    "avalanche": "Colorado Avalanche",
    "blue jackets": "Columbus Blue Jackets",
    "stars": "Dallas Stars",
    "red wings": "Detroit Red Wings",
    "oilers": "Edmonton Oilers",
    "panthers": "Florida Panthers",
    "canadiens": "Montreal Canadiens",
    "predators": "Nashville Predators",
    "devils": "New Jersey Devils",
    "islanders": "New York Islanders",
    "rangers": "New York Rangers",
    "senators": "Ottawa Senators",
    "flyers": "Philadelphia Flyers",
    "penguins": "Pittsburgh Penguins",
    "sharks": "San Jose Sharks",
    "kraken": "Seattle Kraken",
    "blues": "St. Louis Blues",
    "lightning": "Tampa Bay Lightning",
    "maple leafs": "Toronto Maple Leafs",
    "canucks": "Vancouver Canucks",
    "capitals": "Washington Capitals",
    "jets": "Winnipeg Jets",
    # ── Additional nickname/abbreviation aliases ──
    "knights": "Vegas Golden Knights",
    "vgk": "Vegas Golden Knights",
    "vegas": "Vegas Golden Knights",
    "vegas knights": "Vegas Golden Knights",
    "la lakers": "Los Angeles Lakers",
    "la clippers": "Los Angeles Clippers",
    "la kings": "Los Angeles Kings",
    "la chargers": "Los Angeles Chargers",
    "la rams": "Los Angeles Rams",
    "la angels": "Los Angeles Angels",
    "la dodgers": "Los Angeles Dodgers",
    "la galaxy": "Los Angeles Galaxy",
    "ny knicks": "New York Knicks",
    "ny rangers": "New York Rangers",
    "ny islanders": "New York Islanders",
    "ny mets": "New York Mets",
    "ny yankees": "New York Yankees",
    "ny jets": "New York Jets",
    "ny giants": "New York Giants",
    "philly 76ers": "Philadelphia 76ers",
    "philly eagles": "Philadelphia Eagles",
    "san fran 49ers": "San Francisco 49ers",
    "niners": "San Francisco 49ers",
    "sf giants": "San Francisco Giants",
    "sf 49ers": "San Francisco 49ers",
    "tb buccaneers": "Tampa Bay Buccaneers",
    "tb rays": "Tampa Bay Rays",
    "tb lightning": "Tampa Bay Lightning",
    "gs warriors": "Golden State Warriors",
    "gsw": "Golden State Warriors",
    "okc thunder": "Oklahoma City Thunder",
    "okc": "Oklahoma City Thunder",
    "nola pelicans": "New Orleans Pelicans",
    "nola": "New Orleans Pelicans",
    "minny timberwolves": "Minnesota Timberwolves",
    # "wolves" removed — ambiguous across NBA/EPL; use SPORT_TEAM_ALIASES
    "t-wolves": "Minnesota Timberwolves",
    "sixers": "Philadelphia 76ers",
    "blazers": "Portland Trail Blazers",
    "trail blazers": "Portland Trail Blazers",
    # ── MMA / UFC fighter aliases ──
    "jose medina": "Jose Daniel Medina",
    "javier reyes": "Javier Reyes Rugeles",
    "douglas silva": "Douglas Silva de Andrade",
    "bobby green": "King Green",
    "christian quinonez": "Cristian Quinonez",
    "nyamjargal t.": "Nyamjargal Tumendemberel",
    "nyamjargal t": "Nyamjargal Tumendemberel",
    "xiao long": "Long Xiao",
    "jesus aguilar": "Jesus Santos Aguilar",
    "wesley schultz": "Wes Schultz",
    "lone'er kavanagh": "Loneer Kavanagh",
    "raul rosas jr": "Raul Rosas Jr.",
    "reinier de ridder": "Reinier De Ridder",
    # ── Soccer / Football club name aliases ──
    # EPL
    "wolverhampton": "Wolverhampton Wanderers",
    "wolverhampton wanderers fc": "Wolverhampton Wanderers",
    "arsenal fc": "Arsenal",
    "tottenham": "Tottenham Hotspur",
    "tottenham hotspur fc": "Tottenham Hotspur",
    "brighton & hove": "Brighton",
    "brighton & hove albion": "Brighton",
    "brighton & hove albion fc": "Brighton",
    "brighton and hove albion": "Brighton",
    "afc bournemouth": "Bournemouth",
    "bournemouth fc": "Bournemouth",
    "newcastle": "Newcastle United",
    "newcastle united fc": "Newcastle United",
    "west ham": "West Ham United",
    "west ham united fc": "West Ham United",
    "aston villa fc": "Aston Villa",
    "manchester city fc": "Manchester City",
    "man city": "Manchester City",
    "manchester united fc": "Manchester United",
    "man utd": "Manchester United",
    "man united": "Manchester United",
    "liverpool fc": "Liverpool",
    "chelsea fc": "Chelsea",
    "everton fc": "Everton",
    "nottingham forest fc": "Nottingham Forest",
    "nott'm forest": "Nottingham Forest",
    "crystal palace fc": "Crystal Palace",
    "fulham fc": "Fulham",
    "brentford fc": "Brentford",
    "burnley fc": "Burnley",
    "leeds united fc": "Leeds United",
    "sunderland afc": "Sunderland",
    # La Liga
    "betis": "Real Betis",
    "real betis balompie": "Real Betis",
    "fc barcelona": "Barcelona",
    "atletico madrid": "Atletico Madrid",
    "atletico de madrid": "Atletico Madrid",
    "real sociedad de futbol": "Real Sociedad",
    "celta de vigo": "Celta Vigo",
    "celta": "Celta Vigo",
    "rcd mallorca": "Mallorca",
    "rcd espanyol": "Espanyol",
    "deportivo alaves": "Alaves",
    "deportivo alavés": "Alaves",
    "cd leganes": "Leganes",
    "real valladolid": "Valladolid",
    # Bundesliga
    "bayern munich": "Bayern Munich",
    "fc bayern munich": "Bayern Munich",
    "fc bayern": "Bayern Munich",
    "bayern munchen": "Bayern Munich",
    "bayern münchen": "Bayern Munich",
    "borussia dortmund": "Borussia Dortmund",
    "bor. dortmund": "Borussia Dortmund",
    "dortmund": "Borussia Dortmund",
    "borussia monchengladbach": "Borussia Monchengladbach",
    "bor. monchengladbach": "Borussia Monchengladbach",
    "borussia mönchengladbach": "Borussia Monchengladbach",
    "bayer leverkusen": "Bayer Leverkusen",
    "bayer 04 leverkusen": "Bayer Leverkusen",
    "rb leipzig": "RB Leipzig",
    "rasenballsport leipzig": "RB Leipzig",
    "eintracht frankfurt": "Eintracht Frankfurt",
    "ein frankfurt": "Eintracht Frankfurt",
    "vfb stuttgart": "Stuttgart",
    "vfl wolfsburg": "Wolfsburg",
    "sc freiburg": "Freiburg",
    "1. fc union berlin": "Union Berlin",
    "fc union berlin": "Union Berlin",
    "1. fc heidenheim": "Heidenheim",
    "fc heidenheim": "Heidenheim",
    "1. fsv mainz 05": "Mainz",
    "fsv mainz 05": "Mainz",
    "mainz 05": "Mainz",
    "tsg hoffenheim": "Hoffenheim",
    "fc augsburg": "Augsburg",
    "sv werder bremen": "Werder Bremen",
    "werder bremen": "Werder Bremen",
    "vfl bochum": "Bochum",
    "fc st. pauli": "St. Pauli",
    "fc st pauli": "St. Pauli",
    "holstein kiel": "Holstein Kiel",
    # Serie A
    "ac milan": "AC Milan",
    "inter milan": "Inter Milan",
    "fc internazionale milano": "Inter Milan",
    "inter": "Inter Milan",
    "internazionale": "Inter Milan",
    "juventus fc": "Juventus",
    "ssc napoli": "Napoli",
    "as roma": "Roma",
    "ss lazio": "Lazio",
    "acf fiorentina": "Fiorentina",
    "atalanta bc": "Atalanta",
    "torino fc": "Torino",
    "bologna fc": "Bologna",
    "us lecce": "Lecce",
    "cagliari calcio": "Cagliari",
    "hellas verona": "Verona",
    "udinese calcio": "Udinese",
    "us sassuolo": "Sassuolo",
    "genoa cfc": "Genoa",
    "empoli fc": "Empoli",
    "como 1907": "Como",
    "parma calcio 1913": "Parma",
    "venezia fc": "Venezia",
    "ac monza": "Monza",
    # Ligue 1
    "paris saint-germain": "Paris Saint Germain",
    "paris saint germain fc": "Paris Saint Germain",
    "paris sg": "Paris Saint Germain",
    "psg": "Paris Saint Germain",
    "olympique de marseille": "Marseille",
    "olympique marseille": "Marseille",
    "olympique lyonnais": "Lyon",
    "olympique lyon": "Lyon",
    "ol": "Lyon",
    "om": "Marseille",
    "as monaco": "Monaco",
    "as monaco fc": "Monaco",
    "losc lille": "Lille",
    "losc": "Lille",
    "stade rennais": "Rennes",
    "stade rennais fc": "Rennes",
    "rc lens": "Lens",
    "rc strasbourg": "Strasbourg",
    "rc strasbourg alsace": "Strasbourg",
    "ogc nice": "Nice",
    "fc nantes": "Nantes",
    "stade brestois 29": "Brest",
    "stade brestois": "Brest",
    "montpellier hsc": "Montpellier",
    "toulouse fc": "Toulouse",
    "clermont foot 63": "Clermont",
    "clermont foot": "Clermont",
    "fc lorient": "Lorient",
    "stade de reims": "Reims",
    "le havre ac": "Le Havre",
    "rc lens": "Lens",
    "angers sco": "Angers",
    "as saint-etienne": "Saint-Etienne",
    "as saint etienne": "Saint-Etienne",
    "aj auxerre": "Auxerre",
    # UCL - common abbreviations
    "real madrid cf": "Real Madrid",
    # ── Additional La Liga aliases (Novig/Buckeye variants) ──
    "levante ud": "Levante",
    "villarreal cf": "Villarreal",
    "getafe cf": "Getafe",
    "sevilla fc": "Sevilla",
    "valencia cf": "Valencia",
    "reial club deportiu espanyol": "Espanyol",
    "rcd espanyol de barcelona": "Espanyol",
    "barcelona fc": "Barcelona",
    "oviedo": "Real Oviedo",
    "cd alaves": "Alaves",
    "girona fc": "Girona",
    "real betis balompie": "Real Betis",
    "rayo vallecano de madrid": "Rayo Vallecano",
    "athletic club": "Athletic Bilbao",
    "athletic club bilbao": "Athletic Bilbao",
    "real sociedad de futbol": "Real Sociedad",
    "sd eibar": "Eibar",
    "ca osasuna": "Osasuna",
    "elche cf": "Elche",
    "celta de vigo": "Celta Vigo",
    "celta": "Celta Vigo",
    # ── NHL additional aliases ──
    "utah mammoth": "Utah Hockey Club",
    "wild": "Minnesota Wild",
    # ── NCAAB additional aliases (ProphetX full mascot names) ──
    "bucknell bison": "Bucknell",
    "bucknell university": "Bucknell",
    "army west point": "Army",
    "army west point black knights": "Army",
    "oakland golden grizzlies": "Oakland",
    "oakland university": "Oakland",
    "iu indy jaguars": "IU Indianapolis",
    "iu indianapolis jaguars": "IU Indianapolis",
    "indiana university indianapolis": "IU Indianapolis",
    "furman paladins": "Furman",
    "the citadel bulldogs": "Citadel",
    "the citadel": "Citadel",
    "citadel bulldogs": "Citadel",
    "boston u": "Boston University",
    "detroit u": "Detroit Mercy",
    "detroit mercy titans": "Detroit Mercy",
    "east tenn st": "East Tennessee State",
    "wofford terriers": "Wofford",
    "loyola md": "Loyola Maryland",
    "navy midshipmen": "Navy",
    "holy cross crusaders": "Holy Cross",
    "colgate raiders": "Colgate",
    "lafayette leopards": "Lafayette",
    "lehigh mountain hawks": "Lehigh",
    "american university": "American",
    "american eagles": "American",
    "army black knights": "Army",
    # ── MMA / UFC additional fighter aliases ──
    "marlon chito vera": "Marlon Vera",
    "chito vera": "Marlon Vera",
    "miguel david martinez aceves": "David Martinez",
    "david martinez aceves": "David Martinez",
    "bobby green": "King Green",
    "gaston bolanos": "Gaston Bolanos",
    "gregory rodrigues": "Gregory Rodrigues",
    "su mudaerji": "Su Mudaerji",
    "israel adesanya": "Israel Adesanya",
    "charles oliveira": "Charles Oliveira",
    "max holloway": "Max Holloway",
    "caio borralho": "Caio Borralho",
    # ── La Liga accented variants ──
    "club atlético de madrid": "Atletico Madrid",
    "real betis balompié": "Real Betis",
    # ── Tennis player aliases ──
    "stan wawrinka": "Stanislas Wawrinka",
    "daniil medvédev": "Daniil Medvedev",
    "pablo carreno busta": "Pablo Carreno Busta",
    "pablo carreño busta": "Pablo Carreno Busta",
    "pablo carreño-busta": "Pablo Carreno Busta",
    # ── NCAAB comprehensive aliases (fix cross-source duplicates) ──
    # ProphetX full mascot names → canonical school names
    "saint joseph's hawks": "Saint Josephs",
    "saint josephs hawks": "Saint Josephs",
    "st josephs hawks": "Saint Josephs",
    "duquesne dukes": "Duquesne",
    "davidson wildcats": "Davidson",
    "george mason patriots": "George Mason",
    "western carolina catamounts": "Western Carolina",
    "mercer bears": "Mercer",
    "west georgia wolves": "West Georgia",
    "evansville purple aces": "Evansville",
    "evansville aces": "Evansville",
    "belmont bruins": "Belmont",
    "rice owls": "Rice",
    "south florida bulls": "South Florida",
    "georgia southern eagles": "Georgia Southern",
    "james madison dukes": "James Madison",
    "drake bulldogs": "Drake",
    "central arkansas bears": "Central Arkansas",
    "austin peay governors": "Austin Peay",
    "austin peay": "Austin Peay",
    "portland pilots": "Portland",
    "depaul blue demons": "DePaul",
    "depaul": "DePaul",
    "san diego toreros": "San Diego",
    "oregon state beavers": "Oregon State",
    "santa clara broncos": "Santa Clara",
    "saint mary's gaels": "Saint Marys",
    "saint marys gaels": "Saint Marys",
    "loyola marymount lions": "Loyola Marymount",
    "loyola maryland greyhounds": "Loyola Maryland",
    "tulane green wave": "Tulane",
    "north texas mean green": "North Texas",
    "charlotte 49ers": "Charlotte",
    "northern iowa panthers": "Northern Iowa",
    "southern illinois salukis": "Southern Illinois",
    "east carolina pirates": "East Carolina",
    "washington state cougars": "Washington State",
    # Milwaukee variants (Wisconsin-Milwaukee / UWM)
    "wisconsin milwaukee": "Milwaukee",
    "wisc milwaukee": "Milwaukee",
    "uw-milwaukee": "Milwaukee",
    "uw milwaukee": "Milwaukee",
    "uwm": "Milwaukee",
    "milwaukee panthers": "Milwaukee",
    # Charlotte variants
    "charlotte u": "Charlotte",
    "unc charlotte": "Charlotte",
    "uncc": "Charlotte",
    # Omaha / Nebraska Omaha
    "nebraska omaha": "Omaha",
    "nebraska-omaha": "Omaha",
    # Seattle variants
    "seattle u": "Seattle",
    # Saint Mary's variants
    "st. mary's": "Saint Marys",
    "st mary's": "Saint Marys",
    "saint mary's": "Saint Marys",
    "st marys": "Saint Marys",
    "saint marys": "Saint Marys",
    "st marys ca": "Saint Marys",
    "saint marys ca": "Saint Marys",
    # Tennessee-Martin / UT Martin
    "ut martin": "Tennessee-Martin",
    "tennessee martin": "Tennessee-Martin",
    "tennessee-martin": "Tennessee-Martin",
    "tennessee-martin skyhawks": "Tennessee-Martin",
    "ut martin skyhawks": "Tennessee-Martin",
    # Fairleigh Dickinson / FDU
    "fdu": "Fairleigh Dickinson",
    "fairleigh dickinson knights": "Fairleigh Dickinson",
    # IPFW / Purdue Fort Wayne
    "ipfw": "Purdue Fort Wayne",
    # Louisiana-Monroe additional variants
    "louisiana monroe": "Louisiana-Monroe",
    "ul - monroe": "Louisiana-Monroe",
    # SIUE / SIU Edwardsville
    "siue": "SIU Edwardsville",
    "siu edwardsville": "SIU Edwardsville",
    "siu-edwardsville": "SIU Edwardsville",
    "southern illinois edwardsville": "SIU Edwardsville",
    # UNCW / UNC Wilmington
    "uncw": "UNC Wilmington",
    "unc wilmington": "UNC Wilmington",
    "unc wilmington seahawks": "UNC Wilmington",
    # CSUN / Cal State Northridge
    "csun": "Cal State Northridge",
    "cal state northridge": "Cal State Northridge",
    "cal st northridge": "Cal State Northridge",
    "northridge": "Cal State Northridge",
    # Additional Bovada/Pinnacle/Bookmaker variants
    "east tenn state": "East Tennessee State",
    "portland u": "Portland",
    "north alabama lions": "North Alabama",
    # Kalshi / Bookmaker / Polymarket "St." vs "State" variants
    "long beach st.": "Long Beach State",
    "long beach st": "Long Beach State",
    "long beach state": "Long Beach State",
    "long beach state beach": "Long Beach State",
    "central connecticut st.": "Central Connecticut",
    "central connecticut st": "Central Connecticut",
    "south dakota st.": "South Dakota State",
    "chicago st.": "Chicago State",
    "chicago st": "Chicago State",
    "sacramento st.": "Sacramento State",
    "sacramento st": "Sacramento State",
    "sacramento state": "Sacramento State",
    "sacramento state hornets": "Sacramento State",
    "north dakota st.": "North Dakota State",
    "tarleton st.": "Tarleton State",
    "tarleton st": "Tarleton State",
    "tarleton state": "Tarleton State",
    "tarleton state texans": "Tarleton State",
    "new mexico st.": "New Mexico State",
    "new mexico st": "New Mexico State",
    "new mexico state": "New Mexico State",
    "new mexico state aggies": "New Mexico State",
    "st. thomas (mn)": "St. Thomas",
    "st thomas (mn)": "St. Thomas",
    "st. thomas mn": "St. Thomas",
    "st thomas mn": "St. Thomas",
    "st. thomas": "St. Thomas",
    "saint thomas": "St. Thomas",
    "grambling st.": "Grambling",
    "grambling st": "Grambling",
    "kennesaw st.": "Kennesaw State",
    "kennesaw st": "Kennesaw State",
    "kennesaw state": "Kennesaw State",
    "kennesaw state owls": "Kennesaw State",
    "jacksonville st.": "Jacksonville State",
    "jacksonville st": "Jacksonville State",
    "jacksonville state": "Jacksonville State",
    "jacksonville state gamecocks": "Jacksonville State",
    "wichita st.": "Wichita State",
    "wichita st": "Wichita State",
    "wichita state": "Wichita State",
    "wichita state shockers": "Wichita State",
    "murray st.": "Murray State",
    "murray st": "Murray State",
    "murray state": "Murray State",
    "murray state racers": "Murray State",
    "weber st.": "Weber State",
    "weber st": "Weber State",
    "weber state": "Weber State",
    "weber state wildcats": "Weber State",
    "semo": "Southeast Missouri State",
    "se missouri st.": "Southeast Missouri State",
    "se missouri st": "Southeast Missouri State",
    "southeast missouri state": "Southeast Missouri State",
    "southeast missouri st.": "Southeast Missouri State",
    "southeast missouri st": "Southeast Missouri State",
    "tennessee st.": "Tennessee State",
    "tennessee st": "Tennessee State",
    "tennessee state": "Tennessee State",
    "tennessee state tigers": "Tennessee State",
    "norfolk st.": "Norfolk State",
    "norfolk st": "Norfolk State",
    "norfolk state": "Norfolk State",
    "norfolk state spartans": "Norfolk State",
    "coppin st.": "Coppin State",
    "coppin st": "Coppin State",
    "coppin state": "Coppin State",
    "coppin state eagles": "Coppin State",
    "alcorn st.": "Alcorn State",
    "alcorn st": "Alcorn State",
    "alcorn state": "Alcorn State",
    "alcorn state braves": "Alcorn State",
    "jackson st.": "Jackson State",
    "jackson st": "Jackson State",
    "jackson state": "Jackson State",
    "jackson state tigers": "Jackson State",
    "alabama st.": "Alabama State",
    "alabama st": "Alabama State",
    "alabama state": "Alabama State",
    "alabama state hornets": "Alabama State",
    "appalachian st.": "Appalachian State",
    "appalachian st": "Appalachian State",
    "app state": "Appalachian State",
    "app st": "Appalachian State",
    "app st.": "Appalachian State",
    "appalachian state mountaineers": "Appalachian State",
    "montana st.": "Montana State",
    "montana st": "Montana State",
    "montana state": "Montana State",
    "montana state bobcats": "Montana State",
    "idaho st.": "Idaho State",
    "idaho st": "Idaho State",
    "idaho state": "Idaho State",
    "idaho state bengals": "Idaho State",
    "sam houston st.": "Sam Houston",
    "sam houston st": "Sam Houston",
    "sam houston state": "Sam Houston",
    "portland st.": "Portland State",
    "portland st": "Portland State",
    "texas st.": "Texas State",
    "texas st": "Texas State",
    "texas state": "Texas State",
    "texas state bobcats": "Texas State",
    # ── NCAAB cross-source duplicate fixes ──────────────────────────────────
    # UMBC / Maryland-Baltimore County
    "md baltimore co": "UMBC",
    "maryland baltimore county": "UMBC",
    "maryland-baltimore county": "UMBC",
    "umbc": "UMBC",
    "umbc retrievers": "UMBC",
    "md baltimore co retrievers": "UMBC",
    # Florida Gulf Coast / FGCU
    "fla gulf coast": "Florida Gulf Coast",
    "fgcu": "Florida Gulf Coast",
    "florida gulf coast eagles": "Florida Gulf Coast",
    "fla. gulf coast": "Florida Gulf Coast",
    # Arkansas Little Rock / UALR
    "ark little rock": "Arkansas Little Rock",
    "little rock": "Arkansas Little Rock",
    "ualr": "Arkansas Little Rock",
    "arkansas little rock": "Arkansas Little Rock",
    "arkansas-little rock": "Arkansas Little Rock",
    "little rock trojans": "Arkansas Little Rock",
    "ark little rock trojans": "Arkansas Little Rock",
    "arkansas little rock trojans": "Arkansas Little Rock",
    # UNC Asheville / North Carolina Asheville
    "north carolina asheville": "UNC Asheville",
    "unc asheville": "UNC Asheville",
    "unca": "UNC Asheville",
    "nc asheville": "UNC Asheville",
    "unc asheville bulldogs": "UNC Asheville",
    "n.c. asheville": "UNC Asheville",
    # UC Irvine / Cal Irvine
    "cal irvine": "UC Irvine",
    "uc irvine": "UC Irvine",
    "uci": "UC Irvine",
    "uc irvine anteaters": "UC Irvine",
    "cal irvine anteaters": "UC Irvine",
    # CS / CSU Northridge (additional variants beyond existing)
    "cs northridge": "Cal State Northridge",
    "csu northridge": "Cal State Northridge",
    "cal northridge": "Cal State Northridge",
    "cal state northridge matadors": "Cal State Northridge",
    # UC Riverside / Cal Riverside
    "cal riverside": "UC Riverside",
    "uc riverside": "UC Riverside",
    "ucr": "UC Riverside",
    "uc riverside highlanders": "UC Riverside",
    "cal riverside highlanders": "UC Riverside",
    # UC Santa Barbara / Cal Santa Barbara
    "cal santa barbara": "UC Santa Barbara",
    "uc santa barbara": "UC Santa Barbara",
    "ucsb": "UC Santa Barbara",
    "uc santa barbara gauchos": "UC Santa Barbara",
    "cal santa barbara gauchos": "UC Santa Barbara",
    # Northern Colorado
    "no. colorado": "Northern Colorado",
    "n. colorado": "Northern Colorado",
    "northern colorado bears": "Northern Colorado",
    "northern colo": "Northern Colorado",
    # FIU / Florida International (additional variants)
    "florida intl": "FIU",
    "florida international golden panthers": "FIU",
    "fiu golden panthers": "FIU",
    "fla intl": "FIU",
    "fla international": "FIU",
    # CSU/CS Bakersfield (additional variants)
    "cs bakersfield": "Cal State Bakersfield",
    "csu bakersfield": "Cal State Bakersfield",
    "csub": "Cal State Bakersfield",
    "cal state bakersfield roadrunners": "Cal State Bakersfield",
    "cal st bakersfield": "Cal State Bakersfield",
    # UMKC → Kansas City
    "umkc": "Kansas City",
    "umkc kangaroos": "Kansas City",
    "missouri-kansas city": "Kansas City",
    # Saint Thomas MN (missing variant)
    "saint thomas mn": "St. Thomas",
    "saint thomas (mn)": "St. Thomas",
    # Middle Tennessee
    "middle tenn st": "Middle Tennessee",
    "middle tenn st.": "Middle Tennessee",
    "middle tennessee state": "Middle Tennessee",
    "middle tenn": "Middle Tennessee",
    "mtsu": "Middle Tennessee",
    "middle tennessee blue raiders": "Middle Tennessee",
    "middle tennessee st": "Middle Tennessee",
    "middle tennessee st.": "Middle Tennessee",
    # Penn (University of Pennsylvania)
    "penn quakers": "Penn",
    # Long Island → LIU
    "long island": "LIU",
    "long island university": "LIU",
    "long island university sharks": "LIU",
    # SE Missouri State (missing variant)
    "se missouri state": "Southeast Missouri State",
    "se missouri": "Southeast Missouri State",
    "southeast missouri": "Southeast Missouri State",
    # Cornell
    "cornell big red": "Cornell",
    "cornell big": "Cornell",
    # Dartmouth
    "dartmouth big green": "Dartmouth",
    "dartmouth big": "Dartmouth",
    # Hofstra
    "hofstra pride": "Hofstra",
    # Loyola Chicago
    "loyola chicago ramblers": "Loyola Chicago",
    "loyola chicago": "Loyola Chicago",
    "loyola-chicago": "Loyola Chicago",
    "loyola (chi)": "Loyola Chicago",
    # Cal Poly
    "cal poly mustangs": "Cal Poly",
    "cal poly slo": "Cal Poly",
    # Cal State Fullerton
    "cs fullerton": "Cal State Fullerton",
    "csu fullerton": "Cal State Fullerton",
    "cal st fullerton": "Cal State Fullerton",
    "csuf": "Cal State Fullerton",
    "cal state fullerton titans": "Cal State Fullerton",
    # Nicholls State
    "nicholls st.": "Nicholls State",
    "nicholls st": "Nicholls State",
    "nicholls state colonels": "Nicholls State",
    "nicholls": "Nicholls State",
    # McNeese State
    "mcneese st.": "McNeese State",
    "mcneese st": "McNeese State",
    "mcneese": "McNeese State",
    "mcneese state cowboys": "McNeese State",
    # Northwestern State
    "northwestern st.": "Northwestern State",
    "northwestern st": "Northwestern State",
    "northwestern state demons": "Northwestern State",
    # Southeastern Louisiana
    "se louisiana": "SE Louisiana",
    "southeastern louisiana": "SE Louisiana",
    "southeastern la": "SE Louisiana",
    "se la": "SE Louisiana",
    # Southern Utah
    "southern utah thunderbirds": "Southern Utah",
    "suu": "Southern Utah",
    # Incarnate Word
    "incarnate word cardinals": "Incarnate Word",
    "uiw": "Incarnate Word",
    # Stephen F. Austin
    "sfa": "Stephen F. Austin",
    "stephen f austin": "Stephen F. Austin",
    "stephen f. austin lumberjacks": "Stephen F. Austin",
    # Houston Christian
    "houston christian huskies": "Houston Christian",
    "houston baptist": "Houston Christian",
    # Texas A&M-Corpus Christi
    "texas a&m-corpus christi": "Texas A&M-Corpus Christi",
    "a&m-corpus christi": "Texas A&M-Corpus Christi",
    "tamucc": "Texas A&M-Corpus Christi",
    # Mississippi Valley State
    "mississippi valley st.": "Mississippi Valley State",
    "mississippi valley st": "Mississippi Valley State",
    "miss valley st": "Mississippi Valley State",
    "mvsu": "Mississippi Valley State",
    # Arkansas-Pine Bluff
    "arkansas-pine bluff": "Arkansas-Pine Bluff",
    "arkansas pine bluff": "Arkansas-Pine Bluff",
    "uapb": "Arkansas-Pine Bluff",
    "ark-pine bluff": "Arkansas-Pine Bluff",
    "ark pine bluff": "Arkansas-Pine Bluff",
    # South Carolina State
    "south carolina st.": "South Carolina State",
    "south carolina st": "South Carolina State",
    "sc state": "South Carolina State",
    "s carolina state": "South Carolina State",
    "s carolina st": "South Carolina State",
    "s carolina st.": "South Carolina State",
    "s.c. state": "South Carolina State",
    "sc st": "South Carolina State",
    "sc st.": "South Carolina State",
    "so carolina state": "South Carolina State",
    "so carolina st": "South Carolina State",
    "south car state": "South Carolina State",
    "south car st": "South Carolina State",
    # Maryland Eastern Shore
    "maryland eastern shore": "Maryland Eastern Shore",
    "umes": "Maryland Eastern Shore",
    "md eastern shore": "Maryland Eastern Shore",
    # North Carolina Central
    "nc central": "North Carolina Central",
    "north carolina central eagles": "North Carolina Central",
    "n carolina central": "North Carolina Central",
    "n.c. central": "North Carolina Central",
    "n carolina cent": "North Carolina Central",
    "no carolina central": "North Carolina Central",
    "north car central": "North Carolina Central",
    "nccu": "North Carolina Central",
    # Delaware State
    "delaware st.": "Delaware State",
    "delaware st": "Delaware State",
    "delaware state hornets": "Delaware State",
    # Norfolk State / Coppin State — already defined above
    # Alabama A&M
    "alabama a&m bulldogs": "Alabama A&M",
    "aamu": "Alabama A&M",
    # Howard
    "howard bison": "Howard",
    # Morgan State
    "morgan st.": "Morgan State",
    "morgan st": "Morgan State",
    "morgan state bears": "Morgan State",
    # Prairie View A&M
    "prairie view": "Prairie View A&M",
    "prairie view a&m panthers": "Prairie View A&M",
    "pvamu": "Prairie View A&M",
    # Texas Southern
    "texas southern tigers": "Texas Southern",
    "txso": "Texas Southern",
    # Bethune-Cookman
    "bethune cookman": "Bethune-Cookman",
    "bethune-cookman wildcats": "Bethune-Cookman",
    "b-cu": "Bethune-Cookman",
    # Florida A&M
    "florida a&m rattlers": "Florida A&M",
    "famu": "Florida A&M",
    # Grambling
    "grambling state": "Grambling",
    "grambling state tigers": "Grambling",
    # Southern (Southern University)
    "southern jaguars": "Southern",
    "southern university": "Southern",
    # Utah Tech
    "utah tech trailblazers": "Utah Tech",
    "dixie state": "Utah Tech",
    # UT Arlington
    "texas arlington": "UT Arlington",
    "texas-arlington": "UT Arlington",
    "ut arlington": "UT Arlington",
    "ut arlington mavericks": "UT Arlington",
    "uta mavericks": "UT Arlington",
    # California Baptist
    "california baptist lancers": "California Baptist",
    "cal baptist": "California Baptist",
    "cbu": "California Baptist",
    # ── NCAAB name-variant dedup aliases ──
    "st. francis pennsylvania": "Saint Francis",
    "saint francis pennsylvania": "Saint Francis",
    "st francis pennsylvania": "Saint Francis",
    "south carolina upstate": "USC Upstate",
    "sc upstate": "USC Upstate",
    # ── NHL name-variant dedup aliases ──
    "was capitals": "Washington Capitals",
    # ── MLB full team names (prevent mascot stripping from breaking them) ──
    "detroit tigers": "Detroit Tigers",
    "st. louis cardinals": "St. Louis Cardinals",
    "saint louis cardinals": "St. Louis Cardinals",
    "kansas city royals": "Kansas City Royals",
    "atlanta braves": "Atlanta Braves",
    "pittsburgh pirates": "Pittsburgh Pirates",
    "toronto blue jays": "Toronto Blue Jays",
    "chicago white sox": "Chicago White Sox",
    "minnesota twins": "Minnesota Twins",
    # ── MLB Polymarket-style nicknames (no city prefix) ──
    "rays": "Tampa Bay Rays",
    "tigers": "Detroit Tigers",
    "marlins": "Miami Marlins",
    "cardinals": "St. Louis Cardinals",
    "royals": "Kansas City Royals",
    "astros": "Houston Astros",
    "mets": "New York Mets",
    "yankees": "New York Yankees",
    "dodgers": "Los Angeles Dodgers",
    "padres": "San Diego Padres",
    "athletics": "Oakland Athletics",
    "guardians": "Cleveland Guardians",
    "brewers": "Milwaukee Brewers",
    "cubs": "Chicago Cubs",
    "reds": "Cincinnati Reds",
    "diamondbacks": "Arizona Diamondbacks",
    "d-backs": "Arizona Diamondbacks",
    "mariners": "Seattle Mariners",
    "orioles": "Baltimore Orioles",
    "nationals": "Washington Nationals",
    "phillies": "Philadelphia Phillies",
    "rockies": "Colorado Rockies",
    "twins": "Minnesota Twins",
    "white sox": "Chicago White Sox",
    "pirates": "Pittsburgh Pirates",
    "braves": "Atlanta Braves",
    "blue jays": "Toronto Blue Jays",
    "angels": "Los Angeles Angels",
    # ── Soccer dedup aliases (full official names → short canonical) ──
    # La Liga
    "real club celta de vigo": "Celta Vigo",
    "rc celta de vigo": "Celta Vigo",
    # Bundesliga
    "fc bayern munchen": "Bayern Munich",
    "fc bayern münchen": "Bayern Munich",
    "bv borussia 09 dortmund": "Borussia Dortmund",
    "bv borussia dortmund": "Borussia Dortmund",
    "borussia vfl monchengladbach": "Borussia Monchengladbach",
    "borussia vfl mönchengladbach": "Borussia Monchengladbach",
    "vfl borussia monchengladbach": "Borussia Monchengladbach",
    "1. fc koln": "FC Koln",
    "1. fc köln": "FC Koln",
    "fc koln": "FC Koln",
    "koln": "FC Koln",
    "cologne": "FC Koln",
    "mainz": "Mainz",
    "1. fsv mainz": "Mainz",
    "hamburger sv": "Hamburg",
    "hsv": "Hamburg",
    # Serie A
    "fc inter milan": "Inter Milan",
    "hellas verona fc": "Verona",
    "verona fc": "Verona",
    "us cremonese": "Cremonese",
    "cremonese": "Cremonese",
    "us sassuolo calcio": "Sassuolo",
    "sassuolo calcio": "Sassuolo",
    "atalanta bergamasca calcio": "Atalanta",
    "pisa sc": "Pisa",
    # Ligue 1
    "paris st-germain": "Paris Saint Germain",
    "paris st germain": "Paris Saint Germain",
    "association jeunesse auxerroise": "Auxerre",
    "fc metz": "Metz",
    "metz": "Metz",
    "lille osc": "Lille",
    "paris fc": "Paris FC",
    # UCL
    "sporting cp": "Sporting CP",
    "sporting clube de portugal": "Sporting CP",
    "sporting lisbon": "Sporting CP",
    "bodo glimt": "Bodo Glimt",
    "bodo/glimt": "Bodo Glimt",
    "fk bodo/glimt": "Bodo Glimt",
    "fk bodø / glimt": "Bodo Glimt",
    "fk bodo / glimt": "Bodo Glimt",
    "bodø/glimt": "Bodo Glimt",
}


# ── Sport-specific aliases (override global when sport context is available) ──
# Use for short names that are ambiguous across sports (e.g. "Utah" = NHL team vs college)
SPORT_TEAM_ALIASES = {
    "icehockey_nhl": {
        "utah": "Utah Hockey Club",
        "kings": "Los Angeles Kings",
        "sacramento kings": "Los Angeles Kings",
        "minnesota": "Minnesota Wild",
        "minnesota timberwolves": "Minnesota Wild",
        "boston college": "Boston Bruins",
    },
    "basketball_nba": {
        "kings": "Sacramento Kings",
        "minnesota": "Minnesota Timberwolves",
        "detroit": "Detroit Pistons",
        "wolves": "Minnesota Timberwolves",
        "new orleans": "New Orleans Pelicans",
    },
    "basketball_ncaab": {
        "detroit": "Detroit Mercy",
        "minnesota": "Minnesota Golden Gophers",
        "minnesota wild": "Minnesota Golden Gophers",
        "minnesota timberwolves": "Minnesota Golden Gophers",
        "utah jazz": "Utah",
        "new orleans pelicans": "New Orleans",
    },
    "baseball_mlb": {
        "detroit": "Detroit Tigers",
        "detroit mercy": "Detroit Tigers",
        "minnesota": "Minnesota Twins",
        "kings": "Kansas City Royals",
        "saint louis": "St. Louis Cardinals",
        "kansas city": "Kansas City Royals",
    },
    "soccer_epl": {
        "wolves": "Wolverhampton Wanderers",
        "minnesota timberwolves": "Wolverhampton Wanderers",
    },
}


def resolve_team_name(name: str, sport_key: str = "") -> str:
    """Resolve common abbreviations to full team names.

    If sport_key is provided, checks sport-specific aliases first to avoid
    cross-sport collisions (e.g. "Utah" meaning Utah Hockey Club in NHL but
    University of Utah in NCAAB).

    Uses multi-pass lookup:
    0. Strip ranking prefixes/suffixes like "(6) UConn" → "UConn"
    1. Exact alias match on lowercased input
    2. Pre-normalized form (St. → Saint, strip mascots, strip University, etc.)
    3. Pre-normalized + accent-stripped form
    """
    # Strip ranking indicators like (#21), (6), #5 before alias lookup
    name = re.sub(r"\s*\(?#?\d{1,3}\)?\s*$", "", name).strip()
    name = re.sub(r"^\s*\(?#?\d{1,3}\)?\s+", "", name).strip()

    # Normalize non-breaking spaces (U+00A0) and other Unicode whitespace
    lower = name.replace("\xa0", " ").lower().strip()
    # Sport-specific alias takes priority
    if sport_key:
        sport_aliases = SPORT_TEAM_ALIASES.get(sport_key)
        if sport_aliases:
            result = sport_aliases.get(lower)
            if result:
                return result

    # Pass 1: exact alias match
    result = TEAM_ALIASES.get(lower)
    if result:
        return result

    # Pass 2: pre-normalized form (St.→Saint, strip mascots/University/Jr.)
    pre_norm = _pre_normalize_name(name).lower().strip()
    if pre_norm != lower:
        result = TEAM_ALIASES.get(pre_norm)
        if result:
            return result

    # Pass 3: accent-stripped pre-normalized form
    stripped = unicodedata.normalize("NFD", pre_norm)
    stripped = "".join(c for c in stripped if unicodedata.category(c) != "Mn")
    if stripped != pre_norm:
        result = TEAM_ALIASES.get(stripped)
        if result:
            return result

    return name


def cents_to_american(cents: int) -> int:
    """Convert prediction market cent price (1-99) to American odds."""
    if cents <= 0 or cents >= 100:
        return 0
    prob = cents / 100.0
    if prob >= 0.5:
        return round(-100 * prob / (1 - prob))
    else:
        return round(100 * (1 - prob) / prob)


def prob_to_american(prob: float) -> int:
    """Convert a probability (0.0-1.0) to American odds with full float precision.

    Unlike cents_to_american which rounds to integer cents first (losing precision),
    this operates directly on the float probability.  Use this for sources like
    Polymarket that provide decimal probabilities (e.g. 0.625 instead of 63 cents).
    """
    if prob <= 0.0 or prob >= 1.0:
        return 0
    if prob >= 0.5:
        return round(-100.0 * prob / (1.0 - prob))
    else:
        return round(100.0 * (1.0 - prob) / prob)


# ── Kalshi series tickers ──────────────────────────────────────────────────
KALSHI_SERIES_TICKERS = {
    "basketball_nba": {
        "game": "KXNBAGAME",
        "spread": "KXNBASPREAD",
        "total": "KXNBATOTAL",
    },
    "americanfootball_nfl": {
        "game": "KXNFLGAME",
        "spread": "KXNFLSPREAD",
        "total": "KXNFLTOTAL",
    },
    "americanfootball_ncaaf": {
        "game": "KXNCAAFGAME",
    },
    "icehockey_nhl": {
        "game": "KXNHLGAME",
        "spread": "KXNHLSPREAD",
        "total": "KXNHLTOTAL",
    },
    "baseball_mlb": {
        "game": "KXMLBGAME",
        "spread": "KXMLBSPREAD",
        "total": "KXMLBTOTAL",
    },
    "basketball_ncaab": {
        "game": "KXNCAAMBGAME",
        "spread": "KXNCAAMBSPREAD",
        "total": "KXNCAAMBTOTAL",
    },
    "mma_mixed_martial_arts": {
        "game": "KXUFCFIGHT",
    },
    "boxing_boxing": {
        "game": "KXBOXINGFIGHT",
    },
    "soccer_epl": {
        "game": "KXEPLGAME",
    },
    "soccer_spain_la_liga": {
        "game": "KXLALIGAGAME",
    },
    "soccer_germany_bundesliga": {
        "game": "KXBUNDESLIGAGAME",
    },
    "soccer_italy_serie_a": {
        "game": "KXSERIEAGAME",
    },
    "soccer_france_ligue_one": {
        "game": "KXLIGUE1GAME",
    },
    "soccer_usa_mls": {
        "game": "KXMLSGAME",
    },
    "soccer_uefa_champs_league": {
        "game": "KXUCLGAME",
    },
    "tennis_atp": {
        "game": "KXATPMATCH",
    },
    "tennis_wta": {
        "game": "KXWTAMATCH",
    },
}
