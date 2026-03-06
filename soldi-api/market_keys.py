"""
Canonical market key taxonomy for SoldiOdds.

Market keys follow the pattern: {base_type}[_{period_code}]
- base_type: "h2h", "spreads", "totals"
- period_code: "h1", "h2", "q1"-"q4", "p1"-"p3", etc.

Player prop keys: "player_{stat_type}"
Sport-specific micro keys: "hockey_shots_on_goal", "tennis_set1_winner", etc.
"""
import re
from typing import Dict, List, Optional, Tuple

# ─── Full-game base types ───────────────────────────────────────
BASE_MONEYLINE = ("MATCH_BETTING", "MONEY_LINE", "MONEYLINE", "MATCH_RESULT", "FIGHT_WINNER", "BOUT_WINNER", "WINNER")
BASE_SPREADS = ("HANDICAP", "SPREAD", "MATCH_HANDICAP", "MATCH_HANDICAP_(2-WAY)", "POINT_SPREAD", "PUCK_LINE", "RUN_LINE")
BASE_TOTALS = ("TOTAL_POINTS", "TOTAL", "OVER_UNDER", "TOTAL_POINTS_(OVER/UNDER)")

# ─── Period suffix patterns (applied to FanDuel/Bovada/Kambi market names) ──
PERIOD_PATTERNS = [
    (re.compile(r"1ST[_ ]QUARTER", re.IGNORECASE), "_q1"),
    (re.compile(r"2ND[_ ]QUARTER", re.IGNORECASE), "_q2"),
    (re.compile(r"3RD[_ ]QUARTER", re.IGNORECASE), "_q3"),
    (re.compile(r"4TH[_ ]QUARTER", re.IGNORECASE), "_q4"),
    (re.compile(r"1ST[_ ]HALF", re.IGNORECASE), "_h1"),
    (re.compile(r"2ND[_ ]HALF", re.IGNORECASE), "_h2"),
    (re.compile(r"1ST[_ ]PERIOD", re.IGNORECASE), "_p1"),
    (re.compile(r"2ND[_ ]PERIOD", re.IGNORECASE), "_p2"),
    (re.compile(r"3RD[_ ]PERIOD", re.IGNORECASE), "_p3"),
    # Kambi uses "Period N" format (e.g. "Puck Line - Period 3")
    (re.compile(r"PERIOD[_ ]1\b", re.IGNORECASE), "_p1"),
    (re.compile(r"PERIOD[_ ]2\b", re.IGNORECASE), "_p2"),
    (re.compile(r"PERIOD[_ ]3\b", re.IGNORECASE), "_p3"),
    (re.compile(r"1ST[_ ]SET", re.IGNORECASE), "_s1"),
    (re.compile(r"2ND[_ ]SET", re.IGNORECASE), "_s2"),
    (re.compile(r"3RD[_ ]SET", re.IGNORECASE), "_s3"),
    (re.compile(r"1ST[_ ]INNING", re.IGNORECASE), "_i1"),
    (re.compile(r"FIRST[_ ]5[_ ]INNINGS?", re.IGNORECASE), "_f5"),
]

# ─── Player prop patterns ────────────────────────────────────────
# Covers multiple formats: "PLAYER POINTS O/U", "TO_SCORE_25+_POINTS", etc.
PLAYER_PROP_PATTERNS = [
    # Standard O/U format (Kambi, generic)
    (re.compile(r"PLAYER[_ ]POINTS[_ ]O(?:VER)?/?U(?:NDER)?", re.IGNORECASE), "player_points"),
    (re.compile(r"PLAYER[_ ]REBOUNDS[_ ]O(?:VER)?/?U(?:NDER)?", re.IGNORECASE), "player_rebounds"),
    (re.compile(r"PLAYER[_ ]ASSISTS[_ ]O(?:VER)?/?U(?:NDER)?", re.IGNORECASE), "player_assists"),
    (re.compile(r"PLAYER[_ ]THREES?[_ ](?:MADE[_ ])?O(?:VER)?/?U(?:NDER)?", re.IGNORECASE), "player_threes"),
    (re.compile(r"PLAYER[_ ](?:PTS|POINTS)[_ ]\+[_ ](?:REB|REBOUNDS)[_ ]\+[_ ](?:AST|ASSISTS)", re.IGNORECASE), "player_pts_reb_ast"),
    (re.compile(r"PLAYER[_ ]STEALS[_ ]O(?:VER)?/?U(?:NDER)?", re.IGNORECASE), "player_steals"),
    (re.compile(r"PLAYER[_ ]BLOCKS[_ ]O(?:VER)?/?U(?:NDER)?", re.IGNORECASE), "player_blocks"),
    (re.compile(r"PLAYER[_ ]STRIKEOUTS", re.IGNORECASE), "player_strikeouts"),
    (re.compile(r"PLAYER[_ ]TOTAL[_ ]BASES", re.IGNORECASE), "player_total_bases"),
    (re.compile(r"PLAYER[_ ]HITS", re.IGNORECASE), "player_hits"),
    (re.compile(r"PLAYER[_ ]RUNS", re.IGNORECASE), "player_runs"),
    (re.compile(r"PLAYER[_ ]RBIS?", re.IGNORECASE), "player_rbis"),
    (re.compile(r"SHOTS[_ ]ON[_ ]GOAL", re.IGNORECASE), "player_shots_on_goal"),
    (re.compile(r"PLAYER[_ ]GOALS?[_ ]O(?:VER)?/?U(?:NDER)?", re.IGNORECASE), "player_goals"),
    (re.compile(r"PLAYER[_ ]ACES[_ ]O(?:VER)?/?U(?:NDER)?", re.IGNORECASE), "player_aces"),
    (re.compile(r"PLAYER[_ ]GAMES?[_ ]WON", re.IGNORECASE), "player_games_won"),
    # FanDuel milestone format: "TO_SCORE_25+_POINTS", "2+_MADE_THREES", etc.
    (re.compile(r"TO[_ ]SCORE[_ ]\d+\+?[_ ]POINTS", re.IGNORECASE), "player_points"),
    (re.compile(r"TO[_ ]RECORD[_ ]\d+\+?[_ ]REBOUNDS", re.IGNORECASE), "player_rebounds"),
    (re.compile(r"TO[_ ]RECORD[_ ]\d+\+?[_ ]ASSISTS", re.IGNORECASE), "player_assists"),
    (re.compile(r"\d+\+?[_ ]MADE[_ ]THREES", re.IGNORECASE), "player_threes"),
    (re.compile(r"TO[_ ]RECORD[_ ]\d+\+?[_ ]PTS[_ ]\+[_ ]REB[_ ]\+[_ ]AST", re.IGNORECASE), "player_pts_reb_ast"),
    (re.compile(r"TO[_ ]RECORD[_ ]\d+\+?[_ ]PTS[_ ]\+[_ ]REB$", re.IGNORECASE), "player_pts_reb"),
    (re.compile(r"TO[_ ]RECORD[_ ]\d+\+?[_ ]PTS[_ ]\+[_ ]AST", re.IGNORECASE), "player_pts_ast"),
    (re.compile(r"TO[_ ]RECORD[_ ]\d+\+?[_ ]REB[_ ]\+[_ ]AST", re.IGNORECASE), "player_reb_ast"),
    (re.compile(r"TO[_ ]RECORD[_ ]\d+\+?[_ ]STEALS", re.IGNORECASE), "player_steals"),
    (re.compile(r"TO[_ ]RECORD[_ ]\d+\+?[_ ]BLOCKS", re.IGNORECASE), "player_blocks"),
    # Bovada milestone format: "Points Milestones"
    (re.compile(r"Points Milestones", re.IGNORECASE), "player_points"),
    (re.compile(r"Rebounds Milestones", re.IGNORECASE), "player_rebounds"),
    (re.compile(r"Assists Milestones", re.IGNORECASE), "player_assists"),
    (re.compile(r"Total Made Threes Milestones", re.IGNORECASE), "player_threes"),
]

# ─── Sport-specific micro market patterns ───────────────────────
MICRO_MARKET_PATTERNS = [
    (re.compile(r"HOME[_ ]TEAM[_ ]TOTAL", re.IGNORECASE), "team_total_home"),
    (re.compile(r"AWAY[_ ]TEAM[_ ]TOTAL", re.IGNORECASE), "team_total_away"),
    (re.compile(r"TEAM[_ ]TOTAL", re.IGNORECASE), "team_total"),
    (re.compile(r"SHOTS[_ ]ON[_ ]GOAL[_ ]O(?:VER)?/?U(?:NDER)?", re.IGNORECASE), "hockey_shots_on_goal"),
    (re.compile(r"(?:RACE[_ ]TO|FIRST[_ ]TO)[_ ](\d+)", re.IGNORECASE), "race_to"),
    (re.compile(r"(?:SET[_ ]\d+|1ST[_ ]SET)[_ ]WINNER", re.IGNORECASE), "tennis_set1_winner"),
    (re.compile(r"DOUBLE[_ ]RESULT", re.IGNORECASE), "double_result"),
    (re.compile(r"BOTH[_ ]TEAMS[_ ]TO[_ ]SCORE", re.IGNORECASE), "btts"),
    (re.compile(r"DRAW[_ ]NO[_ ]BET", re.IGNORECASE), "draw_no_bet"),
    (re.compile(r"DOUBLE[_ ]CHANCE", re.IGNORECASE), "double_chance"),
    (re.compile(r"3[- ]WAY[_ ](?:ML|MONEYLINE|RESULT)", re.IGNORECASE), "h2h_3way"),
    (re.compile(r"(?:MATCH[_ ]RESULT|MATCH[_ ]BETTING)[_ ]*\(?3[- ]?WAY\)?", re.IGNORECASE), "h2h_3way"),
    (re.compile(r"ALTERNATE[_ ]SPREAD", re.IGNORECASE), "alternate_spreads"),
    (re.compile(r"ALTERNATE[_ ]TOTAL", re.IGNORECASE), "alternate_totals"),
    (re.compile(r"ALT[_ ]SPREAD", re.IGNORECASE), "alternate_spreads"),
    (re.compile(r"ALT[_ ]TOTAL", re.IGNORECASE), "alternate_totals"),
    # MMA/Boxing: fight to go the distance (Yes/No)
    (re.compile(r"(?:WILL[_ ](?:THE[_ ])?)?FIGHT[_ ]GO(?:ES)?[_ ]THE[_ ]DISTANCE", re.IGNORECASE), "fight_to_go_distance"),
    (re.compile(r"GO(?:ES)?[_ ]THE[_ ]DISTANCE", re.IGNORECASE), "fight_to_go_distance"),
]

# ─── Pinnacle period → suffix mapping (per sport type) ──────────
PINNACLE_PERIOD_MAP = {
    # Basketball: period 0=full, 1=1st half, 2=2nd half, 3-6=quarters
    "basketball": {0: "", 1: "_h1", 2: "_h2", 3: "_q1", 4: "_q2", 5: "_q3", 6: "_q4"},
    # Football: period 0=full, 1=1st half, 2=2nd half, 3-6=quarters
    "football": {0: "", 1: "_h1", 2: "_h2", 3: "_q1", 4: "_q2", 5: "_q3", 6: "_q4"},
    # Hockey: period 0=full, 1=1st period, 2=2nd period, 3=3rd period
    "hockey": {0: "", 1: "_p1", 2: "_p2", 3: "_p3"},
    # Baseball: period 0=full, 1=first 5 innings
    "baseball": {0: "", 1: "_f5"},
    # Soccer: period 0=full, 1=1st half, 2=2nd half
    "soccer": {0: "", 1: "_h1", 2: "_h2"},
    # Tennis: period 0=full, 1=1st set, 2=2nd set, 3=3rd set
    "tennis": {0: "", 1: "_s1", 2: "_s2", 3: "_s3"},
    # MMA/Boxing: period 0 only
    "mma": {0: ""},
    "boxing": {0: ""},
}

# ─── Display names for market keys ──────────────────────────────
MARKET_DISPLAY_NAMES = {
    "h2h": "Moneyline",
    "spreads": "Spread",
    "totals": "Total",
    "totals_sets": "Total Sets",
    "totals_games": "Total Games",
    "h2h_h1": "1H Moneyline",
    "spreads_h1": "1H Spread",
    "totals_h1": "1H Total",
    "h2h_h2": "2H Moneyline",
    "spreads_h2": "2H Spread",
    "totals_h2": "2H Total",
    "h2h_q1": "Q1 Moneyline",
    "spreads_q1": "Q1 Spread",
    "totals_q1": "Q1 Total",
    "h2h_q2": "Q2 Moneyline",
    "spreads_q2": "Q2 Spread",
    "totals_q2": "Q2 Total",
    "h2h_q3": "Q3 Moneyline",
    "spreads_q3": "Q3 Spread",
    "totals_q3": "Q3 Total",
    "h2h_q4": "Q4 Moneyline",
    "spreads_q4": "Q4 Spread",
    "totals_q4": "Q4 Total",
    "h2h_p1": "P1 Moneyline",
    "spreads_p1": "P1 Spread",
    "totals_p1": "P1 Total",
    "h2h_p2": "P2 Moneyline",
    "spreads_p2": "P2 Spread",
    "totals_p2": "P2 Total",
    "h2h_p3": "P3 Moneyline",
    "spreads_p3": "P3 Spread",
    "totals_p3": "P3 Total",
    "h2h_s1": "Set 1 ML",
    "h2h_s2": "Set 2 ML",
    "h2h_s3": "Set 3 ML",
    "h2h_f5": "F5 Moneyline",
    "spreads_f5": "F5 Spread",
    "totals_f5": "F5 Total",
    "player_points": "Player Points",
    "player_rebounds": "Player Rebounds",
    "player_assists": "Player Assists",
    "player_threes": "Player Threes",
    "player_pts_reb_ast": "PRA",
    "player_steals": "Player Steals",
    "player_blocks": "Player Blocks",
    "player_strikeouts": "Strikeouts",
    "player_total_bases": "Total Bases",
    "player_hits": "Player Hits",
    "player_runs": "Player Runs",
    "player_rbis": "Player RBIs",
    "player_shots_on_goal": "Shots on Goal",
    "player_goals": "Player Goals",
    "player_aces": "Player Aces",
    "player_games_won": "Games Won",
    "hockey_shots_on_goal": "Shots on Goal",
    "team_total": "Team Total",
    "team_total_home": "Home Team Total",
    "team_total_away": "Away Team Total",
    "btts": "Both Teams to Score",
    "draw_no_bet": "Draw No Bet",
    "double_chance": "Double Chance",
    "h2h_3way": "3-Way Result",
    "h2h_3way_h1": "1H 3-Way Result",
    "h2h_3way_h2": "2H 3-Way Result",
    "alternate_spreads": "Alternate Spreads",
    "alternate_totals": "Alternate Totals",
    "fight_to_go_distance": "Fight to Go Distance",
}


def get_market_display_name(key: str) -> str:
    """Get a human-readable display name for a market key."""
    return MARKET_DISPLAY_NAMES.get(key, key.replace("_", " ").title())


def detect_period_suffix(raw_market_name: str) -> Tuple[str, str]:
    """
    Detect a period suffix from a raw market type string.
    Returns (cleaned_name, suffix) e.g. ("MONEY_LINE", "_q1").
    If no period detected, suffix is "".
    """
    for pattern, suffix in PERIOD_PATTERNS:
        match = pattern.search(raw_market_name)
        if match:
            # Remove the matched period portion from the name
            cleaned = raw_market_name[:match.start()] + raw_market_name[match.end():]
            # Clean up separators: strip leading/trailing dashes, underscores, spaces
            cleaned = re.sub(r"^[\s_\-]+|[\s_\-]+$", "", cleaned)
            cleaned = re.sub(r"[\s_\-]{2,}", "_", cleaned)
            return cleaned, suffix
    return raw_market_name, ""


def classify_base_market(raw_type: str) -> Optional[str]:
    """
    Classify a raw market type string into a base market key: "h2h", "spreads", or "totals".
    Returns None if no match or if the market is an alternate/alt line.
    """
    upper = raw_type.upper().replace(" ", "_").replace("-", "_")

    # Reject alternate lines — these should NOT be classified as mainline markets
    if "ALTERNATE" in upper or "ALT_SPREAD" in upper or "ALT_TOTAL" in upper:
        return None

    for name in BASE_MONEYLINE:
        if name in upper:
            return "h2h"
    for name in BASE_SPREADS:
        if name in upper:
            return "spreads"
    for name in BASE_TOTALS:
        if name in upper:
            return "totals"
    return None


def classify_market_type(raw_market_name: str) -> Optional[str]:
    """
    Classify a raw market type into a canonical market key.
    Handles both full-game and period markets.

    Examples:
        "MONEY_LINE" -> "h2h"
        "MATCH_HANDICAP_(2-WAY)_-_1ST_QUARTER" -> "spreads_q1"
        "TOTAL_POINTS_(OVER/UNDER)_-_1ST_HALF" -> "totals_h1"
        "PLAYER POINTS OVER/UNDER" -> "player_points"
    """
    # Check player props first
    for pattern, key in PLAYER_PROP_PATTERNS:
        if pattern.search(raw_market_name):
            return key

    # Check micro markets
    for pattern, key in MICRO_MARKET_PATTERNS:
        if pattern.search(raw_market_name):
            return key

    # Detect period suffix and classify base
    cleaned, suffix = detect_period_suffix(raw_market_name)
    base = classify_base_market(cleaned)
    if base:
        return base + suffix

    return None


def get_pinnacle_period_suffix(sport_key: str, period: int) -> Optional[str]:
    """
    Get the market key suffix for a Pinnacle period number.
    Returns "" for full-game (period=0), "_h1"/"_p1"/etc. for periods,
    or None if the period is not relevant for this sport.
    """
    # Determine sport type from sport_key
    sport_type = "basketball"  # default
    if "football" in sport_key or "nfl" in sport_key or "ncaaf" in sport_key or "cfl" in sport_key:
        sport_type = "football"
    elif "hockey" in sport_key or "nhl" in sport_key:
        sport_type = "hockey"
    elif "baseball" in sport_key or "mlb" in sport_key:
        sport_type = "baseball"
    elif "soccer" in sport_key or "epl" in sport_key or "liga" in sport_key or "bundesliga" in sport_key or "serie" in sport_key or "ligue" in sport_key or "champs" in sport_key:
        sport_type = "soccer"
    elif "tennis" in sport_key:
        sport_type = "tennis"
    elif "mma" in sport_key:
        sport_type = "mma"
    elif "boxing" in sport_key:
        sport_type = "boxing"

    period_map = PINNACLE_PERIOD_MAP.get(sport_type, {0: ""})
    return period_map.get(period)


def reclassify_tennis_totals(market_key: str, point: Optional[float]) -> str:
    """Reclassify a tennis 'totals' market into 'totals_sets' or 'totals_games'.

    Tennis has two distinct over/under markets:
    - Total sets (e.g., O/U 2.5) - whether the match goes to 3 sets or ends in 2
    - Total games (e.g., O/U 22.5) - total games played across all sets

    Heuristic: if point <= 5.5, it's total sets; if > 5.5, it's total games.
    """
    if market_key != "totals" or point is None:
        return market_key
    if point <= 5.5:
        return "totals_sets"
    else:
        return "totals_games"


# ─── All known market keys (for frontend reference) ────────────
ALL_MARKET_KEYS = [
    # Full game
    "h2h", "spreads", "totals",
    # Halves
    "h2h_h1", "spreads_h1", "totals_h1",
    "h2h_h2", "spreads_h2", "totals_h2",
    # Quarters
    "h2h_q1", "spreads_q1", "totals_q1",
    "h2h_q2", "spreads_q2", "totals_q2",
    "h2h_q3", "spreads_q3", "totals_q3",
    "h2h_q4", "spreads_q4", "totals_q4",
    # Periods (hockey)
    "h2h_p1", "spreads_p1", "totals_p1",
    "h2h_p2", "spreads_p2", "totals_p2",
    "h2h_p3", "spreads_p3", "totals_p3",
    # Sets (tennis)
    "h2h_s1", "h2h_s2", "h2h_s3",
    # Tennis totals split
    "totals_sets", "totals_games",
    # First 5 innings (baseball)
    "h2h_f5", "spreads_f5", "totals_f5",
    # Player props
    "player_points", "player_rebounds", "player_assists",
    "player_threes", "player_pts_reb_ast",
    "player_steals", "player_blocks",
    "player_strikeouts", "player_total_bases",
    "player_hits", "player_runs", "player_rbis",
    "player_shots_on_goal", "player_goals",
    "player_aces", "player_games_won",
    # Micro markets
    "hockey_shots_on_goal", "team_total", "team_total_home", "team_total_away",
    "btts", "draw_no_bet", "double_chance",
    "h2h_3way", "h2h_3way_h1", "h2h_3way_h2",
    "alternate_spreads", "alternate_totals",
    # MMA/Boxing
    "fight_to_go_distance",
]
