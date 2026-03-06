from typing import List, Optional, Union

from pydantic import BaseModel


class Outcome(BaseModel):
    name: str
    price: Union[int, float]
    point: Optional[float] = None
    liquidity: Optional[float] = None  # Available liquidity in USD at this price (P2P exchanges)
    rotation_number: Optional[int] = None  # Sportsbook rotation/bet number (e.g. 523)


class Market(BaseModel):
    key: str  # "h2h", "spreads", "totals", "h2h_h1", "spreads_q1", "player_points", etc.
    last_update: Optional[str] = None
    outcomes: List[Outcome]
    liquidity: Optional[float] = None  # Total available liquidity in USD (prediction markets)


class Bookmaker(BaseModel):
    key: str  # e.g. "fanduel"
    title: str  # e.g. "FanDuel"
    last_update: Optional[str] = None
    markets: List[Market]
    event_url: Optional[str] = None  # Direct link to event on sportsbook


class ScoreData(BaseModel):
    home_score: Optional[str] = None
    away_score: Optional[str] = None
    status: Optional[str] = None  # "pre", "in", "post"
    detail: Optional[str] = None  # "Q3 5:24", "Final", "Half"
    period: Optional[int] = None
    clock: Optional[str] = None


class OddsEvent(BaseModel):
    id: str
    sport_key: str  # e.g. "basketball_nba"
    sport_title: str  # e.g. "NBA"
    commence_time: str  # ISO 8601
    home_team: str
    away_team: str
    bookmakers: List[Bookmaker]
    score_data: Optional[ScoreData] = None


class PlayerProp(BaseModel):
    """A single player prop line from one bookmaker."""
    player_name: str
    stat_type: str       # "points", "rebounds", "assists", "threes", "pts_reb_ast"
    line: float          # Threshold: 25.0 means "25+ points" (or O/U line)
    price: int           # American odds, e.g. -188
    description: Optional[str] = None  # "Over" or "Under" for O/U props, None for threshold
    bookmaker_key: str
    bookmaker_title: str
    event_url: Optional[str] = None
