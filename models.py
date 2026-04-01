"""
Pydantic v2 модели для всех эндпоинтов API.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Any, Dict
from datetime import date, datetime


# ─── Player ──────────────────────────────────────────────────────────────────

class Player(BaseModel):
    tag: str
    name: str
    trophies: int
    highest_trophies: Optional[int] = None
    exp_level: Optional[int] = None
    exp_points: Optional[int] = None
    wins_3v3: Optional[int] = None
    wins_solo: Optional[int] = None
    wins_duo: Optional[int] = None
    club_tag: Optional[str] = None


class PlayerBrawlerStat(BaseModel):
    brawler_id: int
    brawler_name: Optional[str] = None
    trophies: int
    highest_trophies: int
    power: int
    rank: int


# ─── Club ────────────────────────────────────────────────────────────────────

class Club(BaseModel):
    tag: str
    name: str
    trophies: int
    required_trophies: Optional[int] = None
    members_count: Optional[int] = None
    type: Optional[str] = None
    description: Optional[str] = None
    members: Optional[List[Dict]] = None


# ─── API Keys ────────────────────────────────────────────────────────────────

class ApiKeyCreate(BaseModel):
    name: str
    daily_limit: Optional[int] = None


class ApiKeyResponse(BaseModel):
    key: str
    name: str
    daily_limit: int
    created_at: str


# ─── Trophy History ──────────────────────────────────────────────────────────

class TrophyHistoryPoint(BaseModel):
    date: date
    trophies: int


class TrophyHistoryResponse(BaseModel):
    player_tag: str
    history: List[TrophyHistoryPoint]


# ─── Club History ────────────────────────────────────────────────────────────

class ClubHistoryPoint(BaseModel):
    date: date
    trophies: int
    member_count: int
    required_trophies: Optional[int] = None


class ClubHistoryResponse(BaseModel):
    club_tag: str
    history: List[ClubHistoryPoint]


# ─── Battles ─────────────────────────────────────────────────────────────────

class Battle(BaseModel):
    battle_time: datetime
    battle_type: Optional[str] = None
    result: Optional[str] = None
    trophies_change: Optional[int] = None
    brawler_id: Optional[int] = None
    map_name: Optional[str] = None
    game_mode: Optional[str] = None
    teammates: Optional[List[str]] = None
    opponents: Optional[List[str]] = None


class BattleModeStats(BaseModel):
    game_mode: Optional[str] = None
    total: int
    wins: int


class BattleMapStats(BaseModel):
    map_name: Optional[str] = None
    total: int
    wins: int
    avg_trophies_change: Optional[float] = None


class BattleStatsResponse(BaseModel):
    player_tag: str
    total_battles: int
    total_wins: int
    by_mode: List[BattleModeStats]
    by_map: List[BattleMapStats]


# ─── Brawler Rankings ────────────────────────────────────────────────────────

class BrawlerRanking(BaseModel):
    player_tag: str
    trophies: int


# ─── Map Stats ───────────────────────────────────────────────────────────────

class MapStat(BaseModel):
    map_name: str
    game_mode: str
    total_battles: int
    total_wins: int
    avg_trophies_change: float
    win_rate: float


# ─── Team Stats ──────────────────────────────────────────────────────────────

class TeamStatsResponse(BaseModel):
    player_tags: List[str]
    total_battles: int
    total_wins: int
    win_rate: float
    last_updated: Optional[datetime] = None


# ─── Rankings ────────────────────────────────────────────────────────────────

class PlayerRankingEntry(BaseModel):
    player_tag: str
    trophies: int
    date: Optional[date] = None


class ClubRankingEntry(BaseModel):
    tag: str
    name: Optional[str] = None
    trophies: int


class PaginatedResponse(BaseModel):
    total: int
    limit: int
    offset: int
    data: List[Any]


# ─── Rating ──────────────────────────────────────────────────────────────────

class RatingAddRequest(BaseModel):
    action_type: str
    object_id: Optional[str] = None
