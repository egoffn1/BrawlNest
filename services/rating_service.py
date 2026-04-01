"""Рейтинговая система пользователей."""
from __future__ import annotations
from typing import Dict, List, Optional

POINTS_TABLE: Dict[str, int] = {
    "player_view": 1, "battles_view": 1, "club_view": 1,
    "search_name": 5, "save_png": 5, "full_club_collect": 10,
    "check_team": 10, "search_players": 2, "fill_db": 20,
    "continuous_fill": 20, "sync_push": 5, "sync_pull": 5,
    "generate_codes": 2, "check_active_players": 2,
    "search_existing_clubs": 2, "check_players_file": 2,
    "brawlers_view": 1, "rotation_view": 1, "rankings_view": 1,
    "powerplay_view": 1, "locations_view": 1, "search_club_name": 5,
    "check_club": 1,
}


class RatingService:
    def __init__(self, db, cache=None):
        self.db    = db
        self.cache = cache

    async def add_rating(self, api_key: str, action_type: str,
                          object_id: Optional[str] = None) -> Dict:
        points  = POINTS_TABLE.get(action_type, 1)
        success = await self.db.add_rating_with_limit(
            api_key, points, action_type, object_id, cooldown_seconds=300
        )
        if not success:
            return {"success": False, "reason": "cooldown", "points": 0}
        return {"success": True, "points_awarded": points, "action_type": action_type}

    async def get_full(self, api_key: str) -> Dict:
        rating = await self.db.get_rating(api_key)
        return {"api_key": api_key, "rating": rating}

    async def get_leaderboard(self, limit: int = 10) -> List[Dict]:
        return await self.db.get_rating_leaderboard(limit)
