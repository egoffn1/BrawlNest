from typing import Optional, Dict
from datetime import datetime, timezone
from api_client import BrawlAPIClient
from database import Database


class ClubCollector:
    def __init__(self, api: BrawlAPIClient, db: Database):
        self.api = api
        self.db = db

    async def collect(self, tag: str, force_update: bool = False) -> Optional[Dict]:
        normalized = self.api.normalize_tag(tag)
        if not force_update:
            club = await self.db.get_club(normalized)
            if club:
                return club

        data = await self.api.get_club(normalized, force=force_update)
        if not data:
            return None

        # Преобразуем ключи в формат БД
        club_data = {
            "tag": data["tag"],
            "name": data.get("name"),
            "description": data.get("description"),
            "type": data.get("type"),
            "trophies": data.get("trophies"),
            "required_trophies": data.get("requiredTrophies"),
            "members_count": len(data.get("members", [])) or data.get("memberCount", 0),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        await self.db.upsert_club(club_data)

        members_data = await self.api.get_club_members(normalized)
        if members_data and "items" in members_data:
            await self.db.upsert_club_members(normalized, members_data["items"])

        return club_data