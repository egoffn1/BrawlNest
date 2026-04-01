from typing import Optional, Dict
from datetime import datetime, timezone
from api_client import BrawlAPIClient
from database import Database
from remote_storage import storage as remote_storage


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
        
        # Сохраняем полные данные клуба в удаленное хранилище
        safe_tag = normalized.replace("#", "")
        remote_storage.write_data("clubs", safe_tag, data)

        members_data = await self.api.get_club_members(normalized)
        if members_data and "items" in members_data:
            await self.db.upsert_club_members(normalized, members_data["items"])
            
            # Сохраняем участников клуба в удаленное хранилище
            members_with_meta = {
                "club_tag": normalized,
                "members": members_data["items"],
                "collected_at": datetime.now(timezone.utc).isoformat()
            }
            remote_storage.write_data("clubs", f"{safe_tag}_members", members_with_meta)

        # Коммитим изменения пакетно
        remote_storage.commit_changes(f"Обновлены данные клуба {normalized}")

        return club_data