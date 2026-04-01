from typing import Optional, Dict
from datetime import datetime, timezone
from api_client import BrawlAPIClient
from database import Database
from remote_storage import storage as remote_storage


class PlayerCollector:
    def __init__(self, api: BrawlAPIClient, db: Database):
        self.api = api
        self.db = db

    async def collect(self, tag: str, force_update: bool = False) -> Optional[Dict]:
        normalized = self.api.normalize_tag(tag)
        if not force_update and await self.db.is_player_fresh(normalized):
            return await self.db.get_player(normalized)

        data = await self.api.get_player(normalized, force=force_update)
        if not data:
            return None

        # Преобразуем ключи в формат, соответствующий таблице players
        player_data = {
            "tag": data["tag"],  # содержит '#'
            "name": data.get("name"),
            "name_color": data.get("nameColor"),
            "icon_id": data.get("icon", {}).get("id"),
            "trophies": data.get("trophies"),
            "highest_trophies": data.get("highestTrophies"),
            "exp_level": data.get("expLevel"),
            "exp_points": data.get("expPoints"),
            "wins_3v3": data.get("3vs3Victories", 0),
            "wins_solo": data.get("soloVictories", 0),
            "wins_duo": data.get("duoVictories", 0),
            "best_robo_rumble_time": data.get("bestRoboRumbleTime"),
            "best_time_as_big_brawler": data.get("bestTimeAsBigBrawler"),
            "club_tag": data.get("club", {}).get("tag"),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

        await self.db.upsert_player(player_data)
        
        # Сохраняем полные данные игрока в удаленное хранилище
        safe_tag = normalized.replace("#", "")
        remote_storage.write_data("players", safe_tag, data)

        battlelog = await self.api.get_battlelog(normalized, force=force_update)
        if battlelog and "items" in battlelog:
            for battle in battlelog["items"]:
                await self.db.upsert_battle(normalized, battle)
                
                # Сохраняем бои в удаленное хранилище (группируем по тегу игрока)
                battle_with_meta = {
                    **battle,
                    "player_tag": normalized,
                    "collected_at": datetime.now(timezone.utc).isoformat()
                }
                # Используем комбинацию тега и времени боя как идентификатор
                battle_time = battle.get("battleTime", "")[:16].replace("-", "").replace(":", "")
                remote_storage.write_data("battles", f"{safe_tag}_{battle_time}", battle_with_meta)

        # Коммитим изменения пакетно
        remote_storage.commit_changes(f"Обновлены данные игрока {normalized}")

        return player_data