"""Admin service."""
from utils.logger import setup_logger
logger = setup_logger(__name__)

class AdminService:
    def __init__(self, db, cache=None, peer=None):
        self.db = db
        self.cache = cache
        self.peer = peer

    async def list_keys(self):
        return await self.db.get_all_api_keys()

    async def create_key(self, name: str, daily_limit: int) -> str:
        return await self.db.generate_api_key(name, daily_limit)

    async def set_limit(self, key: str, daily_limit: int):
        await self.db.set_key_limit(key, daily_limit)

    async def deactivate_key(self, key: str):
        await self.db.deactivate_key(key)

    async def get_stats(self) -> dict:
        keys = await self.db.get_all_api_keys()
        lb   = await self.db.get_rating_leaderboard(5)
        return {"total_keys": len(keys), "top_rated": lb}

    async def get_queue_info(self) -> dict:
        return {"queues": [], "note": "No queue system implemented"}
