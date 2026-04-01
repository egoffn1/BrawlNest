"""Export service — заглушка."""
from utils.logger import setup_logger
logger = setup_logger(__name__)

class ExportService:
    def __init__(self, db, cache=None):
        self.db = db
        self.cache = cache

    async def export_data(self, user_id: str, export_type: str, filters: dict, fmt: str = "json") -> int:
        return await self.db.create_export(user_id, export_type, filters)

    async def get_status(self, export_id: int) -> dict:
        return await self.db.get_export(export_id)
