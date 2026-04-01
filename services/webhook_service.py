"""Webhook service — заглушка с базовой реализацией."""
from utils.logger import setup_logger
logger = setup_logger(__name__)

class WebhookService:
    def __init__(self, db, cache=None):
        self.db = db
        self.cache = cache

    async def start(self): pass
    async def close(self): pass

    async def subscribe(self, url: str, events: list, secret: str = None) -> int:
        return await self.db.add_webhook(url, events, secret)

    async def unsubscribe(self, wh_id: int):
        await self.db.delete_webhook(wh_id)

    async def trigger(self, event: str, payload: dict):
        hooks = await self.db.get_active_webhooks(event)
        for hook in hooks:
            try:
                import aiohttp, json
                async with aiohttp.ClientSession() as sess:
                    await sess.post(hook["url"], json={"event": event, "data": payload}, timeout=5)
            except Exception as e:
                logger.warning(f"Webhook delivery failed {hook['url']}: {e}")
