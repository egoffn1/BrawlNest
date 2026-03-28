"""
REST API на FastAPI — продажа доступа к данным Brawl Stars.
Запуск: uvicorn api.rest_api:app --host 0.0.0.0 --port 8000
"""
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Annotated
import asyncio

from config import API_KEYS, SERVER_CFG
from database import Database
from api_client import BrawlAPIClient
from collectors.player_collector import PlayerCollector
from collectors.club_collector import ClubCollector
from utils.logger import setup_logger

logger = setup_logger("api")

app = FastAPI(
    title="Brawl Stats API",
    description="REST API для получения статистики Brawl Stars",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Зависимости ──────────────────────────────────────────────────────────────
_db:  Optional[Database]         = None
_api: Optional[BrawlAPIClient]   = None


@app.on_event("startup")
async def startup():
    global _db, _api
    _db  = Database()
    await _db.connect()
    _api = BrawlAPIClient()
    logger.info("REST API started")


@app.on_event("shutdown")
async def shutdown():
    if _db:  await _db.close()
    if _api: await _api.close()


def get_db()  -> Database:         return _db   # type: ignore
def get_api() -> BrawlAPIClient:   return _api  # type: ignore


# Простая проверка API-ключа клиента (передаётся как заголовок X-Api-Key)
# В реальной монетизации здесь должна быть БД клиентских ключей.
def verify_api_key(x_api_key: Annotated[str, Header()] = ""):
    # Для простоты принимаем любой непустой ключ (замените на реальную логику)
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Нужен X-Api-Key")
    return x_api_key


# ── Эндпоинты ────────────────────────────────────────────────────────────────

@app.get("/", tags=["info"])
async def root():
    return {"name": "Brawl Stats API", "version": "2.0.0", "keys": len(API_KEYS)}


@app.get("/player/{tag}", tags=["players"])
async def get_player(
    tag: str,
    force: bool = False,
    db: Database = Depends(get_db),
    api: BrawlAPIClient = Depends(get_api),
    _: str = Depends(verify_api_key),
):
    col  = PlayerCollector(api, db)
    data = await col.collect(tag, force_update=force)
    if not data:
        raise HTTPException(404, "Игрок не найден")
    return data


@app.get("/player/{tag}/battles", tags=["players"])
async def get_battles(
    tag: str,
    limit: int = 10,
    db: Database = Depends(get_db),
    api: BrawlAPIClient = Depends(get_api),
    _: str = Depends(verify_api_key),
):
    col = PlayerCollector(api, db)
    await col.collect(tag)
    battles = await db.get_battles(tag.replace("#", ""), limit=min(limit, 50))
    return {"items": battles}


@app.get("/club/{tag}", tags=["clubs"])
async def get_club(
    tag: str,
    db: Database = Depends(get_db),
    api: BrawlAPIClient = Depends(get_api),
    _: str = Depends(verify_api_key),
):
    col  = ClubCollector(api, db)
    data = await col.collect(tag)
    if not data:
        raise HTTPException(404, "Клуб не найден")
    members = await db.get_club_members(data["tag"])
    return {**data, "members": members}


@app.get("/rankings/{region}/players", tags=["rankings"])
async def get_rankings_players(
    region: str = "global",
    api: BrawlAPIClient = Depends(get_api),
    _: str = Depends(verify_api_key),
):
    data = await api.get_rankings_players(region)
    if not data:
        raise HTTPException(404, "Рейтинг не найден")
    return data


@app.get("/rankings/{region}/clubs", tags=["rankings"])
async def get_rankings_clubs(
    region: str = "global",
    api: BrawlAPIClient = Depends(get_api),
    _: str = Depends(verify_api_key),
):
    data = await api.get_rankings_clubs(region)
    if not data:
        raise HTTPException(404, "Рейтинг не найден")
    return data


@app.get("/events/rotation", tags=["events"])
async def get_rotation(api: BrawlAPIClient = Depends(get_api)):
    data = await api.get_event_rotation()
    return data or {"items": []}


@app.get("/brawlers", tags=["brawlers"])
async def get_brawlers(api: BrawlAPIClient = Depends(get_api)):
    data = await api.get_brawlers()
    return data or {"items": []}
