"""
BrawlNest REST API v2 — Production-ready.
Распределённая отказоустойчивая сеть с GitHub-хранилищем.
"""
from __future__ import annotations

import asyncio
import json
import secrets
import string
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Depends, Query, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse
from pydantic import BaseModel

import database
from auth import verify_api_key
from models import RatingAddRequest
from config import settings
from utils.logger import setup_logger
from utils.sentry import init_sentry
from utils.metrics import init_metrics
from github_client import GitHubClient
from peer import PeerClient
from services.github_service import GitHubService
from services.data_service import DataService
from services.rating_service import RatingService, POINTS_TABLE
from services.search_service import SearchService
from services.webhook_service import WebhookService
from services.export_service import ExportService
from services.admin_service import AdminService
from services.network_service import NetworkService
import background_tasks as bt
from brawl_api_client import BrawlAPIClient

logger = setup_logger(__name__)

# ── Globals ───────────────────────────────────────────────────────────────────
github_client: GitHubClient
cache = None
peer: PeerClient
github_svc: GitHubService
data_svc: DataService
rating_svc: RatingService
search_svc: SearchService
webhook_svc: WebhookService
export_svc: ExportService
admin_svc: AdminService
network_svc: NetworkService
brawl_api: BrawlAPIClient
_bg_tasks: list = []


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global github_client, cache, peer, github_svc, data_svc
    global rating_svc, search_svc, webhook_svc, export_svc
    global admin_svc, network_svc, brawl_api, _bg_tasks

    init_sentry(settings.sentry_dsn)
    if settings.prometheus_enabled:
        init_metrics()

    # Redis / fallback InMemoryCache
    try:
        from utils.redis_cache import RedisCache
        cache = RedisCache(settings.redis_url)
        await cache.redis.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.warning(f"Redis unavailable ({e}), using in-memory cache")
        from utils.cache import InMemoryCache
        cache = InMemoryCache()

    # PostgreSQL
    await database.init_db()

    # GitHub
    github_client = GitHubClient()
    await github_client.start()
    github_svc = GitHubService(github_client, cache)

    # Brawl Stars Official API
    brawl_api = BrawlAPIClient()
    await brawl_api.start()

    # Peer
    peer = PeerClient(cache)
    await peer.register_node()

    # Services
    data_svc    = DataService(database.db, github_svc, brawl_api=brawl_api, cache=cache)
    rating_svc  = RatingService(database.db, cache)
    search_svc  = SearchService(database.db, github_svc, cache)
    webhook_svc = WebhookService(database.db, cache)
    export_svc  = ExportService(database.db, cache)
    admin_svc   = AdminService(database.db, cache, peer)
    network_svc = NetworkService(peer)

    await webhook_svc.start()

    _bg_tasks = bt.start_all(database.db, github_svc, cache, peer, export_svc)

    logger.info(f"BrawlNest API started. Server: {settings.api_server_url}")
    yield

    # Shutdown
    for task in _bg_tasks:
        task.cancel()
    await webhook_svc.close()
    await github_client.close()
    await brawl_api.close()
    if hasattr(cache, "close"):
        await cache.close()
    await database.db.close()
    logger.info("BrawlNest API shutdown complete")


app = FastAPI(
    title="BrawlNest API",
    description="Распределённая статистика Brawl Stars. Репо: egoffn1/BrawlNest",
    version="2.0.0",
    servers=[{"url": settings.api_server_url, "description": "Production"}],
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Routing middleware ───────────────────────────────────────────────────────

@app.middleware("http")
async def route_to_best_node(request: Request, call_next):
    """
    Если текущий узел не является самым быстрым — перенаправляем GET на лучший.
    POST/DELETE обрабатываются локально (GitHub единое хранилище).
    Эндпоинты /ping, /health, /ready, /nodes — всегда локально.
    """
    path = request.url.path
    skip = {"/ping", "/health", "/ready", "/nodes", "/docs", "/openapi.json", "/redoc"}
    if path in skip or request.method != "GET":
        return await call_next(request)

    try:
        best = await peer.get_best_node_address()
        if best and not peer.is_best_node(best):
            # Redirect client to faster node
            qs  = str(request.url.query)
            url = f"{best.rstrip('/')}{path}"
            if qs:
                url += f"?{qs}"
            return RedirectResponse(url=url, status_code=307)
    except Exception:
        pass  # если не удалось — обрабатываем локально

    return await call_next(request)


# ═════════════════════════════════════════════════════════════════════════════
# System endpoints
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/", tags=["System"])
async def root():
    return {
        "name": "BrawlNest API", "version": "2.0.0",
        "docs": "/docs", "server": settings.api_server_url,
        "node": peer.node_address if peer else "unknown",
    }


@app.get("/ping", tags=["System"])
async def ping():
    return {"status": "ok", "node": settings.node_address}


@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/ready", tags=["System"])
async def ready():
    try:
        if hasattr(cache, "redis"):
            await cache.redis.ping()
        if database.db and database.db.pool:
            async with database.db.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
        return {"status": "ready", "db": "ok", "cache": "ok"}
    except Exception as e:
        raise HTTPException(503, f"Not ready: {e}")


@app.get("/nodes", tags=["System"])
async def list_nodes():
    nodes = await peer.get_all_nodes_with_ping()
    return {"current_node": peer.node_address, "nodes": nodes}


# ═════════════════════════════════════════════════════════════════════════════
# Auth / Keys
# ═════════════════════════════════════════════════════════════════════════════

class GenerateKeyRequest(BaseModel):
    name: str = "Public"
    daily_limit: int = 10000


@app.post("/generate_key", tags=["Auth"])
async def generate_key(body: GenerateKeyRequest):
    limit = min(body.daily_limit, settings.default_daily_limit)
    key   = await database.db.generate_api_key(body.name, limit)
    return {"key": key, "name": body.name, "daily_limit": limit,
            "created_at": datetime.now(timezone.utc).isoformat()}


@app.get("/my_status", tags=["Auth"])
async def my_status(api_key: str = Query(...)):
    info = await database.db.get_api_key_info(api_key)
    if not info:
        raise HTTPException(403, "Invalid API Key")
    used = await database.db.get_usage_today(api_key)
    return {
        "key": api_key, "name": info["name"],
        "daily_limit": info["daily_limit"], "used_today": used,
        "remaining": info["daily_limit"] - used,
        "created_at": str(info["created_at"]),
    }


# ═════════════════════════════════════════════════════════════════════════════
# Players
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/players", tags=["Players"])
async def list_players(limit: int = Query(100, le=500), offset: int = 0,
                        api_key: str = Depends(verify_api_key)):
    return await data_svc.list_players(limit, offset)


@app.get("/player/{tag}", tags=["Players"])
async def get_player(tag: str, api_key: str = Depends(verify_api_key)):
    data = await data_svc.get_player(tag)
    if not data:
        raise HTTPException(404, "Player not found")
    return data


@app.get("/player/{tag}/history", tags=["Players"])
async def player_history(tag: str, days: int = Query(30, ge=1, le=365),
                          api_key: str = Depends(verify_api_key)):
    history = await data_svc.get_trophy_history(tag, days)
    if not history:
        raise HTTPException(404, "No history data")
    return {"player_tag": tag.upper().lstrip("#"), "history": history}


@app.get("/player/{tag}/battles", tags=["Players"])
async def player_battles(tag: str, limit: int = Query(20, ge=1, le=100),
                          api_key: str = Depends(verify_api_key)):
    battles = await data_svc.get_battles(tag, limit)
    return {"player_tag": tag.upper().lstrip("#"), "battles": battles}


@app.get("/player/{tag}/battles/stats", tags=["Players"])
async def player_battles_stats(tag: str, api_key: str = Depends(verify_api_key)):
    stats = await data_svc.get_battles_stats(tag)
    return {"player_tag": tag.upper().lstrip("#"), **stats}


@app.get("/player/{tag}/brawlers", tags=["Players"])
async def player_brawlers(tag: str, api_key: str = Depends(verify_api_key)):
    brawlers = await data_svc.get_player_brawlers(tag)
    return {"player_tag": tag.upper().lstrip("#"), "brawlers": brawlers}


@app.get("/player/{tag}/mastery", tags=["Players"])
async def player_mastery(tag: str, api_key: str = Depends(verify_api_key)):
    brawlers = await data_svc.get_player_brawlers(tag)
    mastery  = [
        {"brawler_id": b.get("brawler_id"), "mastery_level": b.get("mastery_level"),
         "mastery_points": b.get("mastery_points")}
        for b in brawlers if b.get("mastery_points") is not None
    ]
    return {"player_tag": tag.upper().lstrip("#"), "mastery": mastery}


@app.get("/compare/players", tags=["Players"])
async def compare_players(tags: str = Query(..., description="Теги через запятую"),
                           api_key: str = Depends(verify_api_key)):
    tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    if len(tag_list) < 2:
        raise HTTPException(400, "Need at least 2 tags")
    return {"players": await data_svc.compare_players(tag_list)}


# ═════════════════════════════════════════════════════════════════════════════
# Clubs
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/clubs", tags=["Clubs"])
async def list_clubs(limit: int = Query(100, le=500), offset: int = 0,
                      api_key: str = Depends(verify_api_key)):
    return await data_svc.list_clubs(limit, offset)


@app.get("/club/{tag}", tags=["Clubs"])
async def get_club(tag: str, api_key: str = Depends(verify_api_key)):
    data = await data_svc.get_club(tag)
    if not data:
        raise HTTPException(404, "Club not found")
    return data


@app.get("/club/{tag}/history", tags=["Clubs"])
async def club_history(tag: str, days: int = Query(30, ge=1, le=365),
                        api_key: str = Depends(verify_api_key)):
    history = await data_svc.get_club_history(tag, days)
    if not history:
        raise HTTPException(404, "No club history")
    return {"club_tag": tag.upper().lstrip("#"), "history": history}


# ═════════════════════════════════════════════════════════════════════════════
# Search
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/search/players", tags=["Search"])
async def search_players(name: str = Query(..., min_length=1),
                          limit: int = Query(20, le=100),
                          min_trophies: Optional[int] = None,
                          api_key: str = Depends(verify_api_key)):
    filters = {"min_trophies": min_trophies} if min_trophies else {}
    results = await search_svc.search_players(name, limit, filters)
    return {"query": name, "results": results, "count": len(results)}


@app.get("/search/clubs", tags=["Search"])
async def search_clubs(name: str = Query(..., min_length=1),
                        limit: int = Query(20, le=100),
                        api_key: str = Depends(verify_api_key)):
    results = await search_svc.search_clubs(name, limit)
    return {"query": name, "results": results, "count": len(results)}


@app.get("/search/advanced", tags=["Search"])
async def advanced_search(query: str = Query(...),
                           search_type: str = Query("players", alias="type"),
                           sort_by: str = "trophies", order: str = "desc",
                           limit: int = Query(20, le=100), offset: int = 0,
                           api_key: str = Depends(verify_api_key)):
    return await search_svc.advanced_search(query, search_type, sort_by, order, limit, offset)


# ═════════════════════════════════════════════════════════════════════════════
# Maps & Teams
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/maps", tags=["Maps"])
async def list_maps(limit: int = Query(50, le=200),
                    api_key: str = Depends(verify_api_key)):
    maps = await data_svc.get_map_stats(limit)
    return {"maps": maps}


@app.get("/maps/{map_name}", tags=["Maps"])
async def get_map(map_name: str, api_key: str = Depends(verify_api_key)):
    stats = await database.db.get_map_stat(map_name)
    if not stats:
        raise HTTPException(404, f"No stats for '{map_name}'")
    return {"map_name": map_name, "stats": stats}


@app.get("/team/stats", tags=["Teams"])
async def team_stats(tags: str = Query(..., description="Теги через запятую"),
                      api_key: str = Depends(verify_api_key)):
    tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    if not (2 <= len(tag_list) <= 3):
        raise HTTPException(400, "Need 2-3 tags")
    stats = await data_svc.get_team_stats(tag_list)
    if not stats:
        return {"player_tags": tag_list, "total_battles": 0, "total_wins": 0,
                "win_rate": 0.0, "note": "No team data yet"}
    return stats


# ═════════════════════════════════════════════════════════════════════════════
# Rankings & Brawlers
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/rankings/players", tags=["Rankings"])
async def rankings_players(limit: int = Query(100, le=500), offset: int = 0,
                            api_key: str = Depends(verify_api_key)):
    cache_key = f"rankings:players:{limit}:{offset}"
    if cache:
        cached = await cache.get(cache_key)
        if cached:
            return cached
    players = await data_svc.get_rankings("players", limit, offset)
    result  = {"limit": limit, "offset": offset, "players": players}
    if cache:
        await cache.set(cache_key, result, ttl=settings.cache_ttl_rankings)
    return result


@app.get("/rankings/clubs", tags=["Rankings"])
async def rankings_clubs(limit: int = Query(100, le=500), offset: int = 0,
                          api_key: str = Depends(verify_api_key)):
    cache_key = f"rankings:clubs:{limit}:{offset}"
    if cache:
        cached = await cache.get(cache_key)
        if cached:
            return cached
    clubs  = await data_svc.get_rankings("clubs", limit, offset)
    result = {"limit": limit, "offset": offset, "clubs": clubs}
    if cache:
        await cache.set(cache_key, result, ttl=settings.cache_ttl_rankings)
    return result


@app.get("/brawlers", tags=["Brawlers"])
async def list_brawlers(api_key: str = Depends(verify_api_key)):
    if brawl_api and brawl_api.api_key:
        data = await brawl_api.get_brawlers()
        if data:
            return data
    return {"brawlers": [], "note": "Brawl API key not configured"}


@app.get("/brawlers/{brawler_id}/rankings", tags=["Brawlers"])
async def brawler_rankings(brawler_id: int, limit: int = Query(100, le=200),
                            api_key: str = Depends(verify_api_key)):
    rankings = await database.db.get_brawler_rankings(brawler_id, limit)
    return {"brawler_id": brawler_id, "rankings": rankings}


# ═════════════════════════════════════════════════════════════════════════════
# Rating
# ═════════════════════════════════════════════════════════════════════════════

@app.post("/rating/add", tags=["Rating"])
async def add_rating(request: RatingAddRequest,
                      api_key: str = Depends(verify_api_key)):
    return await rating_svc.add_rating(api_key, request.action_type, request.object_id)


@app.get("/rating/my", tags=["Rating"])
async def get_my_rating(api_key: str = Depends(verify_api_key)):
    return await rating_svc.get_full(api_key)


@app.get("/rating/leaderboard", tags=["Rating"])
async def rating_leaderboard(limit: int = Query(10, le=50),
                              api_key: str = Depends(verify_api_key)):
    return {"leaderboard": await rating_svc.get_leaderboard(limit)}


# ═════════════════════════════════════════════════════════════════════════════
# Team Codes
# ═════════════════════════════════════════════════════════════════════════════

class GenerateCodeRequest(BaseModel):
    duration_seconds: int = 120


@app.post("/generate_team_code", tags=["Team Codes"])
async def generate_team_code(body: GenerateCodeRequest,
                              api_key: str = Depends(verify_api_key)):
    """Генерация уникального кода команды (X + 2-3 случайных символа)."""
    duration = min(max(body.duration_seconds, 10), 300)
    alphabet  = string.ascii_uppercase + string.digits

    code = None
    for _ in range(5):
        length    = secrets.choice([2, 3])
        candidate = "X" + "".join(secrets.choice(alphabet) for _ in range(length))

        # Check uniqueness in Redis
        if cache:
            if await cache.exists(f"team_code:{candidate}"):
                continue
        # Check uniqueness in GitHub
        existing = await github_svc.get_team_code(candidate)
        if existing:
            # Check if already expired
            try:
                exp = datetime.fromisoformat(existing["expires_at"].replace("Z", "+00:00"))
                if datetime.now(timezone.utc) <= exp:
                    continue  # still active
            except Exception:
                pass

        code = candidate
        break

    if not code:
        raise HTTPException(503, "Could not generate unique code, try again")

    now        = datetime.now(timezone.utc)
    expires_at = datetime.fromtimestamp(now.timestamp() + duration, tz=timezone.utc)

    code_data = {
        "code": code,
        "created_at": now.isoformat(),
        "expires_at": expires_at.isoformat(),
        "creator_api_key": api_key[:8] + "...",
        "duration_seconds": duration,
    }

    # Save to GitHub
    asyncio.create_task(github_svc.upsert_team_code(code, code_data))

    # Save to Redis
    if cache:
        await cache.set(f"team_code:{code}", code_data, ttl=duration)

    return {"code": code, "expires_at": expires_at.isoformat(), "duration_seconds": duration}


@app.get("/team_code/{code}", tags=["Team Codes"])
async def get_team_code(code: str, api_key: str = Depends(verify_api_key)):
    """Проверка активности кода команды."""
    now = datetime.now(timezone.utc)

    # Check Redis
    if cache:
        cached = await cache.get(f"team_code:{code}")
        if cached:
            return {"code": code, "active": True, **cached}

    # Check GitHub
    data = await github_svc.get_team_code(code)
    if not data:
        return {"code": code, "active": False}

    try:
        expires_at = datetime.fromisoformat(data["expires_at"].replace("Z", "+00:00"))
    except Exception:
        return {"code": code, "active": False}

    if now > expires_at:
        asyncio.create_task(github_svc.delete_team_code(code))
        if cache:
            await cache.delete(f"team_code:{code}")
        return {"code": code, "active": False, "expired": True}

    # Restore Redis
    ttl_left = int((expires_at - now).total_seconds())
    if cache and ttl_left > 0:
        await cache.set(f"team_code:{code}", data, ttl=ttl_left)

    return {"code": code, "active": True, **data}


# ═════════════════════════════════════════════════════════════════════════════
# Network
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/network/status", tags=["Network"])
async def network_status():
    return await network_svc.get_status()


@app.get("/network/peers", tags=["Network"])
async def network_peers():
    nodes = await peer.get_all_nodes_with_ping()
    return {"peers": nodes}


@app.post("/network/connect", tags=["Network"])
async def network_connect(node_url: str = Query(...),
                           api_key: str = Depends(verify_api_key)):
    await cache.sadd("nodes:active_manual", node_url)
    return {"status": "registered", "node": node_url}


# ═════════════════════════════════════════════════════════════════════════════
# Webhooks
# ═════════════════════════════════════════════════════════════════════════════

class WebhookCreateRequest(BaseModel):
    url: str
    events: List[str]
    secret: Optional[str] = None


@app.post("/webhooks", tags=["Webhooks"])
async def create_webhook(body: WebhookCreateRequest,
                          api_key: str = Depends(verify_api_key)):
    wh_id = await webhook_svc.subscribe(body.url, body.events, body.secret)
    return {"id": wh_id, "url": body.url, "events": body.events, "status": "active"}


@app.delete("/webhooks/{wh_id}", tags=["Webhooks"])
async def delete_webhook(wh_id: int, api_key: str = Depends(verify_api_key)):
    await webhook_svc.unsubscribe(wh_id)
    return {"status": "deleted"}


# ═════════════════════════════════════════════════════════════════════════════
# Exports
# ═════════════════════════════════════════════════════════════════════════════

class ExportCreateRequest(BaseModel):
    export_type: str
    filters: Dict[str, Any] = {}
    format: str = "json"


@app.post("/exports", tags=["Exports"])
async def create_export(body: ExportCreateRequest,
                         api_key: str = Depends(verify_api_key)):
    export_id = await export_svc.export_data(api_key, body.export_type, body.filters, body.format)
    return {"export_id": export_id, "status": "pending"}


@app.get("/exports/{export_id}", tags=["Exports"])
async def get_export_status(export_id: int, api_key: str = Depends(verify_api_key)):
    status = await export_svc.get_status(export_id)
    if not status:
        raise HTTPException(404, "Export not found")
    return status


# ═════════════════════════════════════════════════════════════════════════════
# Admin
# ═════════════════════════════════════════════════════════════════════════════

def _check_admin(x_admin_secret: str = Header(..., alias="X-Admin-Secret")):
    if x_admin_secret != settings.admin_secret:
        raise HTTPException(403, "Invalid admin secret")
    return x_admin_secret


@app.get("/admin/keys", tags=["Admin"])
async def admin_list_keys(_=Depends(_check_admin)):
    return {"keys": await admin_svc.list_keys()}


@app.post("/admin/keys", tags=["Admin"])
async def admin_create_key(name: str, daily_limit: int = 10000,
                            _=Depends(_check_admin)):
    key = await admin_svc.create_key(name, daily_limit)
    return {"key": key, "name": name, "daily_limit": daily_limit}


@app.patch("/admin/keys/{key}", tags=["Admin"])
async def admin_set_limit(key: str, daily_limit: int, _=Depends(_check_admin)):
    await admin_svc.set_limit(key, daily_limit)
    return {"status": "updated"}


@app.delete("/admin/keys/{key}", tags=["Admin"])
async def admin_delete_key(key: str, _=Depends(_check_admin)):
    await admin_svc.deactivate_key(key)
    return {"status": "deleted"}


@app.get("/admin/stats", tags=["Admin"])
async def admin_stats(_=Depends(_check_admin)):
    return await admin_svc.get_stats()


@app.get("/admin/queues", tags=["Admin"])
async def admin_queues(_=Depends(_check_admin)):
    return await admin_svc.get_queue_info()


@app.delete("/admin/team_code/{code}", tags=["Admin"])
async def admin_delete_team_code(code: str, _=Depends(_check_admin)):
    ok = await github_svc.delete_team_code(code)
    if cache:
        await cache.delete(f"team_code:{code}")
    return {"status": "deleted" if ok else "not_found"}


# ═════════════════════════════════════════════════════════════════════════════
# Internal (node-to-node)
# ═════════════════════════════════════════════════════════════════════════════

def _check_node(request: Request):
    if request.headers.get("X-Node-Secret") != settings.node_secret:
        raise HTTPException(403, "Unauthorized")


@app.get("/player_internal/{tag}", include_in_schema=False)
async def player_internal(tag: str, request: Request):
    _check_node(request)
    return await data_svc.get_player(tag) or {}


@app.get("/club_internal/{tag}", include_in_schema=False)
async def club_internal(tag: str, request: Request):
    _check_node(request)
    return await data_svc.get_club(tag) or {}


@app.get("/players_list_internal", include_in_schema=False)
async def players_list_internal(request: Request):
    _check_node(request)
    return await github_svc.list_players()


@app.get("/clubs_list_internal", include_in_schema=False)
async def clubs_list_internal(request: Request):
    _check_node(request)
    return await github_svc.list_clubs()
