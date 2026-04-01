"""
Фоновые задачи BrawlNest:
- Сбор истории трофеев игроков/клубов (каждые 6ч)
- Пересчёт статистики карт из GitHub battles/ (каждый час)
- Обновление рейтингов (раз в сутки)
- Обновление командной статистики (каждый час)
- Построение индексов имён (каждые 2ч)
- Очистка просроченных командных кодов (каждые 5 минут)
- Измерение пингов узлов (каждые 30 секунд)
"""
import asyncio
import hashlib
import json
from datetime import datetime, timezone, date
from typing import Optional
from utils.logger import setup_logger

logger = setup_logger(__name__)


def start_all(db, github_svc, cache, peer, export_svc) -> list:
    tasks = [
        asyncio.create_task(_heartbeat_loop(peer)),
        asyncio.create_task(_ping_loop(peer)),
        asyncio.create_task(_trophy_collection_loop(db, github_svc, cache)),
        asyncio.create_task(_club_history_loop(db, github_svc, cache)),
        asyncio.create_task(_map_stats_loop(db, github_svc, cache)),
        asyncio.create_task(_rankings_loop(db, github_svc, cache)),
        asyncio.create_task(_team_stats_loop(db, github_svc, cache)),
        asyncio.create_task(_name_index_loop(github_svc, cache)),
        asyncio.create_task(_clean_team_codes_loop(github_svc, cache)),
    ]
    return tasks


# ─── Heartbeat & Ping ─────────────────────────────────────────────────────────

async def _heartbeat_loop(peer):
    await asyncio.sleep(10)
    while True:
        try:
            await peer.heartbeat()
        except Exception as e:
            logger.warning(f"Heartbeat error: {e}")
        await asyncio.sleep(30)


async def _ping_loop(peer):
    await asyncio.sleep(15)
    while True:
        try:
            await peer.measure_pings()
        except Exception as e:
            logger.warning(f"Ping measure error: {e}")
        await asyncio.sleep(30)


# ─── Trophy History ───────────────────────────────────────────────────────────

async def _trophy_collection_loop(db, github_svc, cache):
    await asyncio.sleep(60)
    while True:
        token = await cache.acquire_lock("trophy_collection", ttl=3600) if cache else "ok"
        if token:
            try:
                await _collect_trophy_history(db, github_svc)
            except Exception as e:
                logger.error(f"Trophy collection error: {e}")
            finally:
                if cache and token != "ok":
                    await cache.release_lock("trophy_collection", token)
        await asyncio.sleep(6 * 3600)


async def _collect_trophy_history(db, github_svc):
    today = date.today()
    logger.info("Starting trophy history collection...")
    player_list = await github_svc.list_players()
    all_players = await github_svc.bulk_fetch_players(player_list)

    records = []
    for p in all_players:
        tag      = p.get("tag", "").upper().lstrip("#")
        trophies = p.get("trophies")
        if tag and trophies is not None:
            records.append({"player_tag": tag, "date": today, "trophies": trophies})
            brawlers = p.get("brawlers", [])
            if brawlers:
                asyncio.create_task(db.upsert_player_brawler_stats(tag, brawlers))
            # Upsert trophy_history JSON in GitHub
            gh_history = await github_svc.get_trophy_history(tag) or []
            today_str  = today.isoformat()
            # Replace or append today's entry
            gh_history = [e for e in gh_history if e.get("date") != today_str]
            gh_history.append({"date": today_str, "trophies": trophies})
            asyncio.create_task(github_svc.upsert_trophy_history(tag, gh_history))

    await db.batch_add_trophy_history(records)
    logger.info(f"Trophy history: {len(records)} players")


# ─── Club History ─────────────────────────────────────────────────────────────

async def _club_history_loop(db, github_svc, cache):
    await asyncio.sleep(90)
    while True:
        token = await cache.acquire_lock("club_history", ttl=3600) if cache else "ok"
        if token:
            try:
                await _collect_club_history(db, github_svc)
            except Exception as e:
                logger.error(f"Club history error: {e}")
            finally:
                if cache and token != "ok":
                    await cache.release_lock("club_history", token)
        await asyncio.sleep(6 * 3600)


async def _collect_club_history(db, github_svc):
    today = date.today()
    club_list = await github_svc.list_clubs()
    all_clubs = await github_svc.bulk_fetch_clubs(club_list)

    records = []
    for c in all_clubs:
        tag      = c.get("tag", "").upper().lstrip("#")
        trophies = c.get("trophies")
        members  = c.get("members", [])
        if tag and trophies is not None:
            records.append({
                "club_tag": tag, "date": today, "trophies": trophies,
                "member_count": len(members),
                "required_trophies": c.get("requiredTrophies"),
            })
            today_str  = today.isoformat()
            gh_history = await github_svc.get_club_history(tag) or []
            gh_history  = [e for e in gh_history if e.get("date") != today_str]
            gh_history.append({"date": today_str, "trophies": trophies,
                                "member_count": len(members),
                                "required_trophies": c.get("requiredTrophies")})
            asyncio.create_task(github_svc.upsert_club_history(tag, gh_history))

    await db.batch_add_club_history(records)
    logger.info(f"Club history: {len(records)} clubs")


# ─── Map Stats ────────────────────────────────────────────────────────────────

async def _map_stats_loop(db, github_svc, cache):
    await asyncio.sleep(120)
    while True:
        token = await cache.acquire_lock("map_stats_refresh", ttl=300) if cache else "ok"
        if token:
            try:
                await db.refresh_map_stats()
                # Sync to GitHub
                stats = await db.get_map_stats(200)
                if stats:
                    asyncio.create_task(github_svc.upsert_map_stats(stats))
                logger.info("Map stats refreshed")
            except Exception as e:
                logger.error(f"Map stats error: {e}")
            finally:
                if cache and token != "ok":
                    await cache.release_lock("map_stats_refresh", token)
        await asyncio.sleep(3600)


# ─── Rankings ─────────────────────────────────────────────────────────────────

async def _rankings_loop(db, github_svc, cache):
    await asyncio.sleep(180)
    while True:
        token = await cache.acquire_lock("rankings_update", ttl=600) if cache else "ok"
        if token:
            try:
                today_str = date.today().isoformat()
                # Players
                rows = await db.get_top_players_from_trophy_history(500, 0)
                players_data = {
                    "date": today_str,
                    "players": [{"tag": r["player_tag"], "trophies": r["trophies"]} for r in rows],
                }
                asyncio.create_task(github_svc.upsert_rankings_players(today_str, players_data))

                # Clubs
                clubs    = await github_svc.list_clubs()
                all_c    = await github_svc.bulk_fetch_clubs(clubs)
                ranked   = sorted(all_c, key=lambda x: x.get("trophies", 0), reverse=True)
                clubs_data = {
                    "date": today_str,
                    "clubs": [{"tag": c.get("tag",""), "name": c.get("name",""), "trophies": c.get("trophies",0)} for c in ranked],
                }
                asyncio.create_task(github_svc.upsert_rankings_clubs(today_str, clubs_data))
                logger.info("Rankings updated")
            except Exception as e:
                logger.error(f"Rankings update error: {e}")
            finally:
                if cache and token != "ok":
                    await cache.release_lock("rankings_update", token)
        await asyncio.sleep(24 * 3600)


# ─── Team Stats ───────────────────────────────────────────────────────────────

async def _team_stats_loop(db, github_svc, cache):
    await asyncio.sleep(200)
    while True:
        token = await cache.acquire_lock("team_stats_update", ttl=600) if cache else "ok"
        if token:
            try:
                await _update_team_stats(db, github_svc)
            except Exception as e:
                logger.error(f"Team stats error: {e}")
            finally:
                if cache and token != "ok":
                    await cache.release_lock("team_stats_update", token)
        await asyncio.sleep(3600)


async def _update_team_stats(db, github_svc):
    player_list = await github_svc.list_players()
    for p in player_list:
        tag = p.get("tag", "").upper().lstrip("#")
        battles = await github_svc.get_battles(tag)
        if not battles:
            continue
        for battle in battles:
            teammates = battle.get("teammates", [])
            if not teammates:
                continue
            all_tags = [tag] + [t.lstrip("#") for t in teammates]
            if len(all_tags) < 2:
                continue
            won   = battle.get("result") == "victory"
            th    = hashlib.sha256(",".join(sorted(all_tags)).encode()).hexdigest()[:16]
            gh_ts = await github_svc.get_team_stats(th) or {
                "player_tags": all_tags, "total_battles": 0, "total_wins": 0,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            gh_ts["total_battles"] = gh_ts.get("total_battles", 0) + 1
            if won:
                gh_ts["total_wins"] = gh_ts.get("total_wins", 0) + 1
            gh_ts["last_updated"] = datetime.now(timezone.utc).isoformat()
            asyncio.create_task(github_svc.upsert_team_stats(th, gh_ts))


# ─── Name Indexes ─────────────────────────────────────────────────────────────

async def _name_index_loop(github_svc, cache):
    await asyncio.sleep(30)
    while True:
        token = await cache.acquire_lock("name_index_build", ttl=600) if cache else "ok"
        if token:
            try:
                player_index = await github_svc.build_name_index()
                if player_index and cache:
                    await cache.hmset_dict("idx:player_names", player_index, ttl=7200)
                club_index = await github_svc.build_club_name_index()
                if club_index and cache:
                    await cache.hmset_dict("idx:club_names", club_index, ttl=7200)
                logger.info(f"Name indexes: {len(player_index)} players, {len(club_index)} clubs")
            except Exception as e:
                logger.error(f"Name index build error: {e}")
            finally:
                if cache and token != "ok":
                    await cache.release_lock("name_index_build", token)
        await asyncio.sleep(2 * 3600)


# ─── Team Codes Cleanup ───────────────────────────────────────────────────────

async def _clean_team_codes_loop(github_svc, cache):
    await asyncio.sleep(60)
    while True:
        token = await cache.acquire_lock("clean_team_codes", ttl=120) if cache else "ok"
        if token:
            try:
                await _clean_expired_codes(github_svc, cache)
            except Exception as e:
                logger.error(f"Clean team codes error: {e}")
            finally:
                if cache and token != "ok":
                    await cache.release_lock("clean_team_codes", token)
        await asyncio.sleep(5 * 60)


async def _clean_expired_codes(github_svc, cache):
    codes = await github_svc.list_team_codes()
    now   = datetime.now(timezone.utc)
    for item in codes:
        code = item.get("code", "")
        if not code:
            continue
        data = await github_svc.get_team_code(code)
        if not data:
            continue
        expires_at_str = data.get("expires_at", "")
        try:
            expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
        except Exception:
            continue
        if now > expires_at:
            asyncio.create_task(github_svc.delete_team_code(code))
            if cache:
                await cache.delete(f"team_code:{code}")
            logger.info(f"Expired team code {code} cleaned up")
