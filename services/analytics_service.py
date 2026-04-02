#!/usr/bin/env python3
"""
Модуль аналитики и статистики для BrawlNest.
Предоставляет расширенные метрики, прогнозы и аналитические отчёты.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
import statistics

from utils.logger import setup_logger

logger = setup_logger(__name__)


class AnalyticsService:
    """Сервис для углублённой аналитики игроков и клубов."""

    def __init__(self, db, cache=None):
        self.db = db
        self.cache = cache

    async def get_player_analytics(self, player_tag: str) -> Dict[str, Any]:
        """
        Возвращает полную аналитику игрока:
        - тренд трофеев
        - лучшие режимы
        - любимые бойцы
        - активность по времени
        - прогноз ранга
        """
        tag = player_tag.lstrip('#')
        cache_key = f"analytics:player:{tag}"
        
        if self.cache:
            cached = await self.cache.get(cache_key)
            if cached:
                return cached

        # Получаем бои
        battles = await self.db.get_battles(tag, limit=100)
        if not battles:
            return {"error": "No battles found"}

        # Аналитика по режимам
        mode_stats = defaultdict(lambda: {"wins": 0, "total": 0, "trophy_change": 0})
        brawler_stats = defaultdict(lambda: {"wins": 0, "total": 0, "trophy_change": 0})
        hourly_activity = defaultdict(int)
        trophy_timeline = []

        for battle in battles:
            mode = battle.get("battle_mode", "Unknown")
            brawler = battle.get("brawler_name", "Unknown")
            result = battle.get("result", "")
            trophy_change = battle.get("trophy_change", 0)
            battle_time = battle.get("battle_time", "")

            mode_stats[mode]["total"] += 1
            mode_stats[mode]["trophy_change"] += trophy_change
            if result == "victory":
                mode_stats[mode]["wins"] += 1

            brawler_stats[brawler]["total"] += 1
            brawler_stats[brawler]["trophy_change"] += trophy_change
            if result == "victory":
                brawler_stats[brawler]["wins"] += 1

            # Активность по часам
            if battle_time:
                try:
                    dt = datetime.fromisoformat(battle_time.replace('Z', '+00:00'))
                    hourly_activity[dt.hour] += 1
                    trophy_timeline.append((dt, trophy_change))
                except Exception:
                    pass

        # Расчёт метрик
        best_mode = max(mode_stats.items(), key=lambda x: x[1]["wins"]) if mode_stats else None
        best_brawler = max(brawler_stats.items(), key=lambda x: x[1]["wins"]) if brawler_stats else None
        
        # Тренд трофеев
        trophy_trend = "stable"
        if len(trophy_timeline) >= 10:
            recent = [t[1] for t in sorted(trophy_timeline, key=lambda x: x[0])[-10:]]
            avg_recent = statistics.mean(recent)
            if avg_recent > 2:
                trophy_trend = "rising"
            elif avg_recent < -2:
                trophy_trend = "falling"

        # Прогноз (простая линейная регрессия)
        trophy_prediction = None
        if len(trophy_timeline) >= 20:
            sorted_timeline = sorted(trophy_timeline, key=lambda x: x[0])
            recent_changes = [t[1] for t in sorted_timeline[-20:]]
            avg_change = statistics.mean(recent_changes)
            trophy_prediction = round(avg_change * 10)  # прогноз на 10 игр

        analytics = {
            "player_tag": f"#{tag}",
            "total_battles_analyzed": len(battles),
            "mode_statistics": [
                {
                    "mode": mode,
                    "wins": stats["wins"],
                    "total": stats["total"],
                    "win_rate": round(stats["wins"] / stats["total"] * 100, 2) if stats["total"] > 0 else 0,
                    "avg_trophy_change": round(stats["trophy_change"] / stats["total"], 2) if stats["total"] > 0 else 0
                }
                for mode, stats in sorted(mode_stats.items(), key=lambda x: x[1]["total"], reverse=True)
            ],
            "brawler_statistics": [
                {
                    "brawler": brawler,
                    "wins": stats["wins"],
                    "total": stats["total"],
                    "win_rate": round(stats["wins"] / stats["total"] * 100, 2) if stats["total"] > 0 else 0,
                    "avg_trophy_change": round(stats["trophy_change"] / stats["total"], 2) if stats["total"] > 0 else 0
                }
                for brawler, stats in sorted(brawler_stats.items(), key=lambda x: x[1]["total"], reverse=True)[:10]
            ],
            "peak_hours": sorted(hourly_activity.items(), key=lambda x: x[1], reverse=True)[:5],
            "trophy_trend": trophy_trend,
            "trophy_prediction_next_10_games": trophy_prediction,
            "best_mode": best_mode[0] if best_mode else None,
            "best_brawler": best_brawler[0] if best_brawler else None,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }

        if self.cache:
            await self.cache.set(cache_key, analytics, ttl=3600)

        return analytics

    async def get_club_analytics(self, club_tag: str) -> Dict[str, Any]:
        """Аналитика клуба: активность участников, общие достижения."""
        tag = club_tag.lstrip('#')
        cache_key = f"analytics:club:{tag}"
        
        if self.cache:
            cached = await self.cache.get(cache_key)
            if cached:
                return cached

        members = await self.db.get_club_members(tag)
        if not members:
            return {"error": "No members found"}

        # Статистика участников
        trophy_distribution = {
            "0-999": 0,
            "1000-1999": 0,
            "2000-2999": 0,
            "3000-3999": 0,
            "4000-4999": 0,
            "5000+": 0
        }
        
        role_distribution = defaultdict(int)
        total_trophies = 0
        member_trophies = []

        for member in members:
            trophies = member.get("trophies", 0)
            role = member.get("role", "member")
            
            total_trophies += trophies
            member_trophies.append(trophies)
            role_distribution[role] += 1

            if trophies < 1000:
                trophy_distribution["0-999"] += 1
            elif trophies < 2000:
                trophy_distribution["1000-1999"] += 1
            elif trophies < 3000:
                trophy_distribution["2000-2999"] += 1
            elif trophies < 4000:
                trophy_distribution["3000-3999"] += 1
            elif trophies < 5000:
                trophy_distribution["4000-4999"] += 1
            else:
                trophy_distribution["5000+"] += 1

        analytics = {
            "club_tag": f"#{tag}",
            "total_members": len(members),
            "average_trophies": round(total_trophies / len(members), 2) if members else 0,
            "median_trophies": statistics.median(member_trophies) if member_trophies else 0,
            "trophy_distribution": trophy_distribution,
            "role_distribution": dict(role_distribution),
            "top_members": sorted(members, key=lambda x: x.get("trophies", 0), reverse=True)[:10],
            "generated_at": datetime.now(timezone.utc).isoformat()
        }

        if self.cache:
            await self.cache.set(cache_key, analytics, ttl=3600)

        return analytics

    async def get_meta_report(self, days: int = 7) -> Dict[str, Any]:
        """
        Отчёт о текущей мете:
        - самые популярные режимы
        - самые эффективные бойцы
        - популярные карты
        """
        cache_key = f"analytics:meta:{days}"
        
        if self.cache:
            cached = await self.cache.get(cache_key)
            if cached:
                return cached

        # Получаем все бои за период
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Заглушка - в реальной реализации нужен запрос к БД с фильтром по дате
        all_battles = await self.db._fetchall(
            "SELECT * FROM battles WHERE datetime(battle_time) > datetime(?) ORDER BY battle_time DESC LIMIT 1000",
            (cutoff_date.isoformat(),)
        )

        if not all_battles:
            return {"error": "No battles found for period"}

        mode_winrates = defaultdict(lambda: {"wins": 0, "total": 0})
        brawler_winrates = defaultdict(lambda: {"wins": 0, "total": 0, "trophy_change": 0})
        map_stats = defaultdict(lambda: {"wins": 0, "total": 0})

        for battle in all_battles:
            mode = battle.get("battle_mode", "Unknown")
            brawler = battle.get("brawler_name", "Unknown")
            map_name = battle.get("battle_mode", "Unknown")  # В реальности нужно имя карты
            result = battle.get("result", "")
            trophy_change = battle.get("trophy_change", 0)

            mode_winrates[mode]["total"] += 1
            if result == "victory":
                mode_winrates[mode]["wins"] += 1

            brawler_winrates[brawler]["total"] += 1
            brawler_winrates[brawler]["trophy_change"] += trophy_change
            if result == "victory":
                brawler_winrates[brawler]["wins"] += 1

            map_stats[map_name]["total"] += 1
            if result == "victory":
                map_stats[map_name]["wins"] += 1

        meta_report = {
            "period_days": days,
            "total_battles_analyzed": len(all_battles),
            "mode_meta": sorted([
                {
                    "mode": mode,
                    "total": stats["total"],
                    "win_rate": round(stats["wins"] / stats["total"] * 100, 2) if stats["total"] > 0 else 0
                }
                for mode, stats in mode_winrates.items()
            ], key=lambda x: x["total"], reverse=True)[:10],
            "top_brawlers": sorted([
                {
                    "brawler": brawler,
                    "pick_rate": round(stats["total"] / len(all_battles) * 100, 2),
                    "win_rate": round(stats["wins"] / stats["total"] * 100, 2) if stats["total"] > 0 else 0,
                    "avg_trophy_change": round(stats["trophy_change"] / stats["total"], 2) if stats["total"] > 0 else 0
                }
                for brawler, stats in brawler_winrates.items()
            ], key=lambda x: x["win_rate"], reverse=True)[:20],
            "map_meta": sorted([
                {
                    "map": map_name,
                    "total": stats["total"],
                    "win_rate": round(stats["wins"] / stats["total"] * 100, 2) if stats["total"] > 0 else 0
                }
                for map_name, stats in map_stats.items()
            ], key=lambda x: x["total"], reverse=True)[:10],
            "generated_at": datetime.now(timezone.utc).isoformat()
        }

        if self.cache:
            await self.cache.set(cache_key, meta_report, ttl=1800)

        return meta_report

    async def compare_players_detailed(self, tags: List[str]) -> Dict[str, Any]:
        """Детальное сравнение нескольких игроков."""
        if len(tags) < 2 or len(tags) > 5:
            return {"error": "Need 2-5 player tags"}

        players_data = []
        for tag in tags:
            analytics = await self.get_player_analytics(tag)
            if "error" not in analytics:
                players_data.append(analytics)

        if len(players_data) < 2:
            return {"error": "Not enough valid players to compare"}

        comparison = {
            "players_compared": len(players_data),
            "comparison": [],
            "summary": {}
        }

        for player in players_data:
            comparison["comparison"].append({
                "tag": player["player_tag"],
                "total_battles": player["total_battles_analyzed"],
                "best_mode": player.get("best_mode"),
                "best_brawler": player.get("best_brawler"),
                "trophy_trend": player.get("trophy_trend"),
                "prediction": player.get("trophy_prediction_next_10_games")
            })

        # Сводка
        trends = [p.get("trophy_trend") for p in players_data]
        comparison["summary"] = {
            "most_active": max(players_data, key=lambda x: x["total_battles_analyzed"])["player_tag"],
            "best_trend": [p["player_tag"] for p in players_data if p.get("trophy_trend") == "rising"],
            "trend_distribution": {trend: trends.count(trend) for trend in set(trends)}
        }

        return comparison
