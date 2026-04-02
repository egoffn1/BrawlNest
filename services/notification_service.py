#!/usr/bin/env python3
"""
Уведомления и алерты для BrawlNest.
Отправка уведомлений об изменениях трофеев, достижений и событий.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable
from enum import Enum

from utils.logger import setup_logger

logger = setup_logger(__name__)


class NotificationType(str, Enum):
    TROPHY_MILESTONE = "trophy_milestone"
    RANK_CHANGE = "rank_change"
    CLUB_EVENT = "club_event"
    ACHIEVEMENT = "achievement"
    CUSTOM = "custom"


class NotificationPriority(str, Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class Subscription:
    """Подписка пользователя на уведомления."""
    
    def __init__(
        self,
        user_id: str,
        player_tags: List[str],
        notification_types: List[NotificationType],
        channels: List[str] = None
    ):
        self.user_id = user_id
        self.player_tags = [tag.lstrip('#') for tag in player_tags]
        self.notification_types = notification_types
        self.channels = channels or ["app"]
        self.created_at = datetime.now(timezone.utc)
        self.is_active = True


class NotificationService:
    """Сервис управления уведомлениями."""

    def __init__(self, db, cache=None):
        self.db = db
        self.cache = cache
        self.subscriptions: Dict[str, List[Subscription]] = {}
        self.handlers: Dict[NotificationType, List[Callable]] = {
            nt: [] for nt in NotificationType
        }
        self._milestone_thresholds = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]

    async def subscribe(
        self,
        user_id: str,
        player_tags: List[str],
        notification_types: List[NotificationType],
        channels: List[str] = None
    ) -> Subscription:
        """Подписать пользователя на уведомления."""
        subscription = Subscription(user_id, player_tags, notification_types, channels)
        
        if user_id not in self.subscriptions:
            self.subscriptions[user_id] = []
        self.subscriptions[user_id].append(subscription)

        # Сохраняем в БД
        await self._save_subscription(subscription)

        logger.info(f"User {user_id} subscribed to notifications for {player_tags}")
        return subscription

    async def unsubscribe(self, user_id: str, subscription_id: str = None) -> bool:
        """Отписать пользователя от уведомлений."""
        if user_id not in self.subscriptions:
            return False
        
        if subscription_id:
            self.subscriptions[user_id] = [
                s for s in self.subscriptions[user_id] 
                if id(s) != subscription_id
            ]
        else:
            del self.subscriptions[user_id]

        await self._remove_subscription(user_id, subscription_id)
        logger.info(f"User {user_id} unsubscribed from notifications")
        return True

    def register_handler(self, notification_type: NotificationType, handler: Callable):
        """Регистрация обработчика уведомлений."""
        self.handlers[notification_type].append(handler)

    async def send_notification(
        self,
        user_id: str,
        notification_type: NotificationType,
        title: str,
        message: str,
        data: Dict[str, Any] = None,
        priority: NotificationPriority = NotificationPriority.NORMAL
    ):
        """Отправка уведомления пользователю."""
        notification = {
            "user_id": user_id,
            "type": notification_type.value,
            "title": title,
            "message": message,
            "data": data or {},
            "priority": priority.value,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "is_read": False
        }

        # Сохраняем в БД
        await self._save_notification(notification)

        # Вызываем обработчики
        for handler in self.handlers[notification_type]:
            try:
                await handler(notification)
            except Exception as e:
                logger.error(f"Notification handler error: {e}")

        logger.debug(f"Notification sent to {user_id}: {title}")
        return notification

    async def check_trophy_milestones(self, player_tag: str, new_trophies: int, old_trophies: int = None):
        """Проверка достижения трофейных рубежей."""
        tag = player_tag.lstrip('#')
        
        if old_trophies is None:
            # Получаем старые трофеи из кэша или БД
            cache_key = f"trophies:{tag}"
            if self.cache:
                old_trophies = await self.cache.get(cache_key)
            
            if old_trophies is None:
                player = await self.db.get_player(tag)
                old_trophies = player.get("trophies", 0) if player else 0
        
        old_trophies = int(old_trophies)
        
        # Проверяем пересечение рубежей
        crossed_milestones = []
        for milestone in self._milestone_thresholds:
            if old_trophies < milestone <= new_trophies:
                crossed_milestones.append(milestone)

        if crossed_milestones:
            # Находим подписчиков этого игрока
            for user_id, subscriptions in self.subscriptions.items():
                for sub in subscriptions:
                    if tag in sub.player_tags and NotificationType.TROPHY_MILESTONE in sub.notification_types:
                        for milestone in crossed_milestones:
                            await self.send_notification(
                                user_id=user_id,
                                notification_type=NotificationType.TROPHY_MILESTONE,
                                title="🏆 Трофейный рубеж!",
                                message=f"Игрок #{tag} достиг {milestone} трофеев!",
                                data={
                                    "player_tag": tag,
                                    "milestone": milestone,
                                    "new_trophies": new_trophies
                                },
                                priority=NotificationPriority.HIGH
                            )

        # Обновляем кэш
        if self.cache:
            await self.cache.set(cache_key, new_trophies, ttl=3600)

    async def check_rank_change(self, player_tag: str, new_rank: int, old_rank: int = None):
        """Проверка изменения ранга игрока."""
        if old_rank is None:
            cache_key = f"rank:{player_tag}"
            if self.cache:
                old_rank = await self.cache.get(cache_key)
            old_rank = old_rank or 0

        if new_rank != old_rank:
            for user_id, subscriptions in self.subscriptions.items():
                for sub in subscriptions:
                    if player_tag in sub.player_tags and NotificationType.RANK_CHANGE in sub.notification_types:
                        direction = "⬆️" if new_rank > old_rank else "⬇️"
                        await self.send_notification(
                            user_id=user_id,
                            notification_type=NotificationType.RANK_CHANGE,
                            title=f"{direction} Изменение ранга!",
                            message=f"Игрок #{player_tag} изменил ранг: {old_rank} → {new_rank}",
                            data={
                                "player_tag": player_tag,
                                "old_rank": old_rank,
                                "new_rank": new_rank
                            },
                            priority=NotificationPriority.NORMAL
                        )

            if self.cache:
                await self.cache.set(f"rank:{player_tag}", new_rank, ttl=3600)

    async def notify_club_event(self, club_tag: str, event_type: str, data: Dict[str, Any]):
        """Уведомление о событии в клубе."""
        event_messages = {
            "member_joined": "Новый участник в клубе",
            "member_left": "Участник покинул клуб",
            "trophy_record": "Новый рекорд клуба по трофеям",
            "maintenance": "Технические работы в клубе"
        }

        message = event_messages.get(event_type, f"Событие в клубе: {event_type}")

        # Находим всех подписчиков клуба
        for user_id, subscriptions in self.subscriptions.items():
            for sub in subscriptions:
                if club_tag in sub.player_tags and NotificationType.CLUB_EVENT in sub.notification_types:
                    await self.send_notification(
                        user_id=user_id,
                        notification_type=NotificationType.CLUB_EVENT,
                        title=f"📢 Клуб #{club_tag}",
                        message=message,
                        data={"club_tag": club_tag, "event_type": event_type, **data},
                        priority=NotificationPriority.NORMAL
                    )

    async def get_user_notifications(
        self,
        user_id: str,
        limit: int = 50,
        unread_only: bool = False
    ) -> List[Dict]:
        """Получение уведомлений пользователя."""
        return await self.db.get_user_notifications(user_id, limit, unread_only)

    async def mark_notification_read(self, user_id: str, notification_id: str):
        """Отметить уведомление как прочитанное."""
        await self.db.mark_notification_read(user_id, notification_id)

    async def mark_all_read(self, user_id: str):
        """Отметить все уведомления как прочитанные."""
        await self.db.mark_all_notifications_read(user_id)

    async def _save_subscription(self, subscription: Subscription):
        """Сохранение подписки в БД."""
        # Заглушка - реализовать при наличии таблицы subscriptions
        pass

    async def _remove_subscription(self, user_id: str, subscription_id: str = None):
        """Удаление подписки из БД."""
        pass

    async def _save_notification(self, notification: Dict):
        """Сохранение уведомления в БД."""
        await self.db.save_notification(notification)

    async def cleanup_old_notifications(self, days: int = 30):
        """Очистка старых уведомлений."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        await self.db.cleanup_old_notifications(cutoff.isoformat())
        logger.info(f"Cleaned up notifications older than {days} days")


# Расширения для Database
async def save_notification(self, notification: Dict):
    """Сохранить уведомление в БД."""
    conn = await self._conn_or_raise()
    await conn.execute("""
        INSERT INTO notifications (
            user_id, type, title, message, data, priority, created_at, is_read
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        notification["user_id"],
        notification["type"],
        notification["title"],
        notification["message"],
        str(notification.get("data", {})),
        notification["priority"],
        notification["created_at"],
        1 if notification.get("is_read") else 0
    ))
    await conn.commit()


async def get_user_notifications(self, user_id: str, limit: int = 50, unread_only: bool = False):
    """Получить уведомления пользователя."""
    conn = await self._conn_or_raise()
    query = "SELECT * FROM notifications WHERE user_id=?"
    params = [user_id]
    
    if unread_only:
        query += " AND is_read=0"
    
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    
    async with conn.execute(query, tuple(params)) as cur:
        rows = await cur.fetchall()
    
    return [dict(r) for r in rows]


async def mark_notification_read(self, user_id: str, notification_id: str):
    """Отметить уведомление как прочитанное."""
    conn = await self._conn_or_raise()
    await conn.execute("""
        UPDATE notifications SET is_read=1 WHERE user_id=? AND id=?
    """, (user_id, notification_id))
    await conn.commit()


async def mark_all_notifications_read(self, user_id: str):
    """Отметить все уведомления как прочитанные."""
    conn = await self._conn_or_raise()
    await conn.execute("""
        UPDATE notifications SET is_read=1 WHERE user_id=?
    """, (user_id,))
    await conn.commit()


async def cleanup_old_notifications(self, cutoff_date: str):
    """Удалить старые уведомления."""
    conn = await self._conn_or_raise()
    await conn.execute("""
        DELETE FROM notifications WHERE created_at < ?
    """, (cutoff_date,))
    await conn.commit()
