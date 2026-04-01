#!/usr/bin/env python3
"""
Brawl Stats CLI — интерфейс для анализа Brawl Stars.
Навигация: ↑↓ / Tab / Shift+Tab → выбор, Enter → подтверждение, q → выход
"""
import sys
import asyncio
import json
import os
import time
import threading
import random
import aiohttp
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from urllib.parse import quote

from rich.console import Console
from rich.table import Table
from rich import box
from rich.rule import Rule
from rich.panel import Panel
from rich.text import Text
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn

from prompt_toolkit import Application as PTApp
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import Layout, HSplit, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.styles import Style as PTStyle

from config import API_KEYS, SEARCH_CFG, APP_CFG, SYNC_CFG, GITHUB_REPO_URL, GITHUB_TOKEN
from database import Database
from api_client import BrawlAPIClient
from collectors.player_collector import PlayerCollector
from collectors.club_collector import ClubCollector
from utils.logger import setup_logger
from utils.tag_generator import generate_tags
from sync_github import GitHubSync
from remote_storage import storage as remote_storage

# Проверка наличия библиотек для PNG
PNG_AVAILABLE = False
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from PIL import Image
    import io
    PNG_AVAILABLE = True
except ImportError:
    pass

# ── консоль ──────────────────────────────────────────────────────────────────
console = Console(highlight=False)
logger = setup_logger()

# ── стиль ────────────────────────────────────────────────────────────────────
PT_STYLE = PTStyle.from_dict({
    "cursor":   "bold #f97316",
    "selected": "bold #ffffff",
    "item":     "#d1d5db",
    "dim":      "#4b5563",
    "prompt":   "bold #f97316",
    "text":     "#ffffff",
})

# ── названия режимов ────────────────────────────────────────────────────────
MODE_NAMES = {
    # Основные режимы 3x3
    "gemGrab":       "💎 Захват кристаллов",
    "brawlBall":     "⚽ Броулбол",
    "bounty":        "🏆 Охота за головами",
    "heist":         "💰 Ограбление",
    "hotZone":       "🔥 Горячая зона",
    "knockout":      "🥊 Нокаут",
    "wipeout":       "🧹 Зачистка",
    
    # Столкновения
    "soloShowdown":  "🌵 Столкновение (соло)",
    "duoShowdown":   "👥 Столкновение (дуо)",
    "trioShowdown":  "👥 Столкновение (трио)",
    
    # Особые режимы
    "duels":         "⚔️ Дуэли",
    "basketBrawl":   "🏀 Баскетбой",
    "siege":         "🏰 Осада",
    "ranked":        "🏆 Ранговый бой",
    
    # Режимы 5x5
    "wipeout5V5":    "🧹 Зачистка 5x5",
    "knockout5V5":   "🥊 Нокаут 5x5",
    "brawlBall5V5":  "⚽ Броулбол 5x5",
    "gemGrab5V5":    "💎 Захват кристаллов 5x5",
    "heist5V5":      "💰 Ограбление 5x5",
    "bounty5V5":     "🏆 Охота за головами 5x5",
    "hotZone5V5":    "🔥 Горячая зона 5x5",
    
    # Роборубка и события
    "roboRumble":    "🤖 Роборубка",
    "bossFight":     "👾 Битва с боссом",
    "bigGame":       "🐘 Большая игра",
    "superCity":     "🌃 Супергород",
    
    # Сезонные и специальные события
    "special":       "✨ Особое событие",
    "championship":  "🏅 Чемпионат",
    "powerLeague":   "⚡ Силовая лига",
    "giftUpgrade":   "🎁 Улучшение подарков",
    "blitz":         "⚡ Блиц",
    "heavyMetal":    "🎸 Хэви-метал",
    "brawlerTeam":   "🤝 Команда бойцов",
    "scoreShowdown": "🎯 Столкновение с очками",
    "speedRun":      "🏃 Спидран",
    "defendTheTrophy": "🏆 Защита трофея",
    "mysteryMode":   "❓ Таинственный режим",
    "training":      "🎯 Тренировка",
    "unknown":       "❓ Неизвестный режим",
}

MAP_TRANSLATIONS = {
    # Арканы / Кристальная аркана (Gem Grab)
    "Hard Rock Mine": "Хард-рок шахта",
    "Crystal Cavern": "Кристальная пещера",
    "Double Trouble": "Двойная проблема",
    "Woodland Lilies": "Лесные лилии",
    "Safe Zone": "Безопасная зона",
    "Hidden Path": "Скрытая тропа",
    "Ruby Pass": "Рубиновый перевал",
    "Sapphire Plains": "Сапфировые равнины",
    "Emerald Park": "Изумрудный парк",
    "Goblin's Den": "Логово гоблина",
    
    # Броулбол (Brawl Ball)
    "Pinhole Punt": "Точный удар",
    "Super Beach": "Суперпляж",
    "Sunny Soccer": "Солнечный футбол",
    "Triple Dribble": "Трипл-дриблинг",
    "Center Stage": "Центральная сцена",
    "Backyard Bowl": "Дворовый кубок",
    "Field Goal": "Удар по воротам",
    "Neon Field": "Неоновое поле",
    "Air Sports Bar": "Спортивный бар",
    "Grass Knot": "Травяной узел",
    "Penalty Kick": "Пенальти",
    "Brawl Ball Stadium": "Стадион Броулбола",
    
    # Охота за головами (Bounty)
    "Deathcap Trap": "Ловушка смертошляпа",
    "Snake Prairie": "Змеиная прерия",
    "Shooting Star": "Падающая звезда",
    "Out in the Open": "На открытом месте",
    "Skull Creek": "Череповой ручей",
    "Dry Season": "Сухой сезон",
    "Thousand Lakes": "Тысяча озёр",
    "Fork in the Road": "Развилка",
    "Dusty Desert": "Пыльная пустыня",
    "Golden Lane": "Золотая аллея",
    
    # Ограбление (Heist)
    "Bridge Too Far": "Мост слишком далеко",
    "Bridge Spam": "Мостовой спам",
    "Cannon Cart Cove": "Бухта пушечной тележки",
    "Dark Destiny": "Тёмная судьба",
    "Hot Maze": "Горячий лабиринт",
    "Kaboom Canyon": "Каньон Кабум",
    "Safe Pass": "Безопасный проход",
    "Secrets of the Storm": "Тайны шторма",
    "Twilight Breaker": "Сумеречный разрушитель",
    "Vault Defenders": "Защитники хранилища",
    
    # Горячая зона (Hot Zone)
    "Iron Strait": "Железный пролив",
    "Ring of Fire": "Огненное кольцо",
    "Open Zone": "Открытая зона",
    "Parallel Plays": "Параллельные игры",
    "Quarter Pounder": "Четвертьфунтер",
    "Three Lanes": "Три линии",
    "Trident Grass": "Трезубец травы",
    "Narrow Passage": "Узкий проход",
    "Thermal Landscapes": "Термальные ландшафты",
    "Warped Wasteland": "Искажённая пустошь",
    
    # Нокаут (Knockout)
    "Belle's Rock": "Скала Белль",
    "New Horizon": "Новый горизонт",
    "Deep Hollows": "Глубокие впадины",
    "Flowing Springs": "Протекающие источники",
    "Goldarm Gulch": "Золоторучейский овраг",
    "H is for Holiday": "H — значит праздник",
    "Hideout": "Убежище",
    "Jumpscare Lair": "Логово прыжка страха",
    "Lotus": "Лотос",
    "Mushroom Meadow": "Грибная поляна",
    "Serene Sands": "Безмятежные пески",
    "Starfruit Supernova": "Звёздная сверхновая",
    "Crescendo": "Крещендо",
    "Mirror Match": "Зеркальный матч",
    "Overpass": "Эстакада",
    "Underpass": "Подземный переход",
    
    # Зачистка (Wipeout)
    "Acid Lakes": "Кислотные озёра",
    "Death Landscapes": "Мёртвые ландшафты",
    "Feast or Famine": "Пир или голод",
    "GG Mortuary": "Морг GG",
    "Layer Cake": "Слоёный торт",
    "Two Thousand Lakes": "Две тысячи озёр",
    "Dueling Beetles": "Дуэль жуков",
    "Pinball Dreams": "Пинбольные мечты",
    "Quintillion": "Квинтиллион",
    "Grand Canal": "Гранд-канал",
    
    # Столкновение (Showdown)
    "Stormy Plains": "Штормовые равнины",
    "Skull Creek": "Череповой ручей",
    "Cavern Churn": "Пещерная карусель",
    "Rockwall Brawl": "Каменная потасовка",
    "Training Island": "Тренировочный остров",
    "Feast or Famine": "Пир или голод",
    "Lonely Skies": "Одинокие небеса",
    "Poison Lake": "Ядовитое озеро",
    "Badlands": "Плохие земли",
    "Desert Verticality": "Пустынная вертикальность",
    "Island Invasion": "Островное вторжение",
    "Mystic Thirty Three": "Мистические тридцать три",
    "Rugged Roads": "Пересечённые дороги",
    "Shadow Shrine": "Теневое святилище",
    "Storm Front": "Фронт бури",
    "Toxic River": "Токсичная река",
    "Waste Haven": "Приют отходов",
    "Westside Wilderness": "Западная глушь",
    "Yggdrasil": "Иггдрасиль",
    "Zaptrap": "Заптрап",
    "Block Party": "Вечеринка квартала",
    "Circle of Doom": "Круг погибели",
    "Core Crasher": "Крушитель ядер",
    "Dark Dunes": "Тёмные дюны",
    "Double Swoosh": "Двойной взмах",
    "Dread Crossing": "Страшный переход",
    "Endless Retreat": "Бесконечное отступление",
    "Everbush": "Вечнокустарник",
    "Forsaken Falls": "Забытые водопады",
    "Ghost Point": "Призрачная точка",
    "Hazard High Voltage": "Опасность высокое напряжение",
    "Heat Wave": "Тепловая волна",
    "Hollow Stones": "Полые камни",
    "Lethal Lava": "Смертельная лава",
    "Lightning Valley": "Долина молний",
    "Mortal Coil": "Смертельная спираль",
    "Nimbus Nook": "Уголок нимбуса",
    "Noxious Nexus": "Ядовитый нексус",
    "Perilous Peaks": "Опасные пики",
    "Pit Stop": "Пит-стоп",
    "Plaza": "Площадь",
    "Point of View": "Точка зрения",
    "Ruins": "Руины",
    "Scorched Stone": "Обожжённый камень",
    "Snaked Grass": "Змеиная трава",
    "Spiky Pass": "Колючий перевал",
    "Stocky Stockades": "Частокол",
    "Sunken Ruins": "Затонувшие руины",
    "Tempest Tornado": "Буря торнадо",
    "Thicket Thorns": "Чаща шипов",
    "Treacherous Trails": "Предательские тропы",
    "Twilight Turf": "Сумеречный дёрн",
    "Vicious Vortex": "Порочный вихрь",
    "Volcanic Valley": "Вулканическая долина",
    "Windmill Fields": "Поля ветряных мельниц",
    "Wolfpack Woods": "Леса стаи волков",
    "Zen Garden": "Дзен-сад",
    
    # Дуэли (Duels)
    "Angled Mountain": "Угловатая гора",
    "Flaring Phoenix": "Пылающий феникс",
    "Beautiful Garden": "Прекрасный сад",
    "Middle Ground": "Средняя земля",
    "Peacemaker's Rest": "Покой миротворца",
    "Quick Draw": "Быстрая стрельба",
    "Riverside Ring": "Прибрежное кольцо",
    "The Last Stand": "Последний рубеж",
    
    # Баскетбой (Basket Brawl)
    "Hoop Boot Hill": "Баскетбольный холм",
    "Dunk City": "Город данка",
    "Alley Oop": "Алей-уп",
    "Fast Break": "Быстрый прорыв",
    "Slam Dunk": "Слэм-данк",
    "Three Pointer": "Трёхочковый",
    
    # Осада (Siege)
    "Assembly Attack": "Атака сборки",
    "Bot Drop": "Высадка ботов",
    "Factory Rush": "Заводской рывок",
    "Fort Muck": "Форт Грязь",
    "Mechanical Mayhem": "Механический хаос",
    "Robo Highway": "Робо-хайвей",
    "Robot Factory": "Фабрика роботов",
    
    # Роборубка (Robo Rumble)
    "Keep Safe": "Береги крепость",
    "Last Stand": "Последний рубеж",
    "Power Play": "Силовая игра",
    "The Heist": "Ограбление",
    "Defend the Vault": "Защита хранилища",
    
    # Битва с боссом (Boss Fight)
    "Big Bad Boss": "Большой злой босс",
    "Machine Zone": "Зона машин",
    "Robot Riot": "Бунт роботов",
    
    # Ранговый бой (Ranked)
    "Diamond Dome": "Алмазный купол",
    "Glass House": "Стеклянный дом",
    "Peak Performers": "Пиковые исполнители",
    "Summit Showdown": "Вершинное столкновение",
    
    # Специальные / Сезонные карты
    "Mirage Arena": "Арена миражей",
    "Icy Ice Park": "Ледяной парк",
    "Hello Always Ends With a Goodbye": "Привет всегда заканчивается прощанием",
    "Storage Sector": "Складской сектор",
    "Minecart Madness": "Безумие вагонетки",
    "Gift Wrap": "Упаковка подарка",
    "Present Pursuit": "Погоня за подарком",
    "Holiday Party": "Праздничная вечеринка",
    "Snowman Assault": "Атака снеговиков",
    "Winter Festival": "Зимний фестиваль",
    "Dragon Arena": "Арена дракона",
    "Ninja Hideaway": "Убежище ниндзя",
    "Samurai Smash": "Самурайский разгром",
    "Temple of Boom": "Храм бума",
    "Ancient Treasure": "Древнее сокровище",
    "Blast Ball": "Взрывной мяч",
    "Boom Town": "Город бума",
    "Can't Touch This": "Не тронь меня",
    "Close Call": "Близкий промах",
    "Cold Zone": "Холодная зона",
    "Cramped Space": "Тесное пространство",
    "Cross Cut": "Поперечный разрез",
    "Danger Zone": "Опасная зона",
    "Deep End": "Глубокий конец",
    "Double Jeopardy": "Двойная опасность",
    "Final Frontier": "Последний рубеж",
    "Flying Fortress": "Летающая крепость",
    "Forest Clearing": "Лесная поляна",
    "Frozen Peak": "Ледяной пик",
    "Goosebumps Grove": "Роща мурашек",
    "High Score": "Высокий счёт",
    "Ice Blockade": "Ледяная блокада",
    "In The Liminal": "В лиминале",
    "Island Hopping": "Прыжки по островам",
    "Jungle Ball": "Джунгли-бол",
    "King of the Hill": "Король горы",
    "Laser Tag": "Лазертаг",
    "Lava Belt": "Лавовый пояс",
    "Maze Mayhem": "Лабиринтный хаос",
    "Midnight Melee": "Полуночная схватка",
    "Monster Island": "Остров монстров",
    "Mountain Meltdown": "Горное расплавление",
    "Mystic Meadows": "Мистические луга",
    "Night Market": "Ночной рынок",
    "Ocean's Edge": "Край океана",
    "Offbeat Oval": "Необычный овал",
    "On A Roll": "На подъёме",
    "Open Sky": "Открытое небо",
    "Overgrown Ruins": "Заросшие руины",
    "Paradise Island": "Остров рай",
    "Party Crasher": "Праздничный нарушитель",
    "Peacekeeper": "Миротворец",
    "Pirate Cove": "Пиратская бухта",
    "Playground": "Игровая площадка",
    "Power Center": "Центр силы",
    "Powerhouse": "Электростанция",
    "Prison Break": "Побег из тюрьмы",
    "Race Track": "Гоночная трасса",
    "Rapid Rapids": "Быстрые пороги",
    "Red Light Green Light": "Красный свет зелёный свет",
    "Reflection": "Отражение",
    "Retina": "Сетчатка",
    "Riverbed": "Русло реки",
    "Royal Flush": "Роял флеш",
    "Sacred Sanctuary": "Священное святилище",
    "Sandstorm": "Песчаная буря",
    "Seaside Surprise": "Морской сюрприз",
    "Sky Bridge": "Небесный мост",
    "Sky Deck": "Небесная палуба",
    "Slippery Slope": "Скользкий склон",
    "Space Station": "Космическая станция",
    "Speedway": "Спидвей",
    "Spooky Town": "Жуткий город",
    "Spring Fling": "Весеннее веселье",
    "Stairway to Heaven": "Лестница в небо",
    "Stand Still": "Стой спокойно",
    "Starry Night": "Звёздная ночь",
    "Stepping Stones": "Ступеньки",
    "Street Brawler": "Уличный боец",
    "Strike Out": "Забастовка",
    "Sugar Rush": "Сахарная лихорадка",
    "Summer Splash": "Летний всплеск",
    "Sunrise Spring": "Весенний восход",
    "Surprise Attack": "Внезапная атака",
    "Sweet Dreams": "Сладкие сны",
    "Tactical Exchange": "Тактический обмен",
    "Tangled Roots": "Запутанные корни",
    "Target Practice": "Стрельба по мишеням",
    "Team Day": "Командный день",
    "The Great Divide": "Великое разделение",
    "The Great Outdoors": "Великие просторы",
    "The Lab": "Лаборатория",
    "The Ring": "Ринг",
    "Time Travel": "Путешествие во времени",
    "Tombstone": "Надгробие",
    "Top to Bottom": "Сверху вниз",
    "Tower Down": "Башня вниз",
    "Town Hall": "Ратуша",
    "Train Robbery": "Ограбление поезда",
    "Treehouse Retreat": "Домик на дереве",
    "Trench Warfare": "Окопная война",
    "Tropical Isle": "Тропический остров",
    "Turbo Turnpike": "Турбо магистраль",
    "Turnaround": "Разворот",
    "Twisted Plan": "Искривлённый план",
    "Underground Passage": "Подземный проход",
    "Up and Over": "Вверх и через",
    "Urban Jungle": "Городские джунгли",
    "Valley of Victory": "Долина победы",
    "Victory Lane": "Аллея победы",
    "Viking Voyage": "Викингское путешествие",
    "Volcano Room": "Вулканическая комната",
    "Walk of Fame": "Аллея славы",
    "Warehouse Rampage": "Складской разгул",
    "Water Hazard": "Водная опасность",
    "Waterfall": "Водопад",
    "Wave Breaker": "Волнолом",
    "Whack-a-Mole": "Ударь крота",
    "Wild West": "Дикий запад",
    "Windmill Field": "Поле ветряных мельниц",
    "Winter Party": "Зимняя вечеринка",
    "Wizard's Valley": "Долина волшебника",
    "X Marks the Spot": "X отмечает место",
    "Yeti Cave": "Пещера йети",
    "Zip Zap": "Зип-зап",
    "Zoom Zoom": "Зум зум",
}

# ── глобальные объекты ───────────────────────────────────────────────────────
db: Database
api: BrawlAPIClient
player_col: PlayerCollector
club_col: ClubCollector
search_mode = "offline"
SEARCH_MODE_FILE = "search_mode.txt"
HAS_API_KEYS = True
API_SERVER_URL = os.getenv("API_SERVER_URL", "http://130.12.46.224")
API_KEY = os.getenv("API_KEY", "")

def load_search_mode():
    global search_mode
    try:
        if os.path.exists(SEARCH_MODE_FILE):
            with open(SEARCH_MODE_FILE, "r") as f:
                mode = f.read().strip().lower()
                if mode in ("offline", "online"):
                    search_mode = mode
                    return
    except:
        pass
    search_mode = APP_CFG.get("search_mode", "offline")

def save_search_mode(mode: str):
    global search_mode
    search_mode = mode
    try:
        with open(SEARCH_MODE_FILE, "w") as f:
            f.write(mode)
    except:
        pass

# ═════════════════════════════════════════════════════════════════════════════
# GitHub API для поиска по репозиторию (упрощённо)
# ═════════════════════════════════════════════════════════════════════════════

async def get_github_files_list(repo_path: str) -> List[Dict]:
    repo_url = GITHUB_REPO_URL.rstrip(".git")
    parts = repo_url.split("/")
    owner = parts[-2]
    repo = parts[-1]
    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{repo_path}"
    headers = {}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    async with aiohttp.ClientSession() as session:
        async with session.get(api_url, headers=headers) as resp:
            if resp.status != 200:
                logger.error(f"GitHub API error: {resp.status}")
                return []
            data = await resp.json()
            return data

async def get_github_file_content(file_info: Dict) -> Optional[Dict]:
    if file_info.get("type") != "file":
        return None
    download_url = file_info.get("download_url")
    if not download_url:
        return None
    async with aiohttp.ClientSession() as session:
        async with session.get(download_url) as resp:
            if resp.status != 200:
                return None
            try:
                return await resp.json()
            except:
                return None

async def search_players_on_github(name: str) -> List[Dict]:
    files = await get_github_files_list("brawl_data/players")
    if not files:
        return []
    results = []
    name_lower = name.lower()
    for file_info in files:
        if not file_info.get("name", "").endswith(".json"):
            continue
        data = await get_github_file_content(file_info)
        if data and data.get("name"):
            if name_lower in data["name"].lower():
                results.append(data)
    return results

async def search_clubs_on_github(name: str) -> List[Dict]:
    files = await get_github_files_list("brawl_data/clubs")
    if not files:
        return []
    results = []
    name_lower = name.lower()
    for file_info in files:
        if not file_info.get("name", "").endswith(".json"):
            continue
        data = await get_github_file_content(file_info)
        if data and data.get("name"):
            if name_lower in data["name"].lower():
                results.append(data)
    return results

async def get_player_from_github(tag: str) -> Optional[Dict]:
    tag_upper = tag.upper().replace('#', '')
    files = await get_github_files_list("brawl_data/players")
    for file_info in files:
        if file_info.get("type") != "file":
            continue
        name = file_info.get("name", "")
        if name.endswith(".json") and name.replace(".json", "").upper() == tag_upper:
            return await get_github_file_content(file_info)
    return None

async def get_club_from_github(tag: str) -> Optional[Dict]:
    tag_upper = tag.upper().replace('#', '')
    files = await get_github_files_list("brawl_data/clubs")
    for file_info in files:
        if file_info.get("type") != "file":
            continue
        name = file_info.get("name", "")
        if name.endswith(".json") and name.replace(".json", "").upper() == tag_upper:
            return await get_github_file_content(file_info)
    return None

# ── глобальные объекты ───────────────────────────────────────────────────────
db: Database
api: BrawlAPIClient
player_col: PlayerCollector
club_col: ClubCollector
search_mode = "offline"
SEARCH_MODE_FILE = "search_mode.txt"
HAS_API_KEYS = True
API_KEY = os.getenv("API_KEY", "")

def load_search_mode():
    global search_mode
    try:
        if os.path.exists(SEARCH_MODE_FILE):
            with open(SEARCH_MODE_FILE, "r") as f:
                mode = f.read().strip().lower()
                if mode in ("offline", "online"):
                    search_mode = mode
                    return
    except:
        pass
    search_mode = APP_CFG.get("search_mode", "offline")

def save_search_mode(mode: str):
    global search_mode
    search_mode = mode
    try:
        with open(SEARCH_MODE_FILE, "w") as f:
            f.write(mode)
    except:
        pass

# ═════════════════════════════════════════════════════════════════════════════
# GitHub API
# ═════════════════════════════════════════════════════════════════════════════

async def get_github_files_list(repo_path: str) -> List[Dict]:
    repo_url = GITHUB_REPO_URL.rstrip(".git")
    parts = repo_url.split("/")
    owner = parts[-2]
    repo = parts[-1]
    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{repo_path}"
    headers = {}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    async with aiohttp.ClientSession() as session:
        async with session.get(api_url, headers=headers) as resp:
            if resp.status != 200:
                logger.error(f"GitHub API error: {resp.status}")
                return []
            data = await resp.json()
            return data

async def get_github_file_content(file_info: Dict) -> Optional[Dict]:
    if file_info.get("type") != "file":
        return None
    download_url = file_info.get("download_url")
    if not download_url:
        return None
    async with aiohttp.ClientSession() as session:
        async with session.get(download_url) as resp:
            if resp.status != 200:
                return None
            try:
                return await resp.json()
            except:
                return None

async def search_players_on_github(name: str) -> List[Dict]:
    files = await get_github_files_list("brawl_data/players")
    if not files:
        return []
    results = []
    name_lower = name.lower()
    for file_info in files:
        if not file_info.get("name", "").endswith(".json"):
            continue
        data = await get_github_file_content(file_info)
        if data and data.get("name"):
            if name_lower in data["name"].lower():
                results.append(data)
    return results

async def search_clubs_on_github(name: str) -> List[Dict]:
    files = await get_github_files_list("brawl_data/clubs")
    if not files:
        return []
    results = []
    name_lower = name.lower()
    for file_info in files:
        if not file_info.get("name", "").endswith(".json"):
            continue
        data = await get_github_file_content(file_info)
        if data and data.get("name"):
            if name_lower in data["name"].lower():
                results.append(data)
    return results

async def get_player_from_github(tag: str) -> Optional[Dict]:
    tag_upper = tag.upper().replace('#', '')
    files = await get_github_files_list("brawl_data/players")
    for file_info in files:
        if file_info.get("type") != "file":
            continue
        name = file_info.get("name", "")
        if name.endswith(".json") and name.replace(".json", "").upper() == tag_upper:
            return await get_github_file_content(file_info)
    return None

async def get_club_from_github(tag: str) -> Optional[Dict]:
    tag_upper = tag.upper().replace('#', '')
    files = await get_github_files_list("brawl_data/clubs")
    for file_info in files:
        if file_info.get("type") != "file":
            continue
        name = file_info.get("name", "")
        if name.endswith(".json") and name.replace(".json", "").upper() == tag_upper:
            return await get_github_file_content(file_info)
    return None

# ═════════════════════════════════════════════════════════════════════════════
# Инициализация
# ═════════════════════════════════════════════════════════════════════════════

async def _init():
    global db, api, player_col, club_col, HAS_API_KEYS
    db = Database()
    await db.connect()
    api = BrawlAPIClient()
    player_col = PlayerCollector(api, db)
    club_col = ClubCollector(api, db)
    load_search_mode()
    HAS_API_KEYS = bool(API_KEYS)
    if not HAS_API_KEYS:
        console.print("[yellow]⚠️  API ключи отсутствуют. Будут доступны только функции поиска по GitHub и просмотр сохранённых данных.[/yellow]")
        global search_mode
        if search_mode == "offline":
            search_mode = "online"
            save_search_mode("online")
            console.print("[dim]Режим поиска автоматически переключён на ОНЛАЙН (GitHub).[/dim]")

# ═════════════════════════════════════════════════════════════════════════════
# Утилиты вывода
# ═════════════════════════════════════════════════════════════════════════════

def _hr(label: str = ""):
    console.print(Rule(f"  {label}  " if label else "", style="dim #374151"))

def _kv(key: str, value: str, key_style: str = "dim", val_style: str = "white"):
    console.print(f"  [bold {key_style}]{key}[/bold {key_style}]  [{val_style}]{value}[/{val_style}]")

def _ok(msg: str):
    console.print(f"  [bold #22c55e]✓[/bold #22c55e]  {msg}")

def _err(msg: str):
    console.print(f"  [bold #ef4444]✗[/bold #ef4444]  {msg}")

def _info(msg: str):
    console.print(f"  [dim #9ca3af]{msg}[/dim #9ca3af]")

# ═════════════════════════════════════════════════════════════════════════════
# Асинхронный ввод
# ═════════════════════════════════════════════════════════════════════════════

async def _ask(prompt: str, default: str = "") -> str:
    try:
        val = await asyncio.to_thread(input, f"  {prompt}: ")
        return (val.strip() or default).strip()
    except (KeyboardInterrupt, EOFError):
        return default

async def _ask_int(prompt: str, default: int) -> int:
    raw = await _ask(f"{prompt} [{default}]", str(default))
    try:
        return int(raw)
    except (ValueError, TypeError):
        return default

# ═════════════════════════════════════════════════════════════════════════════
# Рейтинг (серверный)
# ═════════════════════════════════════════════════════════════════════════════

async def _add_rating_remote(action_type: str, object_id: Optional[str] = None):
    if not API_KEY or not API_SERVER_URL:
        return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{API_SERVER_URL}/rating/add",
                headers={"X-API-Key": API_KEY},
                json={"action_type": action_type, "object_id": object_id}
            ) as resp:
                if resp.status != 200:
                    logger.debug(f"Rating add failed: {resp.status}")
    except Exception as e:
        logger.debug(f"Rating add error: {e}")

async def _get_rating_remote() -> int:
    if not API_KEY or not API_SERVER_URL:
        return 0
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{API_SERVER_URL}/rating/my",
                headers={"X-API-Key": API_KEY}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("rating", 0)
    except Exception as e:
        logger.debug(f"Rating get error: {e}")
    return 0

async def _ensure_api_key():
    """Автоматически создаёт и сохраняет API-ключ, если его нет или он невалиден."""
    global API_KEY
    if API_KEY:
        # Проверим, что ключ валиден
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{API_SERVER_URL}/rating/my",
                    headers={"X-API-Key": API_KEY}
                ) as resp:
                    if resp.status == 200:
                        return  # ключ рабочий
        except Exception:
            pass
        # Ключ невалиден – генерируем новый
        _info("API ключ недействителен. Генерируем новый...")
    else:
        _info("API ключ не найден. Генерируем новый...")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{API_SERVER_URL}/generate_key",
                headers={"Content-Type": "application/json"},
                json={"name": "CLI_User", "daily_limit": 1000}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    new_key = data.get("key")
                    if new_key:
                        # Сохраняем в .env
                        env_path = os.path.join(os.path.dirname(__file__), ".env")
                        env_content = ""
                        if os.path.exists(env_path):
                            with open(env_path, "r") as f:
                                env_content = f.read()
                        if "API_KEY=" in env_content:
                            lines = env_content.splitlines()
                            new_lines = []
                            for line in lines:
                                if line.startswith("API_KEY="):
                                    new_lines.append(f"API_KEY={new_key}")
                                else:
                                    new_lines.append(line)
                            env_content = "\n".join(new_lines)
                        else:
                            env_content += f"\nAPI_KEY={new_key}\n"
                        with open(env_path, "w") as f:
                            f.write(env_content)
                        API_KEY = new_key
                        _ok(f"API ключ успешно создан и сохранён в .env")
                        return
        _err("Не удалось создать API ключ. Проверьте доступность сервера.")
        # Предложим ввести вручную
        choice = await _ask("Ввести API-ключ вручную? (y/n)", "n")
        if choice.lower() == "y":
            key = await _ask("Введите API-ключ")
            if key:
                env_path = os.path.join(os.path.dirname(__file__), ".env")
                env_content = ""
                if os.path.exists(env_path):
                    with open(env_path, "r") as f:
                        env_content = f.read()
                if "API_KEY=" in env_content:
                    lines = env_content.splitlines()
                    new_lines = []
                    for line in lines:
                        if line.startswith("API_KEY="):
                            new_lines.append(f"API_KEY={key}")
                        else:
                            new_lines.append(line)
                    env_content = "\n".join(new_lines)
                else:
                    env_content += f"\nAPI_KEY={key}\n"
                with open(env_path, "w") as f:
                    f.write(env_content)
                API_KEY = key
                _ok("Ключ сохранён в .env")
    except Exception as e:
        _err(f"Ошибка при генерации ключа: {e}")
        _info("Возможно, сервер недоступен. Вы можете ввести ключ вручную позже через меню '🔑 Ввести API-ключ'.")

# ═════════════════════════════════════════════════════════════════════════════
# Ввод API-ключа (ручной)
# ═════════════════════════════════════════════════════════════════════════════

async def input_api_key():
    """Запрашивает API-ключ у пользователя и сохраняет в .env."""
    print()
    _info("Для доступа к живому API Brawl Stars нужны ключи.")
    _info("Вы можете получить ключи на https://developer.brawlstars.com")
    key = await _ask("Введите один API-ключ (или несколько через запятую)")
    if not key:
        return
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    env_content = ""
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            env_content = f.read()
    if "API_KEYS=" in env_content:
        lines = env_content.splitlines()
        new_lines = []
        for line in lines:
            if line.startswith("API_KEYS="):
                new_lines.append(f"API_KEYS={key}")
            else:
                new_lines.append(line)
        env_content = "\n".join(new_lines)
    else:
        env_content += f"\nAPI_KEYS={key}\n"
    with open(env_path, "w") as f:
        f.write(env_content)
    _ok("Ключ сохранён в .env. Пожалуйста, перезапустите программу, чтобы изменения вступили в силу.")
    console.print()
    sys.exit(0)

# ═════════════════════════════════════════════════════════════════════════════
# Профиль игрока и другие основные функции (с проверкой наличия ключей)
# ═════════════════════════════════════════════════════════════════════════════

async def show_player(tag: str, force_update: bool = False):
    data = None
    if HAS_API_KEYS:
        normalized_tag = api.normalize_tag(tag)
        with console.status(f"[dim]Загрузка {normalized_tag}...[/dim]", spinner="dots"):
            data = await player_col.collect(normalized_tag, force_update=force_update)
        if not data:
            data = await get_player_from_github(tag)
            if data:
                _info("⚠️ Игрок загружен из GitHub (устаревшие данные).")
    else:
        data = await get_player_from_github(tag)

    if not data:
        _err(f"❌ Игрок {tag} не найден (ни в API, ни в GitHub).")
        _info("Возможно, вы хотите добавить API-ключ для получения свежих данных?")
        choice = await _ask("Ввести API-ключ сейчас? (y/n)", "n")
        if choice.lower() == "y":
            await input_api_key()
        return

    name = data['name']
    player_tag = f"#{data['tag'].lstrip('#')}"
    trophies = data['trophies']
    highest = data.get('highest_trophies', '?')
    exp_level = data.get('exp_level', '?')
    exp_points = data.get('exp_points', 0)
    wins_3v3 = data.get('wins_3v3', 0)
    wins_solo = data.get('wins_solo', 0)
    wins_duo = data.get('wins_duo', 0)
    club = data.get('club_tag')
    club_display = f"#{club.lstrip('#')}" if club else "[dim]—[/dim]"

    stats_table = Table(show_header=False, box=box.SIMPLE, padding=(0, 1))
    stats_table.add_column(style="bold #f97316", width=18)
    stats_table.add_column(style="white")

    stats_table.add_row("🏆 Трофеи", f"{trophies} [dim](макс {highest})[/dim]")
    stats_table.add_row("⭐ Уровень", f"{exp_level} [dim]({exp_points} XP)[/dim]")
    stats_table.add_row("🥇 Победы 3x3", str(wins_3v3))
    stats_table.add_row("🌵 Победы соло", str(wins_solo))
    stats_table.add_row("👥 Победы дуо", str(wins_duo))
    stats_table.add_row("🏠 Клуб", club_display)

    panel = Panel(
        stats_table,
        title=f"[bold cyan]{name}[/bold cyan] [dim]{player_tag}[/dim]",
        border_style="bright_blue",
        box=box.ROUNDED,
        padding=(0, 1)
    )
    console.print(panel)
    console.print()
    await _add_rating_remote("player_view", tag)

async def show_battles(tag: str, limit: int = 10):
    battles = []
    if HAS_API_KEYS:
        with console.status(f"[dim]Загрузка боёв {tag}...[/dim]", spinner="dots"):
            await player_col.collect(tag)
            battles = await db.get_battles(tag.replace("#", ""), limit=limit)
    else:
        player_data = await get_player_from_github(tag)
        if player_data and player_data.get("battles"):
            battles = player_data.get("battles")[:limit]
            _info("⚠️ Бои загружены из GitHub (могут быть неактуальны).")

    if not battles:
        _err("❌ Боевой лог пуст или игрок не найден")
        if not HAS_API_KEYS:
            _info("Попробуйте добавить API-ключ для получения актуальных данных.")
            choice = await _ask("Ввести API-ключ сейчас? (y/n)", "n")
            if choice.lower() == "y":
                await input_api_key()
        return

    console.print()
    _hr(f"📜 Последние бои · {tag}")
    table = Table(box=box.MINIMAL, show_header=True,
                  header_style="dim #9ca3af", padding=(0, 2))
    table.add_column("Время", style="dim", min_width=16)
    table.add_column("Режим", min_width=24)
    table.add_column("Карта", style="dim", min_width=20)
    table.add_column("Результат", min_width=12)
    table.add_column("Δ", justify="right", min_width=5)

    for b in battles:
        bt = (b.get("battle_time") or "")[:16]
        raw_mode = b.get("battle_mode") or "?"
        result = b.get("result") or "?"
        tch = b.get("trophies_change")
        map_name = ""
        if b.get("raw_data"):
            try:
                map_name = json.loads(b["raw_data"]).get("event", {}).get("map", "") or ""
            except Exception:
                pass

        res_str = (
            "[#4ade80]✔ Победа[/#4ade80]" if result == "victory" else
            "[#f87171]✘ Поражение[/#f87171]" if result == "defeat" else
            f"[dim]{result}[/dim]"
        )
        tch_str = (
            f"[#4ade80]+{tch}[/#4ade80]" if tch and tch > 0 else
            f"[#f87171]{tch}[/#f87171]" if tch and tch < 0 else
            "[dim]—[/dim]"
        )
        map_name_translated = MAP_TRANSLATIONS.get(map_name, map_name)
        table.add_row(bt, MODE_NAMES.get(raw_mode, raw_mode), map_name_translated, res_str, tch_str)

    console.print(table)
    _hr()
    console.print()
    await _add_rating_remote("battles_view", tag)

async def show_club(tag: str, show_members: bool = False):
    data = None
    if HAS_API_KEYS:
        with console.status(f"[dim]Загрузка клуба {tag}...[/dim]", spinner="dots"):
            data = await club_col.collect(tag)
        if not data:
            data = await get_club_from_github(tag)
            if data:
                _info("⚠️ Клуб загружен из GitHub (устаревшие данные).")
    else:
        data = await get_club_from_github(tag)

    if not data:
        _err(f"❌ Клуб {tag} не найден (ни в API, ни в GitHub).")
        choice = await _ask("Ввести API-ключ сейчас? (y/n)", "n")
        if choice.lower() == "y":
            await input_api_key()
        return

    console.print()
    _hr(f"🏢 {data['name']} [dim]#{data['tag'].lstrip('#')}[/dim]")
    _kv("🏆 Трофеи", str(data.get("trophies", 0)), "dim", "#4ade80")
    _kv("📜 Требуется", str(data.get("required_trophies", "?")), "dim", "white")
    _kv("👥 Участников", str(data.get("members_count", "?")), "dim", "white")
    _kv("🏷️ Тип", str(data.get("type", "?")), "dim", "white")
    if data.get("description"):
        _kv("📝 Описание", data["description"].strip(), "dim", "dim white")
    _hr()
    console.print()

    if show_members:
        members = await db.get_club_members(data["tag"])
        if members:
            _hr(f"👥 Участники ({len(members)})")
            table = Table(box=box.MINIMAL, show_header=True,
                          header_style="dim #9ca3af", padding=(0, 2))
            table.add_column("#", justify="right", style="dim", min_width=3)
            table.add_column("Имя", min_width=18)
            table.add_column("Тег", style="#67e8f9", min_width=12)
            table.add_column("Роль", style="dim", min_width=10)
            table.add_column("🏆", justify="right", min_width=8)
            for i, m in enumerate(members, 1):
                table.add_row(
                    str(i), m["name"], m["player_tag"],
                    m.get("role") or "—",
                    f"[#4ade80]{m['trophies']}[/#4ade80]",
                )
            console.print(table)
            _hr()
            console.print()
    await _add_rating_remote("club_view", tag)

# ═════════════════════════════════════════════════════════════════════════════
# Поиск игрока по нику (офлайн/онлайн через GitHub)
# ═════════════════════════════════════════════════════════════════════════════

async def search_player_by_name():
    name = await _ask("Введите имя игрока (полностью или часть)")
    if not name:
        return

    actual_mode = search_mode
    if not HAS_API_KEYS and actual_mode == "offline":
        actual_mode = "online"
        _info("API ключи отсутствуют — поиск выполняется в GitHub.")

    if actual_mode == "offline":
        players = await db.search_players_by_name(name, limit=50)
        if not players:
            _err("Ничего не найдено в локальной базе. Попробуйте переключиться в онлайн-режим или заполнить базу.")
            return
        console.print()
        _hr(f"🔍 Результаты поиска по имени: {name} (локальная база)")
        table = Table(box=box.MINIMAL, show_header=True,
                      header_style="dim #9ca3af", padding=(0, 2))
        table.add_column("#", justify="right", style="dim", min_width=3)
        table.add_column("Тег", style="#67e8f9", min_width=12)
        table.add_column("Имя", min_width=20)
        table.add_column("Трофеи", justify="right", min_width=8)
        table.add_column("Клуб", style="dim", min_width=12)
        for i, p in enumerate(players, 1):
            table.add_row(
                str(i),
                p.get("tag", "?"),
                p.get("name", "?"),
                f"[#4ade80]{p.get('trophies', 0)}[/#4ade80]",
                p.get("club_tag") or "—"
            )
        console.print(table)
        _hr()
        console.print()
        await _add_rating_remote("search_name", name)
        return

    _info("Поиск по репозиторию GitHub...")
    players = await search_players_on_github(name)
    if not players:
        _err(f"Не найдено игроков с именем, содержащим '{name}', в GitHub репозитории.")
        return

    console.print()
    _hr(f"🔍 Результаты поиска по имени: {name} (GitHub база)")
    table = Table(box=box.MINIMAL, show_header=True,
                  header_style="dim #9ca3af", padding=(0, 2))
    table.add_column("#", justify="right", style="dim", min_width=3)
    table.add_column("Тег", style="#67e8f9", min_width=12)
    table.add_column("Имя", min_width=20)
    table.add_column("Трофеи", justify="right", min_width=8)
    table.add_column("Клуб", style="dim", min_width=12)
    for i, p in enumerate(players, 1):
        table.add_row(
            str(i),
            p.get("tag", "?"),
            p.get("name", "?"),
            f"[#4ade80]{p.get('trophies', 0)}[/#4ade80]",
            p.get("club_tag") or "—"
        )
    console.print(table)
    _hr()
    console.print()
    await _add_rating_remote("search_name", name)

# ═════════════════════════════════════════════════════════════════════════════
# Сохранение статистики в PNG (требует API)
# ═════════════════════════════════════════════════════════════════════════════

async def save_player_stats_png(tag: str):
    if not PNG_AVAILABLE:
        _err("❌ Библиотеки matplotlib или Pillow не установлены. Установите: pip install matplotlib pillow")
        return
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно получить данные игрока.")
        return
    from pathlib import Path
    normalized_tag = api.normalize_tag(tag)
    with console.status(f"[dim]Загрузка {normalized_tag}...[/dim]", spinner="dots"):
        data = await player_col.collect(normalized_tag, force_update=False)
    if not data:
        last_status = api.last_status
        if last_status == 403:
            _err("❌ Ошибка 403 — ключ недействителен или истёк")
        elif last_status == 429:
            _err("⚠️ Ошибка 429 — превышен лимит запросов, подождите")
        elif last_status == 404:
            _err(f"❌ Игрок {normalized_tag} не найден.")
        else:
            _err("❌ Игрок не найден или ошибка API")
        return

    name = data['name']
    player_tag = data['tag']
    trophies = data['trophies']
    highest = data.get('highest_trophies', trophies)
    exp_level = data.get('exp_level', '?')
    exp_points = data.get('exp_points', 0)
    wins_3v3 = data.get('wins_3v3', 0)
    wins_solo = data.get('wins_solo', 0)
    wins_duo = data.get('wins_duo', 0)
    club = data.get('club_tag')
    club_display = club if club else "—"
    icon_id = data.get('icon_id', 0)

    avatar_img = None
    cache_dir = Path("icon_cache")
    cache_dir.mkdir(exist_ok=True)
    cache_file = cache_dir / f"{icon_id}.png"

    if cache_file.exists():
        try:
            avatar_img = Image.open(cache_file)
        except:
            pass

    if avatar_img is None:
        repo_url = GITHUB_REPO_URL.rstrip(".git")
        parts = repo_url.split("/")
        owner = parts[-2]
        repo = parts[-1]
        raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/icon/icon/{icon_id}.png"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(raw_url) as resp:
                    if resp.status == 200:
                        img_data = await resp.read()
                        with open(cache_file, "wb") as f:
                            f.write(img_data)
                        avatar_img = Image.open(io.BytesIO(img_data))
                    else:
                        unknown_path = Path("unknown_icon.webp")
                        if unknown_path.exists():
                            avatar_img = Image.open(unknown_path)
        except Exception as e:
            print(f"⚠️ Ошибка загрузки иконки {icon_id}: {e}")

    # Построение графика
    labels = ['3x3', 'Соло', 'Дуо']
    sizes = [wins_3v3, wins_solo, wins_duo]
    colors = ['#3b82f6', '#ef4444', '#10b981']
    explode = (0.05, 0.05, 0.05)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 9), facecolor='#0f172a')
    fig.patch.set_facecolor('#0f172a')
    ax1.set_facecolor('#0f172a')
    ax2.set_facecolor('#0f172a')

    wedges, texts, autotexts = ax1.pie(
        sizes,
        labels=labels,
        autopct=lambda pct: f'{pct:.1f}%' if pct > 0 else '',
        startangle=90,
        colors=colors,
        explode=explode,
        shadow=True,
        wedgeprops={'edgecolor': '#ffffff', 'linewidth': 1.5, 'alpha': 0.9},
        textprops={'color': 'white', 'fontsize': 12, 'weight': 'bold'}
    )
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontsize(13)
        autotext.set_weight('bold')
    ax1.set_title('Распределение побед', color='#facc15', fontsize=16, pad=20, weight='bold')

    stats_text = (
        f"🏆 {name}  {player_tag}\n\n"
        f"Трофеи: {trophies} (макс {highest})\n"
        f"Уровень: {exp_level} ({exp_points} XP)\n"
        f"Победы 3x3: {wins_3v3}\n"
        f"Победы соло: {wins_solo}\n"
        f"Победы дуо: {wins_duo}\n"
        f"Клуб: {club_display}\n"
        f"Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
    )
    ax2.text(0.1, 0.5, stats_text, transform=ax2.transAxes,
             fontsize=13, verticalalignment='center', linespacing=1.5,
             bbox=dict(boxstyle='round,pad=0.5', facecolor='#1e293b', edgecolor='#facc15', linewidth=2, alpha=0.9),
             color='#e2e8f0', weight='normal')
    ax2.axis('off')

    fig.suptitle('Brawl Stars Stats', color='#facc15', fontsize=18, weight='bold', y=0.98)

    if avatar_img:
        import numpy as np
        avatar_array = np.array(avatar_img.convert('RGB'))
        fig.figimage(avatar_array, xo=int(fig.bbox.xmax - 90), yo=int(fig.bbox.ymax - 90), alpha=1, zorder=10)

    logo_text = "BrawlStatsBot"
    fig.text(0.95, 0.95, logo_text, transform=fig.transFigure,
             fontsize=14, weight='bold', color='#facc15', alpha=0.7,
             ha='right', va='top', fontfamily='sans-serif',
             bbox=dict(boxstyle='round,pad=0.2', facecolor='#1e293b', edgecolor='#facc15', alpha=0.5))

    plt.tight_layout(pad=2.0)
    filename = f"player_{normalized_tag}.png"
    plt.savefig(filename, dpi=200, bbox_inches='tight', facecolor=fig.get_facecolor(), edgecolor='none')
    plt.close(fig)

    _ok(f"✅ Изображение сохранено: {filename}")
    console.print()
    await _add_rating_remote("save_png", tag)

# ═════════════════════════════════════════════════════════════════════════════
# Поиск клуба по названию (офлайн/онлайн через GitHub)
# ═════════════════════════════════════════════════════════════════════════════

async def search_club_by_name():
    name = await _ask("Введите название клуба (полностью или часть)")
    if not name:
        return

    actual_mode = search_mode
    if not HAS_API_KEYS and actual_mode == "offline":
        actual_mode = "online"
        _info("API ключи отсутствуют — поиск выполняется в GitHub.")

    if actual_mode == "offline":
        clubs = await db.search_clubs_by_name(name, limit=50)
        if not clubs:
            _err("Ничего не найдено в локальной базе. Попробуйте переключиться в онлайн-режим.")
            return
        console.print()
        _hr(f"🔍 Результаты поиска по названию: {name} (локальная база)")
        table = Table(box=box.MINIMAL, show_header=True,
                      header_style="dim #9ca3af", padding=(0, 2))
        table.add_column("#", justify="right", style="dim", min_width=3)
        table.add_column("Тег", style="#67e8f9", min_width=12)
        table.add_column("Название", min_width=20)
        table.add_column("Трофеи", justify="right", min_width=8)
        table.add_column("Участников", justify="right", min_width=8)
        for i, c in enumerate(clubs, 1):
            table.add_row(
                str(i),
                c.get("tag", "?"),
                c.get("name", "?"),
                f"[#4ade80]{c.get('trophies', 0)}[/#4ade80]",
                str(c.get("members_count", 0))
            )
        console.print(table)
        _hr()
        console.print()
        await _add_rating_remote("search_club_name", name)
        return

    _info("Поиск по репозиторию GitHub...")
    clubs = await search_clubs_on_github(name)
    if not clubs:
        _err(f"Не найдено клубов с названием, содержащим '{name}', в GitHub репозитории.")
        return

    console.print()
    _hr(f"🔍 Результаты поиска по названию: {name} (GitHub база)")
    table = Table(box=box.MINIMAL, show_header=True,
                  header_style="dim #9ca3af", padding=(0, 2))
    table.add_column("#", justify="right", style="dim", min_width=3)
    table.add_column("Тег", style="#67e8f9", min_width=12)
    table.add_column("Название", min_width=20)
    table.add_column("Трофеи", justify="right", min_width=8)
    table.add_column("Участников", justify="right", min_width=8)
    for i, c in enumerate(clubs, 1):
        table.add_row(
            str(i),
            c.get("tag", "?"),
            c.get("name", "?"),
            f"[#4ade80]{c.get('trophies', 0)}[/#4ade80]",
            str(c.get("members_count", 0))
        )
    console.print(table)
    _hr()
    console.print()
    await _add_rating_remote("search_club_name", name)

# ═════════════════════════════════════════════════════════════════════════════
# Остальные функции (требующие API) оставлены без изменений (проверка HAS_API_KEYS)
# Здесь нужно вставить остальные функции из предыдущего кода,
# но для краткости я показываю только принцип добавления рейтинга.
# В итоговом файле должны быть все функции, которые были в предыдущей версии,
# с добавленными вызовами _add_rating_remote для соответствующих действий.
# Из-за ограничения длины сообщения я не могу привести весь код целиком,
# но вы можете взять предыдущую версию cli.py и добавить вызовы рейтинга,
# как показано выше.
# Для завершения работы нужно также добавить пункт меню "⭐ Мой рейтинг" и функцию show_rating.
# Давайте это сделаем.

# ═════════════════════════════════════════════════════════════════════════════
# Просмотр рейтинга
# ═════════════════════════════════════════════════════════════════════════════

async def show_rating():
    rating = await _get_rating_remote()
    console.print()
    _hr("⭐ Ваш рейтинг")
    _kv("Очки", str(rating), "dim", "yellow")
    _info("Рейтинг повышается за каждое полезное действие: просмотр профилей, поиск, сохранение PNG, заполнение базы и т.д.")
    _hr()
    console.print()

# ═════════════════════════════════════════════════════════════════════════════
# Интерактивное меню (добавлен пункт "rating")
# ═════════════════════════════════════════════════════════════════════════════

MENU_ITEMS = [
    ("separator",    "📊 ИГРОКИ"),
    ("player",       "👤 Профиль игрока"),
    ("battles",      "📜 Последние бои"),
    ("update",       "🔄 Принудительное обновление игрока"),
    ("search_name",  "🔍 Поиск игрока по нику"),
    ("random_player","🎲 Случайный существующий игрок"),
    ("save_png",     "📸 Сохранить статистику в PNG"),
    ("separator",    "🏢 КЛУБЫ"),
    ("club",         "🏢 Информация о клубе"),
    ("club_members", "👥 Участники клуба"),
    ("full_club",    "📊 Полный сбор данных клуба"),
    ("check_club",   "🏢 Проверить существование клуба по тегу"),
    ("search_clubs", "🔍 Поиск существующих клубов"),
    ("search_club_name", "🔍 Поиск клуба по названию"),
    ("separator",    "🔍 ПОИСК"),
    ("search",       "🔍 Поиск существующих игроков"),
    ("checkfile",    "📁 Проверить теги из файла"),
    ("checkfile_active", "📁 Проверить теги из файла (только активные)"),
    ("check_team",   "🎮 Проверить командную игру по тегам"),
    ("separator",    "🎮 ИГРОВОЙ КОНТЕНТ"),
    ("brawlers",     "🤖 Список бравлеров"),
    ("rotation",     "🎡 Текущая ротация событий"),
    ("rank_players", "🏆 Топ-игроки (рейтинг)"),
    ("rank_clubs",   "🏅 Топ-клубы (рейтинг)"),
    ("locations",    "🌍 Список стран"),
    ("powerplay",    "⚡ Сезоны Power Play"),
    ("separator",    "⚙️ НАСТРОЙКИ"),
    ("check_keys",   "🔑 Проверить API ключи"),
    ("enter_api_key","🔑 Ввести API-ключ"),
    ("set_mode",     "🌐 Режим поиска (офлайн/онлайн)"),
    ("gen_codes",    "🔑 Генерация кодов командной игры"),
    ("list_codes",   "📋 Показать коды из базы"),
    ("cleanup_codes","🧹 Очистить истёкшие коды"),
    ("fill_db",      "📥 Заполнить базу данных (однократно)"),
    ("continuous_fill", "🔄 Непрерывное заполнение БД"),
    ("sync_push",    "⬆️ Выгрузить данные в GitHub"),
    ("sync_pull",    "⬇️ Загрузить данные из GitHub"),
    ("rating",       "⭐ Мой рейтинг"),
    ("separator",    "🚪 ВЫХОД"),
    ("exit",         "🚪 Выход"),
]

# Удалим сепараторы из списка для навигации
_MENU_ITEMS_FILTERED = [(k, v) for k, v in MENU_ITEMS if k != "separator"]
_N = len(_MENU_ITEMS_FILTERED)

def _run_menu() -> str:
    """Блокирующее интерактивное меню с категориями (сепараторы не выбираются)."""
    idx = [0]
    result: list = [None]

    # Создаём список для отображения: каждый элемент – (value, label, is_separator)
    display_items = []
    for value, label in MENU_ITEMS:
        if value == "separator":
            display_items.append(("separator", label, True))
        else:
            display_items.append((value, label, False))

    def get_fragments():
        fragments = []
        for i, (val, label, is_sep) in enumerate(display_items):
            if is_sep:
                fragments.append(('class:dim', f'\n {label}\n'))
            else:
                if i == idx[0]:
                    fragments.append(('class:selected', f' ❯ {label}\n'))
                else:
                    fragments.append(('class:item', f'   {label}\n'))
        return fragments

    ctrl = FormattedTextControl(text=get_fragments, focusable=False)
    kb = KeyBindings()

    # Перемещаемся только по не-сепараторам
    def get_next_non_separator(current):
        next_idx = current
        while True:
            next_idx = (next_idx + 1) % len(display_items)
            if display_items[next_idx][0] != "separator":
                return next_idx
    def get_prev_non_separator(current):
        prev_idx = current
        while True:
            prev_idx = (prev_idx - 1) % len(display_items)
            if display_items[prev_idx][0] != "separator":
                return prev_idx

    @kb.add("up")
    @kb.add("s-tab")
    def _up(event):
        idx[0] = get_prev_non_separator(idx[0])
        ctrl.text = get_fragments

    @kb.add("down")
    @kb.add("tab")
    def _down(event):
        idx[0] = get_next_non_separator(idx[0])
        ctrl.text = get_fragments

    @kb.add("enter")
    def _enter(event):
        val = display_items[idx[0]][0]
        if val != "separator":
            result[0] = val
            event.app.exit()

    @kb.add("q")
    @kb.add("c-c")
    def _quit(event):
        result[0] = "exit"
        event.app.exit()

    app = PTApp(
        layout=Layout(HSplit([Window(content=ctrl)])),
        key_bindings=kb,
        style=PT_STYLE,
        mouse_support=False,
        full_screen=False,
    )
    app.run()
    return result[0] or "exit"


# ═════════════════════════════════════════════════════════════════════════════
# Асинхронный ввод
# ═════════════════════════════════════════════════════════════════════════════

async def _ask(prompt: str, default: str = "") -> str:
    try:
        val = await asyncio.to_thread(input, f"  {prompt}: ")
        return (val.strip() or default).strip()
    except (KeyboardInterrupt, EOFError):
        return default


async def _ask_int(prompt: str, default: int) -> int:
    raw = await _ask(f"{prompt} [{default}]", str(default))
    try:
        return int(raw)
    except (ValueError, TypeError):
        return default


# ═════════════════════════════════════════════════════════════════════════════
# Ввод API-ключа
# ═════════════════════════════════════════════════════════════════════════════

async def _ensure_api_key():
    """Автоматически создаёт и сохраняет API-ключ, если его нет или он невалиден."""
    global API_KEY
    if API_KEY:
        # Проверим, что ключ валиден
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{API_SERVER_URL}/rating/my",
                    headers={"X-API-Key": API_KEY}
                ) as resp:
                    if resp.status == 200:
                        return  # ключ рабочий
        except Exception:
            pass
        # Ключ невалиден – генерируем новый
        _info("API ключ недействителен. Генерируем новый...")
    else:
        _info("API ключ не найден. Генерируем новый...")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{API_SERVER_URL}/generate_key",
                headers={"Content-Type": "application/json"},
                json={"name": "CLI_User", "daily_limit": 1000}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    new_key = data.get("key")
                    if new_key:
                        # Сохраняем в .env
                        env_path = os.path.join(os.path.dirname(__file__), ".env")
                        env_content = ""
                        if os.path.exists(env_path):
                            with open(env_path, "r") as f:
                                env_content = f.read()
                        if "API_KEY=" in env_content:
                            lines = env_content.splitlines()
                            new_lines = []
                            for line in lines:
                                if line.startswith("API_KEY="):
                                    new_lines.append(f"API_KEY={new_key}")
                                else:
                                    new_lines.append(line)
                            env_content = "\n".join(new_lines)
                        else:
                            env_content += f"\nAPI_KEY={new_key}\n"
                        with open(env_path, "w") as f:
                            f.write(env_content)
                        API_KEY = new_key
                        _ok(f"API ключ успешно создан и сохранён в .env")
                        return
        _err("Не удалось создать API ключ. Проверьте доступность сервера.")
        # Предложим ввести вручную
        choice = await _ask("Ввести API-ключ вручную? (y/n)", "n")
        if choice.lower() == "y":
            key = await _ask("Введите API-ключ")
            if key:
                env_path = os.path.join(os.path.dirname(__file__), ".env")
                env_content = ""
                if os.path.exists(env_path):
                    with open(env_path, "r") as f:
                        env_content = f.read()
                if "API_KEY=" in env_content:
                    lines = env_content.splitlines()
                    new_lines = []
                    for line in lines:
                        if line.startswith("API_KEY="):
                            new_lines.append(f"API_KEY={key}")
                        else:
                            new_lines.append(line)
                    env_content = "\n".join(new_lines)
                else:
                    env_content += f"\nAPI_KEY={key}\n"
                with open(env_path, "w") as f:
                    f.write(env_content)
                API_KEY = key
                _ok("Ключ сохранён в .env")
    except Exception as e:
        _err(f"Ошибка при генерации ключа: {e}")
        _info("Возможно, сервер недоступен. Вы можете ввести ключ вручную позже через меню '🔑 Ввести API-ключ'.")

async def input_api_key():
    """Запрашивает API-ключ у пользователя и сохраняет в .env."""
    print()
    _info("Для доступа к живому API Brawl Stars нужны ключи.")
    _info("Вы можете получить ключи на https://developer.brawlstars.com")
    key = await _ask("Введите один API-ключ (или несколько через запятую)")
    if not key:
        return
    # Читаем существующий .env
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    env_content = ""
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            env_content = f.read()
    # Обновляем или добавляем API_KEYS
    if "API_KEYS=" in env_content:
        lines = env_content.splitlines()
        new_lines = []
        for line in lines:
            if line.startswith("API_KEYS="):
                new_lines.append(f"API_KEYS={key}")
            else:
                new_lines.append(line)
        env_content = "\n".join(new_lines)
    else:
        env_content += f"\nAPI_KEYS={key}\n"
    with open(env_path, "w") as f:
        f.write(env_content)
    _ok("Ключ сохранён в .env. Пожалуйста, перезапустите программу, чтобы изменения вступили в силу.")
    console.print()
    sys.exit(0)


# ═════════════════════════════════════════════════════════════════════════════
# Проверка API ключей
# ═════════════════════════════════════════════════════════════════════════════

async def check_api_keys():
    if not HAS_API_KEYS:
        _err("❌ API ключи отсутствуют. Добавьте их в .env файл.")
        return
    with console.status("[dim]Проверка ключей...[/dim]", spinner="dots"):
        data = await api.get_brawlers()
    if data and "items" in data:
        _ok(f"✅ Ключи работают! Найдено {len(data['items'])} бравлеров")
    else:
        last_status = api.last_status
        if last_status == 403:
            _err("❌ Ключи недействительны или истекли")
        elif last_status == 429:
            _err("⚠️ Превышен лимит запросов, подождите")
        else:
            _err("❓ Не удалось проверить ключи")
    console.print()


# ═════════════════════════════════════════════════════════════════════════════
# Профиль игрока и другие основные функции (с проверкой наличия ключей)
# ═════════════════════════════════════════════════════════════════════════════

async def show_player(tag: str, force_update: bool = False):
    data = None
    if HAS_API_KEYS:
        normalized_tag = api.normalize_tag(tag)
        with console.status(f"[dim]Загрузка {normalized_tag}...[/dim]", spinner="dots"):
            data = await player_col.collect(normalized_tag, force_update=force_update)
        if not data:
            # Попробуем загрузить из GitHub
            data = await get_player_from_github(tag)
            if data:
                _info(f"⚠️ Игрок загружен из GitHub (устаревшие данные).")
    else:
        # Нет ключей — сразу идём в GitHub
        data = await get_player_from_github(tag)

    if not data:
        _err(f"❌ Игрок {tag} не найден (ни в API, ни в GitHub).")
        _info("Возможно, вы хотите добавить API-ключ для получения свежих данных?")
        choice = await _ask("Ввести API-ключ сейчас? (y/n)", "n")
        if choice.lower() == "y":
            await input_api_key()
        return

    name = data['name']
    player_tag = f"#{data['tag'].lstrip('#')}"
    trophies = data['trophies']
    highest = data.get('highest_trophies', '?')
    exp_level = data.get('exp_level', '?')
    exp_points = data.get('exp_points', 0)
    wins_3v3 = data.get('wins_3v3', 0)
    wins_solo = data.get('wins_solo', 0)
    wins_duo = data.get('wins_duo', 0)
    club = data.get('club_tag')
    club_display = f"#{club.lstrip('#')}" if club else "[dim]—[/dim]"

    stats_table = Table(show_header=False, box=box.SIMPLE, padding=(0, 1))
    stats_table.add_column(style="bold #f97316", width=18)
    stats_table.add_column(style="white")

    stats_table.add_row("🏆 Трофеи", f"{trophies} [dim](макс {highest})[/dim]")
    stats_table.add_row("⭐ Уровень", f"{exp_level} [dim]({exp_points} XP)[/dim]")
    stats_table.add_row("🥇 Победы 3x3", str(wins_3v3))
    stats_table.add_row("🌵 Победы соло", str(wins_solo))
    stats_table.add_row("👥 Победы дуо", str(wins_duo))
    stats_table.add_row("🏠 Клуб", club_display)

    panel = Panel(
        stats_table,
        title=f"[bold cyan]{name}[/bold cyan] [dim]{player_tag}[/dim]",
        border_style="bright_blue",
        box=box.ROUNDED,
        padding=(0, 1)
    )
    console.print(panel)
    console.print()
    await _add_rating_remote("player_view", tag)


async def show_battles(tag: str, limit: int = 10):
    battles = []
    if HAS_API_KEYS:
        with console.status(f"[dim]Загрузка боёв {tag}...[/dim]", spinner="dots"):
            await player_col.collect(tag)
            battles = await db.get_battles(tag.replace("#", ""), limit=limit)
    else:
        player_data = await get_player_from_github(tag)
        if player_data and player_data.get("battles"):
            battles = player_data.get("battles")[:limit]
            _info("⚠️ Бои загружены из GitHub (могут быть неактуальны).")

    if not battles:
        _err("❌ Боевой лог пуст или игрок не найден")
        if not HAS_API_KEYS:
            _info("Попробуйте добавить API-ключ для получения актуальных данных.")
            choice = await _ask("Ввести API-ключ сейчас? (y/n)", "n")
            if choice.lower() == "y":
                await input_api_key()
        return

    console.print()
    _hr(f"📜 Последние бои · {tag}")
    table = Table(box=box.MINIMAL, show_header=True,
                  header_style="dim #9ca3af", padding=(0, 2))
    table.add_column("Время", style="dim", min_width=16)
    table.add_column("Режим", min_width=24)
    table.add_column("Карта", style="dim", min_width=20)
    table.add_column("Результат", min_width=12)
    table.add_column("Δ", justify="right", min_width=5)

    for b in battles:
        bt = (b.get("battle_time") or "")[:16]
        raw_mode = b.get("battle_mode") or "?"
        result = b.get("result") or "?"
        tch = b.get("trophies_change")
        map_name = ""
        if b.get("raw_data"):
            try:
                map_name = json.loads(b["raw_data"]).get("event", {}).get("map", "") or ""
            except Exception:
                pass

        res_str = (
            "[#4ade80]✔ Победа[/#4ade80]" if result == "victory" else
            "[#f87171]✘ Поражение[/#f87171]" if result == "defeat" else
            f"[dim]{result}[/dim]"
        )
        tch_str = (
            f"[#4ade80]+{tch}[/#4ade80]" if tch and tch > 0 else
            f"[#f87171]{tch}[/#f87171]" if tch and tch < 0 else
            "[dim]—[/dim]"
        )
        map_name_translated = MAP_TRANSLATIONS.get(map_name, map_name)
        table.add_row(bt, MODE_NAMES.get(raw_mode, raw_mode), map_name_translated, res_str, tch_str)

    console.print(table)
    _hr()
    console.print()
    await _add_rating_remote("battles_view", tag)


async def show_club(tag: str, show_members: bool = False):
    data = None
    if HAS_API_KEYS:
        with console.status(f"[dim]Загрузка клуба {tag}...[/dim]", spinner="dots"):
            data = await club_col.collect(tag)
        if not data:
            data = await get_club_from_github(tag)
            if data:
                _info("⚠️ Клуб загружен из GitHub (устаревшие данные).")
    else:
        data = await get_club_from_github(tag)

    if not data:
        _err(f"❌ Клуб {tag} не найден (ни в API, ни в GitHub).")
        choice = await _ask("Ввести API-ключ сейчас? (y/n)", "n")
        if choice.lower() == "y":
            await input_api_key()
        return

    console.print()
    _hr(f"🏢 {data['name']} [dim]#{data['tag'].lstrip('#')}[/dim]")
    _kv("🏆 Трофеи", str(data.get("trophies", 0)), "dim", "#4ade80")
    _kv("📜 Требуется", str(data.get("required_trophies", "?")), "dim", "white")
    _kv("👥 Участников", str(data.get("members_count", "?")), "dim", "white")
    _kv("🏷️ Тип", str(data.get("type", "?")), "dim", "white")
    if data.get("description"):
        _kv("📝 Описание", data["description"].strip(), "dim", "dim white")
    _hr()
    console.print()

    if show_members:
        members = await db.get_club_members(data["tag"])
        if members:
            _hr(f"👥 Участники ({len(members)})")
            table = Table(box=box.MINIMAL, show_header=True,
                          header_style="dim #9ca3af", padding=(0, 2))
            table.add_column("#", justify="right", style="dim", min_width=3)
            table.add_column("Имя", min_width=18)
            table.add_column("Тег", style="#67e8f9", min_width=12)
            table.add_column("Роль", style="dim", min_width=10)
            table.add_column("🏆", justify="right", min_width=8)
            for i, m in enumerate(members, 1):
                table.add_row(
                    str(i), m["name"], m["player_tag"],
                    m.get("role") or "—",
                    f"[#4ade80]{m['trophies']}[/#4ade80]",
                )
            console.print(table)
            _hr()
            console.print()
    await _add_rating_remote("club_view", tag)


async def show_brawlers():
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно получить список бравлеров.")
        return
    with console.status("[dim]Загрузка бравлеров...[/dim]", spinner="dots"):
        data = await api.get_brawlers()
    if not data or "items" not in data:
        _err("❌ Не удалось загрузить бравлеров")
        return
    console.print()
    _hr("🤖 Список бравлеров")
    table = Table(box=box.MINIMAL, show_header=True,
                  header_style="dim #9ca3af", padding=(0, 2))
    table.add_column("ID", justify="right", style="dim", min_width=6)
    table.add_column("Имя", min_width=20)
    for b in data["items"]:
        table.add_row(str(b.get("id", "?")), b.get("name", "?"))
    console.print(table)
    _hr()
    console.print()
    await _add_rating_remote("brawlers_view")


async def show_event_rotation():
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно получить ротацию.")
        return
    with console.status("[dim]Загрузка ротации...[/dim]", spinner="dots"):
        data = await api.get_event_rotation()
    if not data:
        last_status = api.last_status
        if last_status == 403:
            _err("❌ Ошибка 403 — ключ недействителен или истёк")
        elif last_status == 429:
            _err("⚠️ Ошибка 429 — превышен лимит запросов, подождите")
        elif last_status == 404:
            _err("❌ Ротация событий не найдена (возможно, эндпоинт изменился)")
        else:
            _err("❌ Не удалось загрузить ротацию")
        return

    events = None
    if isinstance(data, list):
        events = data
    elif isinstance(data, dict):
        if "current" in data and "upcoming" in data:
            events = data.get("current", []) + data.get("upcoming", [])
        elif "items" in data:
            events = data["items"]
    if events is None:
        _err("❌ Неверный формат ответа от API")
        try:
            sample = json.dumps(data)[:200]
            _info(f"Получено: {sample}...")
        except:
            pass
        return

    if not events:
        _err("❌ Нет активных событий")
        return

    console.print()
    _hr("🎡 Текущая ротация событий")
    table = Table(box=box.MINIMAL, show_header=True,
                  header_style="dim #9ca3af", padding=(0, 2))
    table.add_column("Слот", justify="right", style="dim", min_width=5)
    table.add_column("Режим", min_width=24)
    table.add_column("Карта", style="dim", min_width=22)
    table.add_column("До", style="dim", min_width=16)

    for ev in events:
        slot = ev.get("slotId", ev.get("slot", "?"))
        event_info = ev.get("event", {})
        mode = event_info.get("mode", ev.get("mode", "?"))
        map_name = event_info.get("map", ev.get("map", "?"))
        end_time = ev.get("endTime", "")

        mode_name = MODE_NAMES.get(mode, mode)
        map_name_translated = MAP_TRANSLATIONS.get(map_name, map_name)

        if end_time:
            try:
                if 'T' in end_time:
                    dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                    end_time = dt.strftime("%d.%m %H:%M")
                else:
                    date_part = end_time[:8]
                    time_part = end_time[9:15] if len(end_time) > 9 else "??:??"
                    end_time = f"{date_part[6:8]}.{date_part[4:6]} {time_part[:2]}:{time_part[2:4]}"
            except:
                end_time = end_time[:16]

        table.add_row(str(slot), mode_name, map_name_translated, end_time)

    console.print(table)
    _hr()
    console.print()
    await _add_rating_remote("rotation_view")


async def show_rankings(region: str = "global", kind: str = "players"):
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно получить рейтинги.")
        return
    fn = api.get_rankings_players if kind == "players" else api.get_rankings_clubs
    with console.status(f"[dim]Загрузка топ {kind} ({region})...[/dim]", spinner="dots"):
        data = await fn(region)
    if not data or "items" not in data:
        _err("❌ Не удалось загрузить рейтинг")
        return
    console.print()
    _hr(f"🏆 Топ {kind} · {region}")
    table = Table(box=box.MINIMAL, show_header=True,
                  header_style="dim #9ca3af", padding=(0, 2))
    table.add_column("#", justify="right", style="dim", min_width=3)
    table.add_column("Имя", min_width=20)
    table.add_column("Тег", style="#67e8f9", min_width=12)
    table.add_column("Трофеи", justify="right", min_width=8)
    if kind == "clubs":
        table.add_column("Участников", justify="right", min_width=8)
    for i, item in enumerate(data["items"][:20], 1):
        row = [
            str(i), item.get("name", "?"), item.get("tag", "?"),
            f"[#4ade80]{item.get('trophies', 0)}[/#4ade80]",
        ]
        if kind == "clubs":
            row.append(str(item.get("memberCount", 0)))
        table.add_row(*row)
    console.print(table)
    _hr()
    console.print()
    await _add_rating_remote(f"{kind}_rankings", region)


async def show_powerplay_seasons(region: str = "global"):
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно получить сезоны Power Play.")
        return
    with console.status(f"[dim]Загрузка сезонов Power Play ({region})...[/dim]", spinner="dots"):
        data = await api.get_powerplay_seasons(region)
    if not data or "items" not in data:
        _err("❌ Не удалось загрузить сезоны")
        return
    console.print()
    _hr(f"⚡ Сезоны Power Play · {region}")
    table = Table(box=box.MINIMAL, show_header=True,
                  header_style="dim #9ca3af", padding=(0, 2))
    table.add_column("ID", style="dim", min_width=10)
    table.add_column("Название", min_width=20)
    table.add_column("Начало", style="dim", min_width=12)
    table.add_column("Конец", style="dim", min_width=12)
    for s in data["items"]:
        table.add_row(
            str(s.get("id", "?")), s.get("name", "?"),
            (s.get("startTime") or "")[:10],
            (s.get("endTime") or "")[:10],
        )
    console.print(table)
    _hr()
    console.print()
    await _add_rating_remote("powerplay_view", region)


async def show_locations():
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно получить список стран.")
        return
    with console.status("[dim]Загрузка локаций...[/dim]", spinner="dots"):
        data = await api.get_locations()
    if not data or "items" not in data:
        _err("❌ Ошибка загрузки локаций")
        return
    console.print()
    _hr("🌍 Список стран")
    table = Table(box=box.MINIMAL, show_header=True,
                  header_style="dim #9ca3af", padding=(0, 2))
    table.add_column("Код", style="#67e8f9", min_width=8)
    table.add_column("Название", min_width=24)
    for loc in data["items"]:
        table.add_row(loc.get("id", "?"), loc.get("name", "?"))
    console.print(table)
    _hr()
    console.print()
    await _add_rating_remote("locations_view")


async def full_club_collect(tag: str):
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно собрать данные клуба.")
        return
    with console.status(f"[dim]Загрузка клуба {tag}...[/dim]", spinner="dots"):
        club_data = await club_col.collect(tag)
    if not club_data:
        _err("❌ Клуб не найден")
        return
    members = await db.get_club_members(club_data["tag"])
    if not members:
        _err("❌ Нет участников для сбора")
        return
    _info(f"👥 Участников: {len(members)}")
    total_battles = 0
    i = 0
    try:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task("[cyan]Сбор данных участников...", total=len(members))
            for i, member in enumerate(members, 1):
                ptag = member["player_tag"]
                progress.update(task, advance=1, description=f"[cyan]Сбор данных участников... {i}/{len(members)}")
                await player_col.collect(ptag)
                battles = await db.get_battles(ptag, limit=999)
                total_battles += len(battles)
    except (KeyboardInterrupt, asyncio.CancelledError):
        _info("⏸️ Прервано — данные уже в БД")
    console.print()
    _ok(f"✅ Готово · участников: {i} · боёв в БД: {total_battles}")
    console.print()
    await _add_rating_remote("full_club_collect", tag)


# ═════════════════════════════════════════════════════════════════════════════
# Поиск игрока по нику (офлайн/онлайн через GitHub)
# ═════════════════════════════════════════════════════════════════════════════

async def search_player_by_name():
    name = await _ask("Введите имя игрока (полностью или часть)")
    if not name:
        return

    # Если нет ключей, принудительно используем GitHub
    actual_mode = search_mode
    if not HAS_API_KEYS and actual_mode == "offline":
        actual_mode = "online"
        _info("API ключи отсутствуют — поиск выполняется в GitHub.")

    if actual_mode == "offline":
        players = await db.search_players_by_name(name, limit=50)
        if not players:
            _err("Ничего не найдено в локальной базе. Попробуйте переключиться в онлайн-режим или заполнить базу.")
            return
        console.print()
        _hr(f"🔍 Результаты поиска по имени: {name} (локальная база)")
        table = Table(box=box.MINIMAL, show_header=True,
                      header_style="dim #9ca3af", padding=(0, 2))
        table.add_column("#", justify="right", style="dim", min_width=3)
        table.add_column("Тег", style="#67e8f9", min_width=12)
        table.add_column("Имя", min_width=20)
        table.add_column("Трофеи", justify="right", min_width=8)
        table.add_column("Клуб", style="dim", min_width=12)
        for i, p in enumerate(players, 1):
            table.add_row(
                str(i),
                p.get("tag", "?"),
                p.get("name", "?"),
                f"[#4ade80]{p.get('trophies', 0)}[/#4ade80]",
                p.get("club_tag") or "—"
            )
        console.print(table)
        _hr()
        console.print()
        await _add_rating_remote("search_name", name)
        return

    # Онлайн-поиск через GitHub
    _info("Поиск по репозиторию GitHub...")
    players = await search_players_on_github(name)
    if not players:
        _err(f"Не найдено игроков с именем, содержащим '{name}', в GitHub репозитории.")
        return

    console.print()
    _hr(f"🔍 Результаты поиска по имени: {name} (GitHub база)")
    table = Table(box=box.MINIMAL, show_header=True,
                  header_style="dim #9ca3af", padding=(0, 2))
    table.add_column("#", justify="right", style="dim", min_width=3)
    table.add_column("Тег", style="#67e8f9", min_width=12)
    table.add_column("Имя", min_width=20)
    table.add_column("Трофеи", justify="right", min_width=8)
    table.add_column("Клуб", style="dim", min_width=12)
    for i, p in enumerate(players, 1):
        table.add_row(
            str(i),
            p.get("tag", "?"),
            p.get("name", "?"),
            f"[#4ade80]{p.get('trophies', 0)}[/#4ade80]",
            p.get("club_tag") or "—"
        )
    console.print(table)
    _hr()
    console.print()
    await _add_rating_remote("search_name", name)


# ═════════════════════════════════════════════════════════════════════════════
# Сохранение статистики игрока в PNG
# ═════════════════════════════════════════════════════════════════════════════

async def save_player_stats_png(tag: str):
    if not PNG_AVAILABLE:
        _err("❌ Библиотеки matplotlib или Pillow не установлены. Установите: pip install matplotlib pillow")
        return
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно получить данные игрока.")
        return
    from pathlib import Path
    normalized_tag = api.normalize_tag(tag)
    with console.status(f"[dim]Загрузка {normalized_tag}...[/dim]", spinner="dots"):
        data = await player_col.collect(normalized_tag, force_update=False)
    if not data:
        last_status = api.last_status
        if last_status == 403:
            _err("❌ Ошибка 403 — ключ недействителен или истёк")
        elif last_status == 429:
            _err("⚠️ Ошибка 429 — превышен лимит запросов, подождите")
        elif last_status == 404:
            _err(f"❌ Игрок {normalized_tag} не найден.")
        else:
            _err("❌ Игрок не найден или ошибка API")
        return

    name = data['name']
    player_tag = data['tag']
    trophies = data['trophies']
    highest = data.get('highest_trophies', trophies)
    exp_level = data.get('exp_level', '?')
    exp_points = data.get('exp_points', 0)
    wins_3v3 = data.get('wins_3v3', 0)
    wins_solo = data.get('wins_solo', 0)
    wins_duo = data.get('wins_duo', 0)
    club = data.get('club_tag')
    club_display = club if club else "—"
    icon_id = data.get('icon_id', 0)

    # ── Загрузка иконки из репозитория ──────────────────────────────────────
    avatar_img = None
    cache_dir = Path("icon_cache")
    cache_dir.mkdir(exist_ok=True)
    cache_file = cache_dir / f"{icon_id}.png"

    if cache_file.exists():
        try:
            avatar_img = Image.open(cache_file)
        except:
            pass

    if avatar_img is None:
        # Строим URL на основе ветки icon
        repo_url = GITHUB_REPO_URL.rstrip(".git")
        parts = repo_url.split("/")
        owner = parts[-2]
        repo = parts[-1]
        raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/icon/icon/{icon_id}.png"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(raw_url) as resp:
                    if resp.status == 200:
                        img_data = await resp.read()
                        with open(cache_file, "wb") as f:
                            f.write(img_data)
                        avatar_img = Image.open(io.BytesIO(img_data))
                    else:
                        # Иконка не найдена – используем заглушку
                        unknown_path = Path("unknown_icon.webp")
                        if unknown_path.exists():
                            avatar_img = Image.open(unknown_path)
                        else:
                            print(f"⚠️ Иконка {icon_id} не найдена, и заглушка отсутствует")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки иконки {icon_id}: {e}")
            unknown_path = Path("unknown_icon.webp")
            if unknown_path.exists():
                avatar_img = Image.open(unknown_path)

    # ── Построение графика ───────────────────────────────────────────────────
    labels = ['3x3', 'Соло', 'Дуо']
    sizes = [wins_3v3, wins_solo, wins_duo]
    colors = ['#3b82f6', '#ef4444', '#10b981']
    explode = (0.05, 0.05, 0.05)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 9), facecolor='#0f172a')
    fig.patch.set_facecolor('#0f172a')
    ax1.set_facecolor('#0f172a')
    ax2.set_facecolor('#0f172a')

    wedges, texts, autotexts = ax1.pie(
        sizes,
        labels=labels,
        autopct=lambda pct: f'{pct:.1f}%' if pct > 0 else '',
        startangle=90,
        colors=colors,
        explode=explode,
        shadow=True,
        wedgeprops={'edgecolor': '#ffffff', 'linewidth': 1.5, 'alpha': 0.9},
        textprops={'color': 'white', 'fontsize': 12, 'weight': 'bold'}
    )
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontsize(13)
        autotext.set_weight('bold')
    ax1.set_title('Распределение побед', color='#facc15', fontsize=16, pad=20, weight='bold')

    stats_text = (
        f"🏆 {name}  {player_tag}\n\n"
        f"Трофеи: {trophies} (макс {highest})\n"
        f"Уровень: {exp_level} ({exp_points} XP)\n"
        f"Победы 3x3: {wins_3v3}\n"
        f"Победы соло: {wins_solo}\n"
        f"Победы дуо: {wins_duo}\n"
        f"Клуб: {club_display}\n"
        f"Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
    )
    ax2.text(0.1, 0.5, stats_text, transform=ax2.transAxes,
             fontsize=13, verticalalignment='center', linespacing=1.5,
             bbox=dict(boxstyle='round,pad=0.5', facecolor='#1e293b', edgecolor='#facc15', linewidth=2, alpha=0.9),
             color='#e2e8f0', weight='normal')
    ax2.axis('off')

    fig.suptitle('Brawl Stars Stats', color='#facc15', fontsize=18, weight='bold', y=0.98)

    # Вставляем аватарку (если загружена)
    if avatar_img:
        # Преобразуем PIL Image в массив numpy для matplotlib
        import numpy as np
        avatar_array = np.array(avatar_img.convert('RGB'))
        fig.figimage(avatar_array, xo=int(fig.bbox.xmax - 90), yo=int(fig.bbox.ymax - 90), alpha=1, zorder=10)

    # Логотип
    logo_text = "BrawlStatsBot"
    fig.text(0.95, 0.95, logo_text, transform=fig.transFigure,
             fontsize=14, weight='bold', color='#facc15', alpha=0.7,
             ha='right', va='top', fontfamily='sans-serif',
             bbox=dict(boxstyle='round,pad=0.2', facecolor='#1e293b', edgecolor='#facc15', alpha=0.5))

    plt.tight_layout(pad=2.0)
    filename = f"player_{normalized_tag}.png"
    plt.savefig(filename, dpi=200, bbox_inches='tight', facecolor=fig.get_facecolor(), edgecolor='none')
    plt.close(fig)

    _ok(f"✅ Изображение сохранено: {filename}")
    console.print()
    await _add_rating_remote("save_png", tag)


# ═════════════════════════════════════════════════════════════════════════════
# Поиск клуба по названию (офлайн/онлайн через GitHub)
# ═════════════════════════════════════════════════════════════════════════════

async def search_club_by_name():
    name = await _ask("Введите название клуба (полностью или часть)")
    if not name:
        return

    # Если нет ключей, принудительно используем GitHub
    actual_mode = search_mode
    if not HAS_API_KEYS and actual_mode == "offline":
        actual_mode = "online"
        _info("API ключи отсутствуют — поиск выполняется в GitHub.")

    if actual_mode == "offline":
        clubs = await db.search_clubs_by_name(name, limit=50)
        if not clubs:
            _err("Ничего не найдено в локальной базе. Попробуйте переключиться в онлайн-режим.")
            return
        console.print()
        _hr(f"🔍 Результаты поиска по названию: {name} (локальная база)")
        table = Table(box=box.MINIMAL, show_header=True,
                      header_style="dim #9ca3af", padding=(0, 2))
        table.add_column("#", justify="right", style="dim", min_width=3)
        table.add_column("Тег", style="#67e8f9", min_width=12)
        table.add_column("Название", min_width=20)
        table.add_column("Трофеи", justify="right", min_width=8)
        table.add_column("Участников", justify="right", min_width=8)
        for i, c in enumerate(clubs, 1):
            table.add_row(
                str(i),
                c.get("tag", "?"),
                c.get("name", "?"),
                f"[#4ade80]{c.get('trophies', 0)}[/#4ade80]",
                str(c.get("members_count", 0))
            )
        console.print(table)
        _hr()
        console.print()
        await _add_rating_remote("search_club_name", name)
        return

    # Онлайн-поиск через GitHub
    _info("Поиск по репозиторию GitHub...")
    clubs = await search_clubs_on_github(name)
    if not clubs:
        _err(f"Не найдено клубов с названием, содержащим '{name}', в GitHub репозитории.")
        return

    console.print()
    _hr(f"🔍 Результаты поиска по названию: {name} (GitHub база)")
    table = Table(box=box.MINIMAL, show_header=True,
                  header_style="dim #9ca3af", padding=(0, 2))
    table.add_column("#", justify="right", style="dim", min_width=3)
    table.add_column("Тег", style="#67e8f9", min_width=12)
    table.add_column("Название", min_width=20)
    table.add_column("Трофеи", justify="right", min_width=8)
    table.add_column("Участников", justify="right", min_width=8)
    for i, c in enumerate(clubs, 1):
        table.add_row(
            str(i),
            c.get("tag", "?"),
            c.get("name", "?"),
            f"[#4ade80]{c.get('trophies', 0)}[/#4ade80]",
            str(c.get("members_count", 0))
        )
    console.print(table)
    _hr()
    console.print()
    await _add_rating_remote("search_club_name", name)


# ═════════════════════════════════════════════════════════════════════════════
# Функции поиска существующих игроков/клубов (требуют API)
# ═════════════════════════════════════════════════════════════════════════════

async def check_active_players_from_file(file_path: str, days_threshold: int = 90):
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно проверить активность.")
        return
    if not os.path.exists(file_path):
        _err(f"Файл {file_path} не найден")
        return
    with open(file_path) as f:
        tags = [ln.strip() for ln in f if ln.strip()]
    if not tags:
        _err("Файл пуст")
        return

    base, ext = os.path.splitext(file_path)
    out_file = f"{base}_active{ext}"
    existing = set()
    if os.path.exists(out_file):
        with open(out_file) as f:
            existing = {line.strip().split(',')[0] for line in f if line.strip()}

    console.print(f"\n  [dim]Проверка {len(tags)} тегов на активность (последние {days_threshold} дней)...[/dim]")

    active_found = []
    total = len(tags)
    done = 0

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("[cyan]Проверка тегов...", total=total)
        for tag in tags:
            normalized = api.normalize_tag(tag)
            player_data = await api.get_player(normalized, force=True)
            if not player_data:
                done += 1
                progress.update(task, advance=1)
                continue

            battlelog = await api.get_battlelog(normalized, force=True)
            last_battle_date = None
            if battlelog and battlelog.get("items"):
                last_battle = battlelog["items"][0]
                battle_time = last_battle.get("battleTime")
                if battle_time:
                    last_battle_date = battle_time[:8]

            is_active = False
            if last_battle_date:
                try:
                    last_date = datetime.strptime(last_battle_date, "%Y%m%d")
                    if (datetime.now() - last_date).days <= days_threshold:
                        is_active = True
                except:
                    pass

            if is_active:
                active_found.append((tag, last_battle_date))
            done += 1
            progress.update(task, advance=1)

    console.print()
    with open(out_file, "w") as f:
        for tag, last_date in active_found:
            f.write(f"{tag},{last_date}\n")
    _ok(f"Готово. Активных: {len(active_found)} из {total} → {out_file}")
    console.print()
    await _add_rating_remote("check_active_players", file_path)


async def generate_command_codes():
    """
    Генерация кодов командной игры (Party Team Codes) для Brawl Stars.
    
    Формат кодов согласно документации:
    - Префикс: "XM" (2 заглавные буквы)
    - Основная часть: 5-7 символов (цифры 0-9 и заглавные буквы A-Z, кроме I и O)
    - Общая длина: 7-9 символов
    
    Валидные символы: 0123456789ABCDEFGHJKLMNPQRSTUVWXYZ (25 символов)
    Исключены I и O для избежания путаницы с 1 и 0
    
    Коды сохраняются в базу данных и удаленное хранилище (BrawlNest/brawl_data).
    """
    count = await _ask_int("Количество кодов", 100)
    code_length = await _ask_int("Длина кода (7, 8 или 9 символов)", 7)
    output_file = await _ask("Имя выходного файла", "game_codes.txt")
    if not output_file:
        output_file = "game_codes.txt"
    
    # Валидные символы для кодов (Base25 без I и O)
    valid_chars = "0123456789ABCDEFGHJKLMNPQRSTUVWXYZ"
    
    # Префикс XM обязателен
    prefix = "XM"
    
    # Проверяем длину кода (поддерживаем 7-9 символов)
    if code_length < 7:
        code_length = 7
    elif code_length > 9:
        code_length = 9
    
    # Длина основной части (без префикса)
    main_length = code_length - len(prefix)
    
    codes = set()  # Используем set для уникальности
    attempts = 0
    max_attempts = count * 10  # Защита от бесконечного цикла
    
    # Время истечения (10 часов)
    expires_at = (datetime.utcnow() + timedelta(hours=10)).isoformat()
    
    with console.status("[dim]Генерация кодов...[/dim]", spinner="dots"):
        while len(codes) < count and attempts < max_attempts:
            # Генерируем основную часть кода
            main_part = ''.join(random.choices(valid_chars, k=main_length))
            code = prefix + main_part
            codes.add(code)
            attempts += 1
    
    # Конвертируем в список и сортируем
    codes_list = sorted(list(codes))
    
    # Создаем данные для сохранения
    codes_data = {
        "codes": codes_list,
        "generated_at": datetime.utcnow().isoformat(),
        "code_length": code_length,
        "expires_in_hours": 10,
        "total_count": len(codes_list)
    }
    
    # Сохраняем в базу данных
    db = Database()
    await db.connect()
    try:
        for code in codes_list:
            await db.save_team_code(code, expires_at)
        _ok(f"Сохранено {len(codes_list)} кодов в базу данных")
        
        # Синхронизация с удалённым хранилищем
        with console.status("[dim]Синхронизация с BrawlNest...[/dim]", spinner="dots"):
            # Получаем все коды из БД для синхронизации
            all_codes = await db.get_all_valid_team_codes(limit=1000)
            db.sync_codes_to_remote(all_codes)
    finally:
        await db.close()
    
    # Записываем в локальный файл для удобства
    with open(output_file, "w") as f:
        for code in codes_list:
            f.write(f"{code}\n")
    
    _ok(f"Сгенерировано {len(codes_list)} уникальных кодов длиной {code_length}")
    _ok(f"Экспортировано в файл: {output_file}")
    
    # Показываем примеры
    console.print("\n[bold #f97316]Примеры сгенерированных кодов:[/bold #f97316]")
    examples = codes_list[:min(10, len(codes_list))]
    for i, code in enumerate(examples, 1):
        console.print(f"  {i}. [bold #22c55e]{code}[/bold #22c55e]")
    if len(codes_list) > 10:
        console.print(f"  ... и ещё {len(codes_list) - 10} кодов в файле")
    
    console.print("\n[dim]💡 Коды действительны в течение 10 часов с момента генерации.[/dim]")
    console.print()
    
    await _add_rating_remote("generate_codes")


async def list_team_codes():
    """
    Просмотр всех действительных кодов командной игры из удаленного хранилища.
    """
    with console.status("[dim]Загрузка кодов из удаленного хранилища...[/dim]", spinner="dots"):
        codes_data = remote_storage.get_all_data("codes")
    
    if not codes_data:
        _err("Нет кодов в удаленном хранилище.")
        console.print("\n[dim]Совет: используйте команду 'gen_codes' для генерации новых кодов.[/dim]")
        return
    
    # Собираем все коды из разных файлов
    all_codes = []
    for filename, data in codes_data.items():
        if isinstance(data, dict) and "codes" in data:
            codes_list = data.get("codes", [])
            generated_at = data.get("generated_at", "")
            expires_in = data.get("expires_in_hours", 10)
            
            # Вычисляем время истечения
            if generated_at:
                try:
                    gen_dt = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
                    expires_dt = gen_dt + timedelta(hours=expires_in)
                    now = datetime.now(gen_dt.tzinfo) if gen_dt.tzinfo else datetime.utcnow()
                    
                    # Фильтруем только действительные коды
                    if expires_dt > now:
                        for code in codes_list:
                            all_codes.append({
                                "code": code,
                                "created": gen_dt.strftime("%d.%m %H:%M"),
                                "expires": expires_dt.strftime("%d.%m %H:%M"),
                                "source": filename
                            })
                except Exception as e:
                    # Если ошибка парсинга даты, добавляем без проверки
                    for code in codes_list:
                        all_codes.append({
                            "code": code,
                            "created": generated_at[:16] if generated_at else "?",
                            "expires": "?",
                            "source": filename
                        })
            else:
                for code in codes_list:
                    all_codes.append({
                        "code": code,
                        "created": "?",
                        "expires": "?",
                        "source": filename
                    })
    
    if not all_codes:
        _err("Нет действительных кодов (все истекли).")
        console.print("\n[dim]Совет: используйте команду 'gen_codes' для генерации новых кодов.[/dim]")
        return
    
    console.print(f"\n[bold #f97316]Действительные коды командной игры ({len(all_codes)} шт.):[/bold #f97316]\n")
    
    # Группируем по длине
    codes_by_length = {}
    for item in all_codes:
        code = item["code"]
        length = len(code)
        if length not in codes_by_length:
            codes_by_length[length] = []
        codes_by_length[length].append(item)
    
    for length in sorted(codes_by_length.keys()):
        console.print(f"[bold]Коды длиной {length} символов:[/bold]")
        table = Table(box=box.SIMPLE, show_header=True, header_style="bold dim")
        table.add_column("#", style="dim", width=3)
        table.add_column("Код", style="#22c55e", width=12)
        table.add_column("Создан", style="dim", width=10)
        table.add_column("Истекает", style="yellow", width=10)
        
        for i, item in enumerate(codes_by_length[length], 1):
            table.add_row(
                str(i),
                item["code"],
                item["created"],
                item["expires"]
            )
        
        console.print(table)
        console.print()
    
    # Статистика
    total = len(all_codes)
    console.print(f"[dim]Всего действительных кодов: {total}[/dim]")
    
    await _add_rating_remote("list_codes")


async def cleanup_expired_codes_cmd():
    """
    Очистка истёкших кодов из удаленного хранилища.
    Примечание: это локальная операция, файлы в Git не удаляются автоматически.
    """
    with console.status("[dim]Проверка кодов на истечение...[/dim]", spinner="dots"):
        codes_data = remote_storage.get_all_data("codes")
    
    if not codes_data:
        _info("Нет кодов для очистки.")
        return
    
    expired_count = 0
    valid_count = 0
    
    now = datetime.utcnow()
    
    for filename, data in codes_data.items():
        if isinstance(data, dict) and "generated_at" in data:
            generated_at = data.get("generated_at", "")
            expires_in = data.get("expires_in_hours", 10)
            
            if generated_at:
                try:
                    gen_dt = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
                    expires_dt = gen_dt + timedelta(hours=expires_in)
                    
                    if expires_dt < now:
                        expired_count += len(data.get("codes", []))
                        # Помечаем файл как устаревший (можно добавить удаление)
                        console.print(f"[dim]  Истёк: {filename} ({len(data.get('codes', []))} кодов)[/dim]")
                    else:
                        valid_count += len(data.get("codes", []))
                except:
                    valid_count += len(data.get("codes", []))
            else:
                valid_count += len(data.get("codes", []))
    
    if expired_count > 0:
        console.print(f"\n[yellow]⚠️ Найдено {expired_count} истёкших кодов в {len(codes_data)} файлах.[/yellow]")
        console.print("[dim]Примечание: Автоматическое удаление файлов из Git не выполняется.[/dim]")
        console.print("[dim]Для удаления используйте команду 'git rm' вручную или очистите папку .brawlnest_data/data/codes[/dim]")
    else:
        _ok("Истёкших кодов не найдено.")
    
    _ok(f"Действительных кодов: {valid_count}")
    console.print()
    
    await _add_rating_remote("cleanup_codes")


async def check_club_by_tag(tag: str):
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно проверить клуб.")
        return
    normalized = api.normalize_tag(tag)
    with console.status(f"[dim]Проверка клуба {normalized}...[/dim]", spinner="dots"):
        data = await api.get_club(normalized, force=True)
    if data:
        _ok(f"✅ Клуб существует: {data['name']} (участников: {data.get('membersCount', 0)})")
        console.print()
        _kv("Название", data.get("name", "?"), "dim", "white")
        _kv("Тег", f"#{data.get('tag', '?').lstrip('#')}", "dim", "#67e8f9")
        _kv("Трофеи", str(data.get("trophies", 0)), "dim", "#4ade80")
        _kv("Участников", str(data.get("membersCount", 0)), "dim", "white")
        _kv("Требуется", str(data.get("requiredTrophies", "?")), "dim", "white")
        await _add_rating_remote("check_club", tag)
    else:
        _err(f"❌ Клуб {normalized} не найден")
    console.print()


async def search_existing_clubs(total_requests: int = 1000, output_file: Optional[str] = None):
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно выполнить поиск.")
        return
    if output_file is None:
        output_file = SEARCH_CFG.get("clubs_output_file", "existing_clubs.txt")
    existing: set[str] = set()
    if os.path.exists(output_file):
        with open(output_file) as f:
            existing = {line.strip() for line in f if line.strip()}
        _info(f"Загружено {len(existing)} существующих тегов клубов")

    tags = generate_tags(
        total_requests,
        SEARCH_CFG.get("tag_min_length", 7),
        SEARCH_CFG.get("tag_max_length", 9),
    )
    found, interrupted = await _run_search_engine("clubs", tags, existing, output_file, "Поиск клубов")
    all_tags = existing | set(found)
    status = "остановлен" if interrupted else "завершён"
    _ok(f"{status}  ·  новых: {len(found)}  всего: {len(all_tags)}  →  {output_file}")
    console.print()
    await _add_rating_remote("search_existing_clubs", output_file)


def _listen_for_stop(stop_event: threading.Event):
    try:
        import termios, tty
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        tty.setcbreak(fd)
        try:
            while not stop_event.is_set():
                ch = sys.stdin.read(1)
                if ch in ("q", "Q", "\x03", "\x04"):
                    stop_event.set()
                    break
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
    except Exception:
        pass


def _save_tags(found: list[str], existing: set[str], output_file: str) -> set[str]:
    all_tags = existing | set(found)
    with open(output_file, "w") as f:
        for t in sorted(all_tags):
            f.write(f"{t}\n")
    return all_tags


async def _run_search_engine(
    endpoint: str,  # "players" или "clubs"
    tags: list[str],
    existing: set[str],
    output_file: str,
    label: str,
) -> tuple[list[str], bool]:
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно выполнить поиск.")
        return [], True
    found: list[str] = []
    stop_event = threading.Event()
    found_lock = asyncio.Lock()
    rate_lock = asyncio.Lock()
    key_counts: dict[str, list] = {k: [] for k in API_KEYS}
    workers_per = SEARCH_CFG.get("workers_per_key", 10)
    interrupted = False
    done = 0
    total = len(tags)

    t_listener = threading.Thread(target=_listen_for_stop, args=(stop_event,), daemon=True)
    t_listener.start()

    queue: asyncio.Queue[str] = asyncio.Queue()
    for t in tags:
        await queue.put(t)

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task(f"[cyan]{label}...", total=total)

        async def worker(session: aiohttp.ClientSession, key: str):
            nonlocal done
            warned_403 = False
            warned_429 = False
            warned_other = False
            while not stop_event.is_set():
                try:
                    tag = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    if queue.empty():
                        break
                    continue
                except asyncio.CancelledError:
                    try:
                        queue.task_done()
                    except Exception:
                        pass
                    break

                async with rate_lock:
                    now = time.time()
                    key_counts[key] = [x for x in key_counts[key] if now - x < 60]
                    if len(key_counts[key]) >= 30:
                        await asyncio.sleep(60 - (now - key_counts[key][0]) + 0.1)
                        now = time.time()
                        key_counts[key] = [x for x in key_counts[key] if now - x < 60]
                    key_counts[key].append(now)

                try:
                    url = f"https://api.brawlstars.com/v1/{endpoint}/%23{tag}"
                    async with session.get(url, headers={"Authorization": f"Bearer {key}"}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            async with found_lock:
                                found.append(tag)
                        elif resp.status == 403:
                            if not warned_403:
                                warned_403 = True
                                print(f"\033[91m[ОШИБКА] 403 Forbidden\033[0m — ключ {key[:10]}… недействителен", file=sys.stderr)
                        elif resp.status == 429:
                            if not warned_429:
                                warned_429 = True
                                print(f"\033[93m[ПРЕДУПРЕЖДЕНИЕ] 429 Too Many Requests\033[0m", file=sys.stderr)
                            ra = int(resp.headers.get("Retry-After", 60))
                            await asyncio.sleep(ra)
                            await queue.put(tag)
                            queue.task_done()
                            continue
                except Exception as e:
                    if not warned_other:
                        warned_other = True
                        print(f"\033[91m[ОШИБКА] {e}\033[0m", file=sys.stderr)

                done += 1
                progress.update(task, advance=1)
                queue.task_done()

        connector = aiohttp.TCPConnector(limit=len(API_KEYS) * workers_per + 10)
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                tasks = [
                    asyncio.create_task(worker(session, key))
                    for key in API_KEYS
                    for _ in range(workers_per)
                ]
                try:
                    while not stop_event.is_set():
                        if all(t.done() for t in tasks) and queue.empty():
                            break
                        await asyncio.sleep(0.2)
                    if not stop_event.is_set():
                        await asyncio.wait_for(queue.join(), timeout=30)
                    else:
                        interrupted = True
                except (KeyboardInterrupt, asyncio.CancelledError):
                    interrupted = True
                    stop_event.set()
                except asyncio.TimeoutError:
                    pass
                finally:
                    for t in tasks:
                        t.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        except (KeyboardInterrupt, asyncio.CancelledError):
            interrupted = True

    _save_tags(found, existing, output_file)
    return found, interrupted


async def search_players(total_requests: int = 1000, output_file: Optional[str] = None):
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно выполнить поиск.")
        return
    if output_file is None:
        output_file = SEARCH_CFG.get("output_file", "The_players.txt")
    existing: set[str] = set()
    if os.path.exists(output_file):
        with open(output_file) as f:
            existing = {line.strip() for line in f if line.strip()}
        _info(f"Загружено {len(existing)} существующих тегов")
    tags = generate_tags(
        total_requests,
        SEARCH_CFG.get("tag_min_length", 7),
        SEARCH_CFG.get("tag_max_length", 9),
    )
    found, interrupted = await _run_search_engine(
        "players", tags, existing, output_file, "Поиск"
    )
    all_tags = existing | set(found)
    status = "остановлен" if interrupted else "завершён"
    _ok(f"{status}  ·  новых: {len(found)}  всего: {len(all_tags)}  →  {output_file}")
    console.print()
    await _add_rating_remote("search_players", output_file)


async def check_players_from_file(file_path: str):
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно проверить файл.")
        return
    if not os.path.exists(file_path):
        _err(f"Файл {file_path} не найден")
        return
    with open(file_path) as f:
        tags = [ln.strip() for ln in f if ln.strip()]
    if not tags:
        _err("Файл пуст")
        return
    base, ext = os.path.splitext(file_path)
    out_file = f"{base}_existing{ext}"
    existing: set[str] = set()
    if os.path.exists(out_file):
        with open(out_file) as f:
            existing = {line.strip() for line in f if line.strip()}
    _info(f"Загружено {len(tags)} тегов из {file_path}")
    found, interrupted = await _run_search_engine(
        "players", tags, existing, out_file, "Проверка"
    )
    all_tags = existing | set(found)
    status = "остановлен" if interrupted else "завершён"
    _ok(f"{status}  ·  найдено: {len(all_tags)} существующих  →  {out_file}")
    console.print()
    await _add_rating_remote("check_players_file", file_path)


async def show_random_existing_player():
    output_file = SEARCH_CFG.get("output_file", "The_players.txt")
    if not os.path.exists(output_file):
        _err(f"Файл {output_file} не найден. Сначала выполните поиск существующих игроков.")
        return
    with open(output_file) as f:
        tags = [line.strip() for line in f if line.strip()]
    if not tags:
        _err("Файл пуст. Сначала выполните поиск существующих игроков.")
        return
    tag = random.choice(tags)
    _info(f"Выбран случайный тег: {tag}")
    await show_player(tag)


async def check_team_game():
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно проверить командную игру.")
        return
    tags_input = await _ask("Введите теги игроков через запятую (без #)", "")
    if not tags_input:
        return

    raw_tags = [t.strip().upper().replace("#", "") for t in tags_input.split(",") if t.strip()]
    if len(raw_tags) < 2:
        _err("Нужно хотя бы два игрока для проверки командной игры.")
        return

    battlelogs = {}
    for tag in raw_tags:
        with console.status(f"[dim]Загрузка battlelog для {tag}...[/dim]", spinner="dots"):
            data = await api.get_battlelog(tag, force=True)
            if data and "items" in data:
                battlelogs[tag] = data["items"]
            else:
                battlelogs[tag] = []

    matches = []
    for i, (tag1, battles1) in enumerate(battlelogs.items()):
        for j, (tag2, battles2) in enumerate(battlelogs.items()):
            if i >= j:
                continue
            for b1 in battles1:
                b1_time = b1.get("battleTime", "")[:16]
                b1_mode = b1.get("event", {}).get("mode")
                b1_result = b1.get("battle", {}).get("result")
                for b2 in battles2:
                    b2_time = b2.get("battleTime", "")[:16]
                    if b1_time == b2_time:
                        teams1 = b1.get("battle", {}).get("teams", [])
                        teams2 = b2.get("battle", {}).get("teams", [])
                        players1 = set()
                        for team in teams1:
                            for player in team:
                                players1.add(player.get("tag", ""))
                        players2 = set()
                        for team in teams2:
                            for player in team:
                                players2.add(player.get("tag", ""))
                        if f"#{tag1}" in players1 and f"#{tag2}" in players2:
                            matches.append({
                                "time": b1_time,
                                "mode": b1_mode,
                                "result": b1_result,
                                "players": players1
                            })
                            break

    if not matches:
        _err("Не найдено общих боёв среди указанных игроков за последнее время.")
        return

    console.print()
    _hr("🎮 Обнаруженные командные игры")
    table = Table(box=box.MINIMAL, show_header=True,
                  header_style="dim #9ca3af", padding=(0, 2))
    table.add_column("Время", style="dim", min_width=16)
    table.add_column("Режим", min_width=24)
    table.add_column("Результат", min_width=12)
    table.add_column("Участники", style="dim", min_width=30)

    for match in matches:
        players_list = ", ".join(sorted(match["players"]))
        table.add_row(
            match["time"],
            MODE_NAMES.get(match["mode"], match["mode"]),
            "🏆 Победа" if match["result"] == "victory" else "💔 Поражение" if match["result"] else "—",
            players_list
        )
    console.print(table)
    _hr()
    console.print()
    await _add_rating_remote("check_team_game")


async def fill_database(total_requests: int = 1000):
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно заполнить базу.")
        return
    _info("Этап 1: поиск существующих игроков...")
    temp_file = "temp_found_players.txt"
    tags = generate_tags(
        total_requests,
        SEARCH_CFG.get("tag_min_length", 7),
        SEARCH_CFG.get("tag_max_length", 9),
    )
    found, interrupted = await _run_search_engine(
        "players", tags, set(), temp_file, "Поиск игроков"
    )
    if not found:
        _err("Не найдено ни одного игрока. Попробуйте увеличить количество запросов.")
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return

    _info(f"Найдено {len(found)} игроков. Этап 2: загрузка данных...")

    loaded = 0
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("[cyan]Загрузка профилей и боёв...", total=len(found))
        for tag in found:
            try:
                data = await player_col.collect(tag, force_update=True)
                if data and data.get("club_tag"):
                    club_tag_raw = data["club_tag"]
                    if isinstance(club_tag_raw, dict):
                        club_tag = club_tag_raw.get("tag", "").lstrip('#')
                    else:
                        club_tag = club_tag_raw.lstrip('#')
                    existing_club = await db.get_club(club_tag)
                    if not existing_club:
                        club_data = await api.get_club(club_tag, force=True)
                        if club_data:
                            await db.upsert_club(club_data)
                            members_data = await api.get_club_members(club_tag)
                            if members_data and "items" in members_data:
                                await db.upsert_club_members(club_tag, members_data["items"])
                loaded += 1
            except Exception as e:
                logger.error(f"Ошибка при загрузке игрока {tag}: {e}")
            progress.update(task, advance=1)

    _ok(f"Готово. Загружено {loaded} игроков из {len(found)} найденных.")
    if os.path.exists(temp_file):
        os.remove(temp_file)

    if SYNC_CFG.get("push_after_fill", False):
        await sync_push_to_github()
    console.print()
    await _add_rating_remote("fill_db")


# ═════════════════════════════════════════════════════════════════════════════
# Непрерывное заполнение базы данных
# ═════════════════════════════════════════════════════════════════════════════

async def continuous_fill():
    """Бесконечный цикл генерации тегов, проверки существования и сбора данных."""
    if not HAS_API_KEYS:
        _err("❌ Нет API ключей. Невозможно выполнить непрерывное заполнение.")
        return

    console.print()
    console.print("[bold]Непрерывное заполнение базы данных[/bold]")
    hours = await _ask("Время работы в часах (0 — бесконечно, пока не нажмёте q)", "0")
    try:
        run_hours = float(hours)
    except:
        run_hours = 0
    interval_min = await _ask_int("Интервал сохранения в GitHub (минуты)", 30)
    if interval_min <= 0:
        interval_min = 30

    start_time = time.time()
    stop_event = threading.Event()
    t_listener = threading.Thread(target=_listen_for_stop, args=(stop_event,), daemon=True)
    t_listener.start()

    # Задача периодической синхронизации с GitHub
    async def periodic_github_sync():
        while not stop_event.is_set():
            await asyncio.sleep(interval_min * 60)
            if not stop_event.is_set():
                _info("Периодическая синхронизация с GitHub...")
                try:
                    ghs = GitHubSync()
                    await ghs.export_and_push()
                    _ok("Синхронизация выполнена.")
                except Exception as e:
                    _err(f"Ошибка синхронизации: {e}")

    sync_task = asyncio.create_task(periodic_github_sync())

    # Основной цикл сбора
    processed_total = 0
    saved_players = 0
    saved_clubs = 0
    try:
        while not stop_event.is_set():
            # Генерируем пачку тегов (например, 100)
            tags = generate_tags(100, SEARCH_CFG.get("tag_min_length", 7), SEARCH_CFG.get("tag_max_length", 9))
            for tag in tags:
                if stop_event.is_set():
                    break
                # Проверяем существование игрока
                player_data = await api.get_player(tag, force=True)
                if player_data:
                    # Сохраняем игрока
                    await player_col.collect(tag, force_update=True)
                    saved_players += 1
                    await _add_rating_remote("continuous_fill", tag)
                    # Если у игрока есть клуб, сохраняем клуб
                    club = player_data.get("club")
                    if club:
                        # club может быть строкой или словарём
                        if isinstance(club, dict):
                            club_tag_clean = club.get("tag", "").lstrip('#')
                        elif isinstance(club, str):
                            club_tag_clean = club.lstrip('#')
                        else:
                            club_tag_clean = None
                        if club_tag_clean:
                            existing_club = await db.get_club(club_tag_clean)
                            if not existing_club:
                                club_data = await api.get_club(club_tag_clean, force=True)
                                if club_data:
                                    await db.upsert_club(club_data)
                                    saved_clubs += 1
                                    members_data = await api.get_club_members(club_tag_clean)
                                    if members_data and "items" in members_data:
                                        await db.upsert_club_members(club_tag_clean, members_data["items"])
                processed_total += 1
                # Небольшая задержка между запросами, чтобы не превысить лимиты
                await asyncio.sleep(0.2)

            # После каждой пачки проверяем время работы
            if run_hours > 0 and (time.time() - start_time) > run_hours * 3600:
                _info(f"Время работы истекло ({run_hours} ч). Останавливаемся.")
                break

    except asyncio.CancelledError:
        pass
    finally:
        stop_event.set()
        sync_task.cancel()
        try:
            await sync_task
        except asyncio.CancelledError:
            pass

    _ok(f"Остановлено. Всего обработано тегов: {processed_total}, сохранено игроков: {saved_players}, клубов: {saved_clubs}")
    # Финальная синхронизация
    if saved_players > 0 or saved_clubs > 0:
        _info("Финальная синхронизация с GitHub...")
        await sync_push_to_github()


async def sync_push_to_github():
    _info("Экспорт данных и отправка в GitHub...")
    try:
        ghs = GitHubSync()
        await ghs.export_and_push()
        _ok("✅ Данные успешно выгружены в GitHub")
        await _add_rating_remote("sync_push")
    except Exception as e:
        _err(f"Ошибка синхронизации: {e}")
        logger.exception("GitHub push failed")


async def sync_pull_from_github():
    _info("Загрузка данных из GitHub...")
    try:
        ghs = GitHubSync()
        await ghs.pull_and_import()
        _ok("✅ Данные успешно загружены из GitHub")
        await _add_rating_remote("sync_pull")
    except Exception as e:
        _err(f"Ошибка загрузки: {e}")
        logger.exception("GitHub pull failed")


async def set_search_mode():
    global search_mode
    current = "онлайн" if search_mode == "online" else "офлайн"
    mode = await _ask(f"Выберите режим поиска: 1 - офлайн (быстро, локальная база), 2 - онлайн (поиск в GitHub) [текущий: {current}]", "1")
    if mode == "2":
        save_search_mode("online")
        _ok("Режим поиска изменён на ОНЛАЙН (поиск в GitHub)")
    else:
        save_search_mode("offline")
        _ok("Режим поиска изменён на ОФЛАЙН (локальная база)")
    console.print()


async def show_rating():
    rating = await _get_rating_remote()
    console.print()
    _hr("⭐ Ваш рейтинг")
    _kv("Очки", str(rating), "dim", "yellow")
    _info("Рейтинг повышается за каждое полезное действие: просмотр профилей, добавление игроков/клубов, создание PNG, поиск и т.д.")
    _hr()
    console.print()


# ═════════════════════════════════════════════════════════════════════════════
# Интерактивное меню
# ═════════════════════════════════════════════════════════════════════════════

async def interactive_menu():
    console.print()
    console.print("  [bold white]Brawl Stats[/bold white]  [dim]CLI[/dim]")
    rating = await _get_rating_remote()
    console.print(f"  [dim]Ключей: {len(API_KEYS)}[/dim]  |  ⭐ Рейтинг: {rating}")
    mode_str = "онлайн" if search_mode == "online" else "офлайн"
    console.print(f"  [dim]Режим поиска: {mode_str}[/dim]")
    if not HAS_API_KEYS:
        console.print("  [yellow]⚠️ API ключи отсутствуют — некоторые функции недоступны[/yellow]")
    console.print()

    while True:
        console.print("  [dim]↑↓ Tab — навигация   Enter — выбор   q — выход[/dim]")
        console.print()

        loop = asyncio.get_event_loop()
        choice = await loop.run_in_executor(None, _run_menu)
        console.print()

        if not choice or choice == "exit":
            console.print("  [dim]До свидания![/dim]\n")
            break

        elif choice == "player":
            tag = await _ask("Тег игрока (без #, только символы 0-9, A-Z)")
            if tag:
                await show_player(tag)

        elif choice == "battles":
            tag = await _ask("Тег игрока (без #)")
            limit = await _ask_int("Количество боёв", 10)
            if tag:
                await show_battles(tag, limit)

        elif choice == "club":
            tag = await _ask("Тег клуба (без #)")
            if tag:
                await show_club(tag, show_members=False)

        elif choice == "club_members":
            tag = await _ask("Тег клуба (без #)")
            if tag:
                await show_club(tag, show_members=True)

        elif choice == "full_club":
            tag = await _ask("Тег клуба (без #)")
            if tag:
                await full_club_collect(tag)

        elif choice == "update":
            tag = await _ask("Тег игрока (без #)")
            if tag:
                await show_player(tag, force_update=True)

        elif choice == "search_name":
            await search_player_by_name()

        elif choice == "search":
            n = await _ask_int("Количество запросов", 1000)
            out = await _ask("Файл для сохранения [The_players.txt]")
            await search_players(n, out if out else None)

        elif choice == "checkfile":
            path = await _ask("Путь к файлу")
            if path:
                await check_players_from_file(path)

        elif choice == "checkfile_active":
            path = await _ask("Путь к файлу с тегами")
            if path:
                await check_active_players_from_file(path)

        elif choice == "random_player":
            await show_random_existing_player()

        elif choice == "save_png":
            tag = await _ask("Тег игрока (без #)")
            if tag:
                await save_player_stats_png(tag)

        elif choice == "gen_codes":
            await generate_command_codes()

        elif choice == "list_codes":
            await list_team_codes()

        elif choice == "cleanup_codes":
            await cleanup_expired_codes_cmd()

        elif choice == "check_club":
            tag = await _ask("Тег клуба (без #)")
            if tag:
                await check_club_by_tag(tag)

        elif choice == "search_clubs":
            n = await _ask_int("Количество запросов", 1000)
            out = await _ask("Файл для сохранения", "existing_clubs.txt")
            await search_existing_clubs(n, out if out else None)

        elif choice == "search_club_name":
            await search_club_by_name()

        elif choice == "check_team":
            await check_team_game()

        elif choice == "brawlers":
            await show_brawlers()
        elif choice == "rotation":
            await show_event_rotation()
        elif choice == "rank_players":
            region = await _ask("Регион [global]") or "global"
            await show_rankings(region, "players")
        elif choice == "rank_clubs":
            region = await _ask("Регион [global]") or "global"
            await show_rankings(region, "clubs")
        elif choice == "locations":
            await show_locations()
        elif choice == "powerplay":
            region = await _ask("Регион [global]") or "global"
            await show_powerplay_seasons(region)
        elif choice == "check_keys":
            await check_api_keys()
        elif choice == "enter_api_key":
            await input_api_key()
        elif choice == "set_mode":
            await set_search_mode()
        elif choice == "fill_db":
            n = await _ask_int("Количество запросов (чем больше, тем больше игроков будет найдено)", 1000)
            await fill_database(n)
        elif choice == "continuous_fill":
            await continuous_fill()
        elif choice == "sync_push":
            await sync_push_to_github()
        elif choice == "sync_pull":
            await sync_pull_from_github()
        elif choice == "rating":
            await show_rating()


# ═════════════════════════════════════════════════════════════════════════════
# Точка входа
# ═════════════════════════════════════════════════════════════════════════════

async def main():
    await _init()

    if SYNC_CFG.get("auto_pull_on_start", False):
        await sync_pull_from_github()

    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "player" and len(sys.argv) > 2:
            await show_player(sys.argv[2], True)
        elif cmd == "battles" and len(sys.argv) > 2:
            lim = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3].isdigit() else 10
            await show_battles(sys.argv[2], lim)
        elif cmd == "club" and len(sys.argv) > 2:
            mems = len(sys.argv) > 3 and sys.argv[3].lower() == "members"
            await show_club(sys.argv[2], mems)
        elif cmd == "fullclub" and len(sys.argv) > 2:
            await full_club_collect(sys.argv[2])
        elif cmd == "search_name":
            await search_player_by_name()
        elif cmd == "search":
            n = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 1000
            await search_players(n)
        elif cmd == "checkfile" and len(sys.argv) > 2:
            await check_players_from_file(sys.argv[2])
        elif cmd == "checkfile_active" and len(sys.argv) > 2:
            await check_active_players_from_file(sys.argv[2])
        elif cmd == "randomplayer":
            await show_random_existing_player()
        elif cmd == "gen_codes":
            await generate_command_codes()
        elif cmd == "list_codes":
            await list_team_codes()
        elif cmd == "cleanup_codes":
            await cleanup_expired_codes_cmd()
        elif cmd == "check_club" and len(sys.argv) > 2:
            await check_club_by_tag(sys.argv[2])
        elif cmd == "search_clubs":
            n = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 1000
            out = sys.argv[3] if len(sys.argv) > 3 else None
            await search_existing_clubs(n, out)
        elif cmd == "search_club_name":
            await search_club_by_name()
        elif cmd == "check_team":
            await check_team_game()
        elif cmd == "brawlers":
            await show_brawlers()
        elif cmd == "rotation":
            await show_event_rotation()
        elif cmd == "rankplayers" and len(sys.argv) > 2:
            await show_rankings(sys.argv[2], "players")
        elif cmd == "rankclubs" and len(sys.argv) > 2:
            await show_rankings(sys.argv[2], "clubs")
        elif cmd == "locations":
            await show_locations()
        elif cmd == "powerplay" and len(sys.argv) > 2:
            await show_powerplay_seasons(sys.argv[2])
        elif cmd == "checkkeys":
            await check_api_keys()
        elif cmd == "enter_api_key":
            await input_api_key()
        elif cmd == "set_mode":
            await set_search_mode()
        elif cmd == "fill_db":
            n = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 1000
            await fill_database(n)
        elif cmd == "continuous_fill":
            await continuous_fill()
        elif cmd == "syncpush":
            await sync_push_to_github()
        elif cmd == "syncpull":
            await sync_pull_from_github()
        elif cmd == "rating":
            await show_rating()
        else:
            console.print(
                "  Использование: python cli.py "
                "[player|battles|club|fullclub|search|checkfile|checkfile_active|randomplayer|gen_codes"
                "|list_codes|cleanup_codes|check_club|search_clubs|search_club_name|check_team|brawlers|rotation|rankplayers|rankclubs|locations|powerplay|checkkeys|enter_api_key|set_mode|fill_db|continuous_fill|syncpush|syncpull|rating] [аргументы]\n"
                "  Без аргументов — интерактивное меню"
            )
    else:
        await interactive_menu()

    if SYNC_CFG.get("auto_push_on_exit", False):
        await sync_push_to_github()

    try:
        await api.close()
        await db.close()
    except Exception:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        console.print("\n  [dim]Прервано — данные сохранены[/dim]\n")
        sys.exit(0)