import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
_cfg_path = BASE_DIR / "config.yaml"

with open(_cfg_path, "r", encoding="utf-8") as _f:
    _yaml = yaml.safe_load(_f)

APP_CFG    = _yaml.get("app", {})
API_CFG    = _yaml.get("api", {})
SEARCH_CFG = _yaml.get("search", {})
BOT_CFG    = _yaml.get("bot", {})
SERVER_CFG = _yaml.get("api_server", {})
SYNC_CFG   = _yaml.get("sync", {})

# Режим поиска по умолчанию (можно переопределить через пункт меню)
SEARCH_MODE = APP_CFG.get("search_mode", "offline")

_raw = os.getenv("API_KEYS", "") or os.getenv("API_KEY", "")
API_KEYS = [k.strip() for k in _raw.split(",") if k.strip()]

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")

_proxy_raw = os.getenv("PROXY", "")
PROXY_LIST = [p.strip() for p in _proxy_raw.split(",") if p.strip()]

# GitHub настройки
GITHUB_REPO_URL = os.getenv("GITHUB_REPO_URL", "")
GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN", "")

DB_PATH = APP_CFG.get("db_path", "brawl_stats.db")

if not API_KEYS:
    raise ValueError("Нет API-ключей. Укажи API_KEYS или API_KEY в .env")