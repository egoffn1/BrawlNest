import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
_cfg_path = BASE_DIR / "config.yaml"

if _cfg_path.exists():
    with open(_cfg_path, "r", encoding="utf-8") as _f:
        _yaml = yaml.safe_load(_f)
else:
    _yaml = {}

APP_CFG    = _yaml.get("app", {})
API_CFG    = _yaml.get("api", {})
SEARCH_CFG = _yaml.get("search", {})
BOT_CFG    = _yaml.get("bot", {})
SERVER_CFG = _yaml.get("api_server", {})
SYNC_CFG   = _yaml.get("sync", {})

SEARCH_MODE = APP_CFG.get("search_mode", "offline")

_raw = os.getenv("API_KEYS", "") or os.getenv("API_KEY", "")
API_KEYS = [k.strip() for k in _raw.split(",") if k.strip()]

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")

_proxy_raw = os.getenv("PROXY", "")
PROXY_LIST = [p.strip() for p in _proxy_raw.split(",") if p.strip()]

GITHUB_REPO_URL = os.getenv("GITHUB_REPO_URL", "https://github.com/egoffn1/BrawlNest")
GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO     = os.getenv("GITHUB_REPO", "egoffn1/BrawlNest")
GITHUB_BRANCH   = os.getenv("GITHUB_BRANCH", "brawl_data")

DB_PATH = APP_CFG.get("db_path", "brawl_stats.db")

# BrawlNest REST API
API_SERVER_URL = os.getenv("API_SERVER_URL", "http://130.12.46.224")
BRAWLNEST_API_KEY = os.getenv("API_KEY", "")      # ключ для BrawlNest REST API
NODE_ADDRESS = os.getenv("NODE_ADDRESS", "http://localhost:80")
NODE_SECRET  = os.getenv("NODE_SECRET", "node_secret")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "change_me")
