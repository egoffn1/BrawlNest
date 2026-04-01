"""
GitHub клиент: чтение и запись данных в ветку brawl_data.
Поддерживает: get, put (upsert), delete, list, bulk_fetch.
"""
import asyncio
import base64
import json
import aiohttp
from typing import List, Dict, Optional, Any
from config import GITHUB_REPO, GITHUB_BRANCH, GITHUB_TOKEN

_CONCURRENCY = 10


class GitHubClient:
    def __init__(self):
        self.base_url = "https://api.github.com/repos"
        self.repo     = GITHUB_REPO
        self.branch   = GITHUB_BRANCH
        self.token    = GITHUB_TOKEN

        self._headers: Dict[str, str] = {"Accept": "application/vnd.github.v3+json"}
        if self.token:
            self._headers["Authorization"] = f"token {self.token}"

        self._session: Optional[aiohttp.ClientSession] = None
        self._sem = asyncio.Semaphore(_CONCURRENCY)

    async def start(self):
        connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
        timeout   = aiohttp.ClientTimeout(total=20)
        self._session = aiohttp.ClientSession(
            headers=self._headers, connector=connector, timeout=timeout
        )

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ─── Internal helpers ────────────────────────────────────────────────────

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{self.repo}/contents/{path}?ref={self.branch}"

    async def _get(self, url: str) -> Any:
        async with self._sem:
            async with self._session.get(url) as resp:
                if resp.status == 404:
                    return None
                if resp.status != 200:
                    raise Exception(f"GitHub GET error {resp.status}: {url}")
                return await resp.json()

    async def _get_raw(self, url: str) -> Any:
        async with self._sem:
            async with self._session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"GitHub raw error {resp.status}: {url}")
                return await resp.json(content_type=None)

    # ─── Public read ─────────────────────────────────────────────────────────

    async def get_file(self, path: str) -> Optional[Dict]:
        """Получить JSON-файл из репозитория. None если не существует."""
        url   = f"{self.base_url}/{self.repo}/contents/{path}?ref={self.branch}"
        meta  = await self._get(url)
        if meta is None:
            return None
        download = meta.get("download_url")
        if not download:
            return None
        return await self._get_raw(download)

    async def get_file_meta(self, path: str) -> Optional[Dict]:
        """Получить метаданные файла (включая sha). None если не существует."""
        url = f"{self.base_url}/{self.repo}/contents/{path}?ref={self.branch}"
        return await self._get(url)

    async def list_dir(self, path: str) -> List[Dict]:
        """Список файлов в директории."""
        url   = f"{self.base_url}/{self.repo}/contents/{path}?ref={self.branch}"
        items = await self._get(url)
        if items is None:
            return []
        if isinstance(items, list):
            return items
        return []

    # ─── Public write ────────────────────────────────────────────────────────

    async def upsert_file(self, path: str, data: Any, message: str = "update") -> bool:
        """Создать или обновить JSON-файл в репозитории."""
        if not self.token:
            return False
        content = base64.b64encode(json.dumps(data, ensure_ascii=False).encode()).decode()
        # Получить sha для обновления
        meta = await self.get_file_meta(path)
        sha  = meta.get("sha") if meta else None

        payload: Dict[str, Any] = {
            "message": message,
            "content": content,
            "branch": self.branch,
        }
        if sha:
            payload["sha"] = sha

        url = f"{self.base_url}/{self.repo}/contents/{path}"
        async with self._sem:
            async with self._session.put(url, json=payload) as resp:
                return resp.status in (200, 201)

    async def delete_file(self, path: str, message: str = "delete") -> bool:
        """Удалить файл из репозитория."""
        if not self.token:
            return False
        meta = await self.get_file_meta(path)
        if meta is None:
            return True  # already gone
        sha = meta.get("sha")
        if not sha:
            return False

        payload = {"message": message, "sha": sha, "branch": self.branch}
        url = f"{self.base_url}/{self.repo}/contents/{path}"
        async with self._sem:
            async with self._session.delete(url, json=payload) as resp:
                return resp.status == 200

    # ─── Shortcuts ───────────────────────────────────────────────────────────

    async def list_players(self) -> List[Dict]:
        items = await self.list_dir("brawl_data/players")
        return [
            {"tag": i["name"].replace(".json", ""), "download_url": i.get("download_url")}
            for i in items if i.get("type") == "file" and i["name"].endswith(".json")
        ]

    async def list_clubs(self) -> List[Dict]:
        items = await self.list_dir("brawl_data/clubs")
        return [
            {"tag": i["name"].replace(".json", ""), "download_url": i.get("download_url")}
            for i in items if i.get("type") == "file" and i["name"].endswith(".json")
        ]

    async def get_player(self, tag: str) -> Optional[Dict]:
        tag = tag.upper().lstrip("#")
        return await self.get_file(f"brawl_data/players/{tag}.json")

    async def upsert_player(self, tag: str, data: Dict) -> bool:
        tag = tag.upper().lstrip("#")
        return await self.upsert_file(f"brawl_data/players/{tag}.json", data, f"upsert player {tag}")

    async def get_club(self, tag: str) -> Optional[Dict]:
        tag = tag.upper().lstrip("#")
        return await self.get_file(f"brawl_data/clubs/{tag}.json")

    async def upsert_club(self, tag: str, data: Dict) -> bool:
        tag = tag.upper().lstrip("#")
        return await self.upsert_file(f"brawl_data/clubs/{tag}.json", data, f"upsert club {tag}")

    async def get_trophy_history(self, tag: str) -> Optional[List]:
        tag = tag.upper().lstrip("#")
        return await self.get_file(f"brawl_data/trophy_history/{tag}.json")

    async def upsert_trophy_history(self, tag: str, data: List) -> bool:
        tag = tag.upper().lstrip("#")
        return await self.upsert_file(f"brawl_data/trophy_history/{tag}.json", data, f"trophy history {tag}")

    async def get_club_history(self, tag: str) -> Optional[List]:
        tag = tag.upper().lstrip("#")
        return await self.get_file(f"brawl_data/club_history/{tag}.json")

    async def upsert_club_history(self, tag: str, data: List) -> bool:
        tag = tag.upper().lstrip("#")
        return await self.upsert_file(f"brawl_data/club_history/{tag}.json", data, f"club history {tag}")

    async def get_battles(self, tag: str) -> Optional[List]:
        tag = tag.upper().lstrip("#")
        return await self.get_file(f"brawl_data/battles/{tag}.json")

    async def upsert_battles(self, tag: str, data: List) -> bool:
        tag = tag.upper().lstrip("#")
        return await self.upsert_file(f"brawl_data/battles/{tag}.json", data, f"battles {tag}")

    async def get_map_stats(self) -> Optional[List]:
        return await self.get_file("brawl_data/map_stats.json")

    async def upsert_map_stats(self, data: List) -> bool:
        return await self.upsert_file("brawl_data/map_stats.json", data, "update map_stats")

    async def get_team_stats(self, team_hash: str) -> Optional[Dict]:
        return await self.get_file(f"brawl_data/team_stats/{team_hash}.json")

    async def upsert_team_stats(self, team_hash: str, data: Dict) -> bool:
        return await self.upsert_file(f"brawl_data/team_stats/{team_hash}.json", data, f"team_stats {team_hash}")

    async def get_rankings_players(self, date_str: str) -> Optional[Dict]:
        return await self.get_file(f"brawl_data/rankings/players/{date_str}.json")

    async def upsert_rankings_players(self, date_str: str, data: Dict) -> bool:
        return await self.upsert_file(f"brawl_data/rankings/players/{date_str}.json", data, f"rankings players {date_str}")

    async def get_rankings_clubs(self, date_str: str) -> Optional[Dict]:
        return await self.get_file(f"brawl_data/rankings/clubs/{date_str}.json")

    async def upsert_rankings_clubs(self, date_str: str, data: Dict) -> bool:
        return await self.upsert_file(f"brawl_data/rankings/clubs/{date_str}.json", data, f"rankings clubs {date_str}")

    # ─── Team codes ──────────────────────────────────────────────────────────

    async def list_team_codes(self) -> List[Dict]:
        items = await self.list_dir("brawl_data/team_codes")
        return [
            {"code": i["name"].replace(".json", ""), "download_url": i.get("download_url")}
            for i in items if i.get("type") == "file" and i["name"].endswith(".json")
        ]

    async def get_team_code(self, code: str) -> Optional[Dict]:
        return await self.get_file(f"brawl_data/team_codes/{code}.json")

    async def upsert_team_code(self, code: str, data: Dict) -> bool:
        return await self.upsert_file(f"brawl_data/team_codes/{code}.json", data, f"team code {code}")

    async def delete_team_code(self, code: str) -> bool:
        return await self.delete_file(f"brawl_data/team_codes/{code}.json", f"expire team code {code}")

    # ─── Bulk fetch ──────────────────────────────────────────────────────────

    async def bulk_fetch_players(self, items: List[Dict], *, max_items: Optional[int] = None) -> List[Dict]:
        if max_items:
            items = items[:max_items]
        async def _fetch(item):
            try:
                return await self._get_raw(item["download_url"])
            except Exception:
                return None
        results = await asyncio.gather(*[_fetch(i) for i in items])
        return [r for r in results if r]

    async def bulk_fetch_clubs(self, items: List[Dict], *, max_items: Optional[int] = None) -> List[Dict]:
        if max_items:
            items = items[:max_items]
        async def _fetch(item):
            try:
                return await self._get_raw(item["download_url"])
            except Exception:
                return None
        results = await asyncio.gather(*[_fetch(i) for i in items])
        return [r for r in results if r]

    async def build_name_index(self) -> Dict[str, Dict]:
        player_list = await self.list_players()
        all_data    = await self.bulk_fetch_players(player_list)
        return {d.get("tag","").upper(): {"tag": d.get("tag","").upper(), "name": d.get("name","")}
                for d in all_data if d.get("tag")}

    async def build_club_name_index(self) -> Dict[str, Dict]:
        club_list = await self.list_clubs()
        all_data  = await self.bulk_fetch_clubs(club_list)
        return {d.get("tag","").upper(): {"tag": d.get("tag","").upper(), "name": d.get("name","")}
                for d in all_data if d.get("tag")}
