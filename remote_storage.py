import os
import json
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

class RemoteStorage:
    def __init__(self, repo_url="https://github.com/egoffn1/BrawlNest.git", branch="brawl_data"):
        self.repo_url = repo_url
        self.branch = branch
        self.local_path = Path(".brawlnest_data")
        # Все данные хранятся в brawl_data/brawl_data/ согласно структуре репозитория
        self.data_path = self.local_path / "brawl_data"
        
    def ensure_repo(self):
        """Клонирует или обновляет репозиторий"""
        if not self.local_path.exists():
            print(f"📥 Клонирование репозитория {self.repo_url} (ветка {self.branch})...")
            try:
                subprocess.run(
                    ["git", "clone", "-b", self.branch, self.repo_url, str(self.local_path)],
                    check=True,
                    capture_output=True
                )
            except subprocess.CalledProcessError as e:
                # Если ветка не найдена при клонировании, клонируем главную и переключаемся
                if b"not found" in e.stderr or b"didn't match any file" in e.stderr:
                    print("⚠️ Ветка не найдена при клонировании, пытаемся создать локально...")
                    subprocess.run(["git", "clone", self.repo_url, str(self.local_path)], check=True, capture_output=True)
                    os.chdir(self.local_path)
                    subprocess.run(["git", "checkout", "-b", self.branch], check=True, capture_output=True)
                    os.chdir("..")
                else:
                    raise e
        else:
            print("🔄 Обновление локальной копии репозитория...")
            os.chdir(self.local_path)
            try:
                subprocess.run(["git", "pull", "origin", self.branch], check=True, capture_output=True)
            except subprocess.CalledProcessError:
                # Игнорируем ошибки pull, если нет интернета или конфликтов, работаем локально
                pass
            finally:
                os.chdir("..")
        
        # Создаем структуру папок если нет (согласно структуре репозитория BrawlNest)
        self.data_path.mkdir(parents=True, exist_ok=True)
        (self.data_path / "players").mkdir(exist_ok=True)
        (self.data_path / "battles").mkdir(exist_ok=True)
        (self.data_path / "codes").mkdir(exist_ok=True)
        (self.data_path / "clubs").mkdir(exist_ok=True)
        (self.data_path / "club_history").mkdir(exist_ok=True)
        (self.data_path / "player_brawler_stats").mkdir(exist_ok=True)
        (self.data_path / "rankings").mkdir(exist_ok=True)
        (self.data_path / "team_stats").mkdir(exist_ok=True)
        (self.data_path / "trophy_history").mkdir(exist_ok=True)

    def _get_file_path(self, category, identifier):
        """Генерирует путь к файлу на основе категории и идентификатора"""
        # Очищаем идентификатор от недопустимых символов для имени файла
        safe_id = "".join(c for c in str(identifier) if c.isalnum() or c in '-_')
        return self.data_path / category / f"{safe_id}.json"

    def read_data(self, category, identifier):
        """Читает данные из репозитория"""
        self.ensure_repo()
        file_path = self._get_file_path(category, identifier)
        
        if not file_path.exists():
            return None
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"⚠️ Ошибка чтения JSON: {file_path}")
            return None

    def write_data(self, category, identifier, data):
        """Записывает данные в репозиторий и делает коммит"""
        self.ensure_repo()
        file_path = self._get_file_path(category, identifier)
        
        # Добавляем метаданные
        if isinstance(data, dict):
            data['_last_updated'] = datetime.utcnow().isoformat()
            data['_source'] = 'cli_tool'

        os.makedirs(file_path.parent, exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Данные сохранены: {file_path.relative_to(self.local_path)}")
        return True

    def commit_changes(self, message="Auto-update from CLI"):
        """Коммитит и пушит изменения в удаленный репозиторий"""
        if not self.local_path.exists():
            print("❌ Репозиторий не найден. Сначала выполните синхронизацию.")
            return False

        os.chdir(self.local_path)
        try:
            # Добавляем все изменения
            subprocess.run(["git", "add", "."], check=True, capture_output=True)
            
            # Проверяем, есть ли изменения для коммита
            result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
            if not result.stdout.strip():
                print("✅ Изменений нет.")
                return True

            # Коммит
            subprocess.run(["git", "commit", "-m", f"{message} ({datetime.now().strftime('%Y-%m-%d %H:%M')})"], 
                          check=True, capture_output=True)
            
            # Пуш (может потребовать аутентификации, если репозиторий приватный или настройки такие)
            print("🚀 Отправка изменений в удаленный репозиторий...")
            subprocess.run(["git", "push", "origin", self.branch], check=True, capture_output=True)
            
            print("✅ Данные успешно отправлены в BrawlNest (brawl_data).")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Ошибка при отправке данных: {e.stderr.decode()}")
            # Если ошибка авторизации, подсказываем пользователю
            if "Authentication failed" in e.stderr.decode() or "403" in e.stderr.decode():
                print("⚠️ Возможно, требуется настройка токена GitHub для записи.")
            return False
        finally:
            os.chdir("..")

    def list_files(self, category):
        """Список всех файлов в категории"""
        self.ensure_repo()
        dir_path = self.data_path / category
        if not dir_path.exists():
            return []
        
        files = [f.stem for f in dir_path.glob("*.json")]
        return files

    def get_all_data(self, category):
        """Получает все данные из категории"""
        files = self.list_files(category)
        data = {}
        for f in files:
            content = self.read_data(category, f)
            if content:
                data[f] = content
        return data

# Глобальный экземпляр
storage = RemoteStorage()
