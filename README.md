# BrawlNest v2

Распределённый API + CLI для статистики Brawl Stars.

## Быстрый старт — CLI

```bash
pip install -r requirements.txt
cp .env .env.local   # заполните токены
python cli.py        # интерактивное меню
```

При первом запуске CLI автоматически получит API ключ от BrawlNest сервера.

## Быстрый старт — REST API сервер

```bash
docker-compose up -d --build
# API: http://localhost:80/docs
```

## Переменные окружения (.env)

| Переменная | Описание |
|---|---|
| `API_SERVER_URL` | URL BrawlNest REST API (по умолч. http://130.12.46.224) |
| `API_KEYS` | Ключи Brawl Stars (developer.brawlstars.com), через запятую |
| `GITHUB_TOKEN` | GitHub токен для записи в brawl_data |
| `ADMIN_SECRET` | Секрет для /admin/* |
| `POSTGRES_DSN` | PostgreSQL для REST API сервера |
| `REDIS_URL` | Redis URL |

## Команды CLI

```bash
python cli.py                    # интерактивное меню
python cli.py player 8UG9C0L    # профиль игрока
python cli.py battles 8UG9C0L   # бои игрока
python cli.py club 2YCCU        # клуб
python cli.py maps               # статистика карт
python cli.py rankings           # топ игроков
python cli.py nodes              # активные узлы сети
python cli.py status             # статус API
python cli.py rating             # мой рейтинг
python cli.py gen_code           # создать код команды
python cli.py fill               # заполнить базу
python cli.py syncpush           # синхронизация с GitHub
python main.py api               # запустить REST API сервер
```

## Функции CLI (все пункты меню)

### 👤 Игроки
- Профиль игрока (из BrawlNest API или напрямую из Brawl Stars)
- История трофеев
- Последние бои с детализацией
- Статистика боёв по режимам и картам
- Список бравлеров и мастерство
- Сравнение до 3 игроков
- Сохранение статистики в PNG (требует matplotlib)

### 🏢 Клубы
- Полная информация с участниками
- История трофеев и состава
- Полный сбор данных всех участников клуба

### 🔍 Поиск
- Поиск игроков/клубов по имени (через BrawlNest API с Redis-индексом)
- Расширенный поиск с сортировкой
- Поиск существующих игроков/клубов по API
- Проверка тегов из файла
- Командная статистика по тегам

### 🗺️ Карты и рейтинги
- Статистика карт из GitHub
- Рейтинги игроков и клубов (BrawlNest + глобальный)
- Рейтинг по конкретному бравлеру

### 🔑 Коды команд
- Генерация кодов (сохраняются в GitHub + Redis)
- Проверка активности кода

### ⭐ Рейтинговая система
- Просмотр своего рейтинга
- Таблица лидеров
- Автоматическое начисление очков за действия

### 🌐 Сеть
- Список активных узлов с пингами
- Статус API ключей

### ⚙️ Данные
- Заполнение базы через поиск тегов
- Непрерывное заполнение
- Синхронизация с GitHub (pull/push)
