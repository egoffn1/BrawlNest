import logging
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from config import APP_CFG


class _JsonFormatter(logging.Formatter):
    """Форматирует лог-записи как однострочный JSON."""

    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, ensure_ascii=False)


def setup_logger(name: str = "brawl") -> logging.Logger:
    """Возвращает логгер: в файл — JSON, в консоль — только WARNING+."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level_str = APP_CFG.get("log_level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    logger.setLevel(level)

    # ── Файл (JSON) ──────────────────────────────────────────────────────────
    log_path = Path(APP_CFG.get("log_file", "logs/app.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(_JsonFormatter())
    logger.addHandler(fh)

    # ── Консоль (plain, WARNING+) ─────────────────────────────────────────────
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.WARNING)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(ch)

    logger.propagate = False
    return logger
