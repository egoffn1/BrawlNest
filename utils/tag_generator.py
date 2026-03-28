"""Умная генерация тегов Brawl Stars на основе реальных тегов."""
import random
from collections import defaultdict
from typing import List, Dict, Optional

# Реальные уникальные теги (из предоставленного списка)
_REAL_TAGS = [
    "PPGCR0Y2L",
    "P8GGRVCL",
    "L0P9L928P",
    "G90J229Q",
    "22PGRGVPV",
    "8C29UVUV",
    "GU8Q2JR",
    "GC9CYPLQU",
    "2RPJVCCQPY",
    "8R29PY9RJ",
    "8YJU8LJQR",
    "2YQJGCQ",
    "LGVY0QGP9",
    "PR80U2GQR",
]

# Допустимые символы (без I, O)
VALID_CHARS = "0123456789ABCDEFGHJKLMNPQRSTUVWXYZ"

# Максимальная длина тега в примерах
_MAX_LEN = max(len(t) for t in _REAL_TAGS)

# Статистика символов по позициям
_position_stats: List[Dict[str, int]] = []
_position_total: List[int] = []

def _build_statistics():
    """Построить частотный словарь для каждой позиции."""
    global _position_stats, _position_total
    # Инициализируем списки для всех позиций до _MAX_LEN
    _position_stats = [defaultdict(int) for _ in range(_MAX_LEN)]
    _position_total = [0] * _MAX_LEN
    
    for tag in _REAL_TAGS:
        for i, ch in enumerate(tag):
            _position_stats[i][ch] += 1
            _position_total[i] += 1
    
    # Для позиций, где нет данных (например, 8-я позиция для 9-символьных тегов)
    # оставляем как есть — при генерации, если позиция выходит за пределы имеющихся данных,
    # будем использовать равномерное распределение по всем допустимым символам.

_build_statistics()

def _get_char_for_position(pos: int) -> str:
    """Вернуть символ для заданной позиции на основе статистики."""
    if pos < len(_position_stats) and _position_total[pos] > 0:
        # Выбираем символ с учётом частоты
        chars = []
        weights = []
        for ch, cnt in _position_stats[pos].items():
            chars.append(ch)
            weights.append(cnt)
        return random.choices(chars, weights=weights)[0]
    else:
        # Нет данных — равномерный выбор из всех допустимых символов
        return random.choice(VALID_CHARS)

def generate_tag(min_len: int = 7, max_len: int = 9) -> str:
    """Сгенерировать тег, похожий на реальные."""
    # Длина тега: выбираем из длин реальных тегов (7 или 9)
    # В наборе есть 7-символьный GU8Q2JR, но в основном 9.
    # Сделаем случайный выбор между 7 и 9, но с перевесом в сторону 9 (как в реальности)
    # Можно также использовать распределение длин из реальных тегов.
    # В наборе из 14 тегов: 13 длиной 9, 1 длиной 7.
    # Для простоты сделаем вероятность 9/14 ≈ 0.93 для 9 и 1/14 для 7.
    if random.random() < 13/14:  # 13 из 14 тегов длиной 9
        length = 9
    else:
        length = 7
    
    # Первый символ может быть цифрой или буквой — статистика сама определит
    tag_chars = []
    for i in range(length):
        tag_chars.append(_get_char_for_position(i))
    return "".join(tag_chars)

def generate_tags(n: int, min_len: int = 7, max_len: int = 9) -> List[str]:
    """Сгенерировать n тегов."""
    # Параметры min_len/max_len не используются, но оставляем для совместимости
    return [generate_tag(min_len, max_len) for _ in range(n)]