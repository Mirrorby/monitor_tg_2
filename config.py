"""
Конфигурация: переменные окружения, логирование, глобальное состояние, метрики.
"""
import logging
import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor

# ── Переменные окружения ───────────────────────────────────────────────────────

API_ID_1   = int(os.environ.get('TG_API_ID_1', '0'))
API_HASH_1 = os.environ.get('TG_API_HASH_1', '')
SESSION_1  = os.environ.get('TG_SESSION_1', '')

API_ID_2   = int(os.environ.get('TG_API_ID_2', '0'))
API_HASH_2 = os.environ.get('TG_API_HASH_2', '')
SESSION_2  = os.environ.get('TG_SESSION_2', '')

SPREADSHEET_ID         = os.environ.get('SPREADSHEET_ID', '')
GOOGLE_CREDENTIALS_B64 = os.environ.get('GOOGLE_CREDENTIALS_BASE64', '')
SETTINGS_RELOAD_SEC    = int(os.environ.get('SETTINGS_RELOAD_INTERVAL', '300'))
EXECUTOR_WORKERS       = int(os.environ.get('EXECUTOR_WORKERS', '8'))
QUEUE_WORKERS          = int(os.environ.get('QUEUE_WORKERS', '3'))
GEMINI_API_KEY         = os.environ.get('GEMINI_API_KEY', '')

# Задержка между сообщениями при рассылке в бот (секунды)
# 0.05 = 20 сообщений/сек — безопасно при лимите TG 30/сек
BOT_BROADCAST_DELAY = float(os.environ.get('BOT_BROADCAST_DELAY', '0.05'))

TZ_OFFSET_HOURS = 3

# ── Логирование ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# ── Глобальное состояние ───────────────────────────────────────────────────────

state = {
    'tg_token':             '',
    'score_threshold':      7,
    'moderation_threshold': 4,
    'min_length':           20,
    'moderator_chat_id':    '',
    'dest_chat_id':         '',       # канал для частных (пусто = только бот)
    'dest_chat_id_agent':   '',       # канал для агентов (пусто = только бот)
    'scoring_rules':        [],
    'minus_words':          [],
    'watched_ids':          set(),
    'id_to_meta':           {},
    'username_to_meta':     {},
    'excluded_accounts':    set(),
    'realtors':             set(),
    # Подписчики бота: set[int] — chat_id пользователей
    'bot_subscribers':      {},
    'channel_city_map':     {},       # dict[str, str] — {username: city}
}

# ── Очереди дедупликации ───────────────────────────────────────────────────────

seen_ids: deque = deque(maxlen=2000)
published_fingerprints: deque = deque(maxlen=10000)
ai_rejected_fingerprints: deque = deque(maxlen=10000)

# ── Очередь ожидающих модерации ────────────────────────────────────────────────

pending_moderation: dict = {}

# ── Пул потоков и блокировки (инициализируются в main) ─────────────────────────

_executor    = ThreadPoolExecutor(max_workers=EXECUTOR_WORKERS)
_sheets_lock = None   # asyncio.Lock — создаётся в main()
_state_lock  = None   # asyncio.Lock — создаётся в main()

# ── Метрики ────────────────────────────────────────────────────────────────────

metrics = {
    'processed':   0,
    'published':   0,
    'moderated':   0,
    'errors':      0,
    'bot_sent':    0,
    'bot_blocked': 0,
}
