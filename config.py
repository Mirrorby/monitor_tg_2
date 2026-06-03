"""
Конфигурация: переменные окружения, логирование, глобальное состояние, метрики.
"""
import asyncio
import logging
import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor

# ── Переменные окружения ───────────────────────────────────────────────────────
API_ID_1     = int(os.environ.get('TG_API_ID_1', '0'))
API_HASH_1   = os.environ.get('TG_API_HASH_1', '')
SESSION_1    = os.environ.get('TG_SESSION_1', '')

API_ID_2     = int(os.environ.get('TG_API_ID_2', '0'))
API_HASH_2   = os.environ.get('TG_API_HASH_2', '')
SESSION_2    = os.environ.get('TG_SESSION_2', '')

SPREADSHEET_ID          = os.environ.get('SPREADSHEET_ID', '')
GOOGLE_CREDENTIALS_B64  = os.environ.get('GOOGLE_CREDENTIALS_BASE64', '')
SETTINGS_RELOAD_SEC     = int(os.environ.get('SETTINGS_RELOAD_INTERVAL', '300'))
EXECUTOR_WORKERS        = int(os.environ.get('EXECUTOR_WORKERS', '8'))
GEMINI_API_KEY          = os.environ.get('GEMINI_API_KEY', '')

BOT_BROADCAST_DELAY = float(os.environ.get('BOT_BROADCAST_DELAY', '0.05'))
TZ_OFFSET_HOURS = 3

# Количество параллельных воркеров обработки постов
PROCESS_WORKERS = int(os.environ.get('PROCESS_WORKERS', '8'))

# Порог очереди для алерта модератору
QUEUE_ALERT_THRESHOLD = int(os.environ.get('QUEUE_ALERT_THRESHOLD', '100'))

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
    'dest_chat_id':         '',
    'dest_chat_id_agent':   '',
    'scoring_rules':        [],
    'minus_words':          [],
    'watched_ids':          set(),
    'id_to_meta':           {},
    'username_to_meta':     {},
    'excluded_accounts':    set(),
    'realtors':             set(),
    'bot_subscribers':      {},
    'channel_city_map':     {},
}

# ── Очереди дедупликации ───────────────────────────────────────────────────────
seen_ids:                deque = deque(maxlen=2000)
published_fingerprints:  deque = deque(maxlen=10000)
ai_rejected_fingerprints: deque = deque(maxlen=10000)

# ── Очередь ожидающих модерации ────────────────────────────────────────────────
pending_moderation: dict = {}

# ── Очередь постов для воркеров (инициализируется в main) ──────────────────────
post_queue: asyncio.Queue = None

# Флаг: алерт о переполнении уже отправлен (чтобы не спамить)
queue_alert_sent: bool = False

# ── Пул потоков и блокировки (инициализируются в main) ─────────────────────────
_executor      = ThreadPoolExecutor(max_workers=EXECUTOR_WORKERS)
_sheets_lock   = None   # asyncio.Lock
_state_lock    = None   # asyncio.Lock

# ── Метрики ────────────────────────────────────────────────────────────────────
metrics = {
    'processed':   0,
    'published':   0,
    'moderated':   0,
    'errors':      0,
    'bot_sent':    0,
    'bot_blocked': 0,
}
