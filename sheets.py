"""
Работа с Google Sheets: подключение, чтение и запись данных.
"""
import asyncio
import base64
import json
import re
import time
from datetime import datetime, timezone, timedelta

import gspread
from google.oauth2.service_account import Credentials
from telethon.errors import FloodWaitError, UsernameInvalidError, UsernameNotOccupiedError

from config import (
    GOOGLE_CREDENTIALS_B64, SPREADSHEET_ID, TZ_OFFSET_HOURS,
    state, _executor, log,
)
from utils import _extract_username


# ══════════════════════════════════════════════════════════════════════════════
# Подключение
# ══════════════════════════════════════════════════════════════════════════════

def _get_spreadsheet():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_B64).decode('utf-8'))
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


# ══════════════════════════════════════════════════════════════════════════════
# Retry-обёртка и async-хелперы
# ══════════════════════════════════════════════════════════════════════════════

def _write_with_retry(fn, *args, max_attempts: int = 3):
    for attempt in range(max_attempts):
        try:
            fn(*args)
            return
        except gspread.exceptions.APIError as e:
            code = getattr(e.response, 'status_code', 0)
            if code in (429, 500):
                wait = (2 ** attempt) * 5
                log.warning(f'Sheets {code} — retry через {wait}s (попытка {attempt + 1})')
                time.sleep(wait)
            else:
                log.error(f'Sheets APIError: {e}', exc_info=True)
                return
        except Exception as e:
            log.error(f'Sheets write failed: {e}', exc_info=True)
            return
    log.error(f'Sheets write не удалась после {max_attempts} попыток')


async def _safe_sheets(fn, *args):
    from config import _sheets_lock
    loop = asyncio.get_event_loop()
    async with _sheets_lock:
        await loop.run_in_executor(_executor, fn, *args)


async def _safe_sheets_retry(fn, *args):
    from config import _sheets_lock
    loop = asyncio.get_event_loop()
    async with _sheets_lock:
        await loop.run_in_executor(_executor, _write_with_retry, fn, *args)


async def _safe_sheets_result(fn, *args):
    from config import _sheets_lock
    loop = asyncio.get_event_loop()
    async with _sheets_lock:
        return await loop.run_in_executor(_executor, fn, *args)


# ══════════════════════════════════════════════════════════════════════════════
# Чтение
# ══════════════════════════════════════════════════════════════════════════════

def _read_settings(ss):
    try:
        data = ss.worksheet('Настройки').get_all_values()
        def val(row_idx):
            return str(data[row_idx][1]).strip() if len(data) > row_idx and len(data[row_idx]) > 1 else ''
        return {
            'tg_token':             val(1),
            'score_threshold':      int(val(2) or 7),
            'moderation_threshold': int(val(3) or 3),
            'min_length':           int(val(4) or 20),
            'moderator_chat_id':    val(5),
            'excluded_accounts':    val(8),
        }
    except Exception as e:
        log.error('Ошибка чтения настроек: ' + str(e), exc_info=True)
        return None


def _parse_excluded_accounts(raw: str) -> set:
    result = set()
    for part in raw.split(','):
        part = part.strip()
        if part.isdigit():
            result.add(int(part))
    return result


def _read_scoring_rules(ss):
    try:
        data = ss.worksheet('Скоринг').get_all_values()
        rules = []
        for row in data[1:]:
            if not row or not row[0].strip():
                continue
            try:
                weight = int(str(row[1]).strip())
            except (ValueError, IndexError):
                continue
            keywords_raw = row[2] if len(row) > 2 else ''
            keywords = [k.strip().lower() for k in keywords_raw.split(',') if k.strip()]
            if keywords:
                rules.append({'category': row[0].strip(), 'weight': weight, 'keywords': keywords})
        return rules
    except Exception as e:
        log.error('Ошибка чтения скоринга: ' + str(e), exc_info=True)
        return []


def _read_minus_words(ss):
    try:
        data = ss.worksheet('Минус-слова').get_all_values()
        return [str(row[0]).strip().lower() for row in data[1:] if row and row[0].strip()]
    except Exception as e:
        log.error('Ошибка чтения минус-слов: ' + str(e), exc_info=True)
        return []


def _read_realtors_raw(ss) -> tuple[set, list]:
    try:
        try:
            ws = ss.worksheet('Риэлторы')
        except Exception:
            ws = ss.add_worksheet('Риэлторы', 1000, 3)
            ws.append_row(['user_id', 'имя', 'комментарий'])
            return set(), []

        resolved   = set()
        to_resolve = []

        for row in ws.get_all_values()[1:]:
            if not row or not row[0].strip():
                continue
            raw = row[0].strip()
            if raw.lstrip('-').isdigit():
                resolved.add(int(raw))
                continue
            username = _extract_username(raw)
            if username:
                to_resolve.append(username)
            else:
                log.warning(f'[риэлторы] Не удалось распознать запись: {repr(raw)}')

        log.info(f'Риэлторы: {len(resolved)} числовых ID, {len(to_resolve)} username для резолва')
        return resolved, to_resolve

    except Exception as e:
        log.error(f'Ошибка чтения риэлторов: {e}', exc_info=True)
        return set(), []


async def _resolve_realtors(ss, clients: dict, resolved: set, to_resolve: list) -> set:
    if not to_resolve:
        return resolved

    client = next(iter(clients.values()), None)
    if not client:
        log.warning('[риэлторы] Нет Telethon-клиента для резолва username-ов')
        return resolved

    try:
        ws   = ss.worksheet('Риэлторы')
        rows = ws.get_all_values()
    except Exception as e:
        log.error(f'[риэлторы] Не удалось открыть лист для обновления: {e}')
        return resolved

    row_index: dict[str, int] = {}
    for i, row in enumerate(rows[1:], start=2):
        if not row or not row[0].strip():
            continue
        raw = row[0].strip()
        if not raw.lstrip('-').isdigit():
            uname = _extract_username(raw)
            if uname:
                row_index[uname.lower()] = i

    for username in to_resolve:
        try:
            entity  = await client.get_entity(username)
            user_id = entity.id
            resolved.add(user_id)
            log.info(f'[риэлторы] @{username} → user_id={user_id}')
            sheet_row = row_index.get(username.lower())
            if sheet_row:
                try:
                    ws.update(values=[[str(user_id)]], range_name=f'A{sheet_row}')
                except Exception as e:
                    log.warning(f'[риэлторы] Не удалось обновить A{sheet_row}: {e}')
        except FloodWaitError as e:
            log.warning(f'[риэлторы] FloodWait {e.seconds}s при резолве @{username}')
            await asyncio.sleep(e.seconds + 2)
            try:
                entity  = await client.get_entity(username)
                resolved.add(entity.id)
            except Exception as e2:
                log.warning(f'[риэлторы] Повторная ошибка @{username}: {e2}')
        except (UsernameNotOccupiedError, UsernameInvalidError) as e:
            log.warning(f'[риэлторы] Username не найден @{username}: {e}')
        except Exception as e:
            log.warning(f'[риэлторы] Ошибка резолва @{username}: {e}')
        await asyncio.sleep(0.5)

    return resolved


def _read_channels(ss):
    try:
        data = ss.worksheet('Каналы').get_all_values()
        result = []
        for row in data[1:]:
            if not row or not row[0].strip():
                continue
            username = _extract_username(row[0].strip())
            if not username:
                continue
            city  = str(row[1]).strip() if len(row) > 1 else ''
            theme = str(row[2]).strip() if len(row) > 2 else ''
            result.append({'username': username, 'city': city, 'theme': theme})
        return result
    except Exception as e:
        log.error('Ошибка чтения каналов: ' + str(e), exc_info=True)
        return []


# ══════════════════════════════════════════════════════════════════════════════
# Подписчики бота (CRM)
# ══════════════════════════════════════════════════════════════════════════════

def _read_bot_subscribers(ss) -> dict:
    """
    Возвращает dict: {chat_id: {'city': str, 'theme': str}}
    Только активные/триал с не истёкшей датой И с выбранным городом.
    """
    try:
        ws   = ss.worksheet('CRM')
        rows = ws.get_all_values()
        result = {}
        today = _local_now().date()
        for row in rows[5:]:
            if not row:
                continue
            chat_id_raw = row[0].strip()
            subscribed  = row[8].strip()   # I
            status      = row[9].strip()   # J
            end_date    = row[12].strip()  # M
            city        = row[15].strip() if len(row) > 15 else ''   # P
            theme       = row[16].strip() if len(row) > 16 else ''   # Q
            if subscribed != 'Да':
                continue
            if not chat_id_raw.lstrip('-').isdigit():
                continue
            if status not in ('✅ Активен', '🔵 Триал'):
                continue
            try:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                if end_dt < today:
                    continue
            except ValueError:
                continue
            if not city:
                continue  # без города — не получает ничего
            uname = row[1].strip().lstrip('@') if len(row) > 1 else ''
            result[int(chat_id_raw)] = {'city': city, 'theme': theme, 'username': uname}
        log.info(f'Активных подписчиков загружено из CRM: {len(result)}')
        return result
    except Exception as e:
        log.error(f'Ошибка чтения подписчиков из CRM: {e}', exc_info=True)
        return {}


def _add_bot_subscriber(ss, chat_id: int, username: str = ''):
    """
    Вызывается при /start. Ищет строку в CRM по chat_id или username.
    Если нашёл — ставит 'Подписался на бот' = Да, запускает триал.
    Если не нашёл — создаёт новую строку.
    """
    try:
        ws   = ss.worksheet('CRM')
        rows = ws.get_all_values()

        today_dt  = _local_now().strftime('%Y-%m-%d')
        trial_end = (_local_now() + timedelta(days=3)).strftime('%Y-%m-%d')

        found_row = None
        for i, row in enumerate(rows[5:], start=6):
            if not row:
                continue
            row_chat_id  = row[0].strip()
            row_username = row[1].strip().lstrip('@')
            clean_username = username.lstrip('@')
            if (row_chat_id and row_chat_id == str(chat_id)) or \
               (clean_username and row_username.lower() == clean_username.lower()):
                found_row = i
                break

        if found_row:
            existing_subscribed = rows[found_row - 1][8].strip() if len(rows[found_row - 1]) > 8 else ''
            if existing_subscribed == 'Да':
                log.info(f'[подписчик] {username} (chat_id={chat_id}) уже подписан, пропускаем')
                return False

            cells = [
                gspread.Cell(found_row, 1,  str(chat_id)),
                gspread.Cell(found_row, 9,  'Да'),
                gspread.Cell(found_row, 10, '🔵 Триал'),
                gspread.Cell(found_row, 11, today_dt),
                gspread.Cell(found_row, 12, '3'),
                gspread.Cell(found_row, 13, trial_end),
            ]
            ws.update_cells(cells, value_input_option='USER_ENTERED')
            log.info(f'[подписчик] {username} (chat_id={chat_id}) — триал запущен, истекает {trial_end}')
            return {'is_new': False, 'username': username, 'chat_id': chat_id, 'trial_end': trial_end}
        else:
            next_row = len(rows) + 1
            cells = [
                gspread.Cell(next_row, 1,  str(chat_id)),
                gspread.Cell(next_row, 2,  f'@{username.lstrip("@")}'),
                gspread.Cell(next_row, 5,  today_dt),
                gspread.Cell(next_row, 9,  'Да'),
                gspread.Cell(next_row, 10, '🔵 Триал'),
                gspread.Cell(next_row, 11, today_dt),
                gspread.Cell(next_row, 12, '3'),
                gspread.Cell(next_row, 13, trial_end),
            ]
            ws.update_cells(cells, value_input_option='USER_ENTERED')
            log.info(f'[подписчик] НОВЫЙ {username} (chat_id={chat_id}) — пришёл напрямую, триал до {trial_end}')
            return {'is_new': True, 'username': username, 'chat_id': chat_id, 'trial_end': trial_end}

    except Exception as e:
        log.error(f'Ошибка добавления подписчика в CRM: {e}', exc_info=True)
        return False


def _remove_bot_subscriber(ss, chat_id: int):
    """Вызывается при /stop. Ставит Подписался = Нет, Статус = Отключён."""
    try:
        ws   = ss.worksheet('CRM')
        rows = ws.get_all_values()

        for i, row in enumerate(rows[5:], start=6):
            if not row:
                continue
            if row[0].strip() == str(chat_id):
                cells = [
                    gspread.Cell(i, 9,  'Нет'),
                    gspread.Cell(i, 10, '🔴 Отключён'),
                ]
                ws.update_cells(cells, value_input_option='USER_ENTERED')
                log.info(f'[подписчик] chat_id={chat_id} отписался — статус обновлён')
                return True

        log.warning(f'[подписчик] chat_id={chat_id} не найден в CRM для отписки')
        return False

    except Exception as e:
        log.error(f'Ошибка удаления подписчика из CRM: {e}', exc_info=True)
        return False

def _set_subscriber_city(ss, chat_id: int, city: str) -> bool:
    """Записывает выбранный город (P) и тему 'Арендаторы' (Q) в CRM."""
    try:
        ws   = ss.worksheet('CRM')
        rows = ws.get_all_values()
        for i, row in enumerate(rows[5:], start=6):
            if not row:
                continue
            if row[0].strip() == str(chat_id):
                cells = [
                    gspread.Cell(i, 16, city),
                    gspread.Cell(i, 17, 'Арендаторы'),
                ]
                ws.update_cells(cells, value_input_option='USER_ENTERED')
                log.info(f'[CRM] chat_id={chat_id} → город: {city}, тема: Арендаторы')
                return True
        log.warning(f'[CRM] chat_id={chat_id} не найден для записи города')
        return False
    except Exception as e:
        log.error(f'Ошибка записи города/темы в CRM: {e}', exc_info=True)
        return False
        
def _expire_crm_subscriptions(ss) -> list[int]:
    """
    Проверяет CRM: у кого дата окончания < сегодня и статус Активен/Триал —
    ставит статус 'Не продлил', колонку I = 'Нет'.
    Возвращает список chat_id которых нужно уведомить и отключить.
    """
    try:
        ws   = ss.worksheet('CRM')
        rows = ws.get_all_values()
        today = _local_now().date()
        expired = []

        for i, row in enumerate(rows[5:], start=6):
            if not row or not row[0].strip():
                continue
            chat_id_raw = row[0].strip()
            status      = row[9].strip()   # J
            end_date    = row[12].strip()  # M

            if status not in ('✅ Активен', '🔵 Триал'):
                continue
            if not chat_id_raw.lstrip('-').isdigit():
                continue
            try:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            except ValueError:
                continue

            if end_dt < today:
                cells = [
                    gspread.Cell(i, 9,  'Нет'),
                    gspread.Cell(i, 10, '🔴 Не продлил'),
                ]
                ws.update_cells(cells, value_input_option='USER_ENTERED')
                expired.append(int(chat_id_raw))
                log.info(f'[CRM] chat_id={chat_id_raw} — истёк {end_date}, статус → Не продлил')

        return expired
    except Exception as e:
        log.error(f'Ошибка проверки истёкших подписок: {e}', exc_info=True)
        return []

# ══════════════════════════════════════════════════════════════════════════════
# Запись
# ══════════════════════════════════════════════════════════════════════════════

def _local_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(
        timezone(timedelta(hours=TZ_OFFSET_HOURS))
    ).replace(tzinfo=None)


def _local_dt(dt: datetime) -> datetime:
    tz_local = timezone(timedelta(hours=TZ_OFFSET_HOURS))
    return dt.replace(tzinfo=timezone.utc).astimezone(tz_local).replace(tzinfo=None)


def _flatten_text(text: str) -> str:
    return re.sub(r'\s+', ' ', text or '').strip()


def _write_post(ss, post):
    try:
        ss.worksheet('Посты').append_row([
            _local_dt(post['date']).strftime('%Y-%m-%d %H:%M:%S'),
            post['chat_name'],
            post['author_name'],
            post['author_link'],
            post['link'],
            _flatten_text(post['text']),
            post['score'],
            post['account'],
            post.get('ai_decision', ''),
        ], value_input_option='USER_ENTERED')
    except Exception as e:
        log.error('Ошибка записи поста: ' + str(e), exc_info=True)
        raise

def _write_ai_rejected(ss, post: dict, ai_decision: str):
    try:
        ss.worksheet('Отклонено ИИ').append_row([
            _local_dt(post['date']).strftime('%Y-%m-%d %H:%M:%S'),
            post['chat_name'],
            post['link'],
            _flatten_text(post['text']),
            post['score'],
            ai_decision,
            post['account'],
        ], value_input_option='USER_ENTERED')
    except Exception as e:
        log.error(f'Ошибка записи в Отклонено ИИ: {e}', exc_info=True)


def _add_realtor_to_sheet(ss, post: dict, user_id: int):
    try:
        try:
            ws = ss.worksheet('Риэлторы')
        except Exception:
            ws = ss.add_worksheet('Риэлторы', 1000, 4)
            ws.append_row(['user_id', 'имя', 'комментарий', 'username'])

        existing = [r[0].strip() for r in ws.get_all_values()[1:] if r]
        if str(user_id) in existing:
            return

        author_link = post.get('author_link', '')
        username = ''
        if author_link:
            m = re.search(r't\.me/([a-zA-Z0-9_]+)', author_link)
            if m:
                username = '@' + m.group(1)

        ws.append_row([
            str(user_id),
            post.get('author_name', ''),
            f'авто: AI определил агента {_local_now().strftime("%Y-%m-%d %H:%M")}',
            username,
        ], value_input_option='USER_ENTERED')
        log.info(f'[риэлторы] user_id={user_id} ({username}) записан в лист')
    except Exception as e:
        log.error(f'Ошибка записи риэлтора: {e}', exc_info=True)


def _write_log(ss, level, message, account=''):
    try:
        safe = str(message)
        if safe and safe[0] in '=+-@':
            safe = "'" + safe
        ss.worksheet('Логи').append_row(
            [_local_now().strftime('%Y-%m-%d %H:%M:%S'), level, safe, str(account)],
            value_input_option='USER_ENTERED',
        )
    except Exception as e:
        log.error('Ошибка записи лога: ' + str(e), exc_info=True)


def _read_entity_cache(ss) -> dict:
    try:
        try:
            ws = ss.worksheet('Кеш')
        except Exception:
            ws = ss.add_worksheet('Кеш', 1000, 3)
            ws.append_row(['username', 'entity_id', 'chat_name'])
            return {}
        result = {}
        for row in ws.get_all_values()[1:]:
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                try:
                    result[row[0].strip()] = {
                        'entity_id': int(float(row[1].strip())),
                        'chat_name': row[2].strip() if len(row) > 2 else '',
                        'username':  row[0].strip(),
                    }
                except ValueError:
                    pass
        log.info(f'Кеш загружен: {len(result)} каналов')
        return result
    except Exception as e:
        log.error(f'Ошибка чтения кеша: {e}', exc_info=True)
        return {}


def _write_entity_cache(ss):
    try:
        try:
            ws = ss.worksheet('Кеш')
        except Exception:
            ws = ss.add_worksheet('Кеш', 1000, 3)
        rows = [['username', 'entity_id', 'chat_name']]
        for uname, meta in state['username_to_meta'].items():
            rows.append([uname, str(meta.get('entity_id', '')), meta.get('chat_name', '')])
        ws.clear()
        ws.update(rows, value_input_option='USER_ENTERED')
        log.info(f'Кеш записан: {len(rows) - 1} каналов')
    except Exception as e:
        log.error(f'Ошибка записи кеша: {e}', exc_info=True)

def _add_crm_comment(ss, chat_id: int, comment: str):
    try:
        ws = ss.worksheet('CRM')
        rows = ws.get_all_values()
        now_str = _local_now().strftime('%Y-%m-%d %H:%M')
        for i, row in enumerate(rows[5:], start=6):
            if not row or not row[0].strip():
                continue
            if row[0].strip() == str(chat_id):
                # Колонка R = индекс 18 (0-based), номер 18 в gspread (1-based)
                existing = row[17].strip() if len(row) > 17 else ''
                new_comment = f'{now_str} — {comment}'
                if existing:
                    new_comment = existing + '\n' + new_comment
                ws.update(values=[[new_comment]], range_name=f'R{i}')
                log.info(f'[CRM] Комментарий для chat_id={chat_id}: {comment}')
                return True
        log.warning(f'[CRM] chat_id={chat_id} не найден для записи комментария')
        return False
    except Exception as e:
        log.error(f'Ошибка записи комментария в CRM: {e}', exc_info=True)
        return False

# ══════════════════════════════════════════════════════════════════════════════
# Fingerprints
# ══════════════════════════════════════════════════════════════════════════════

def _post_fingerprint(text: str, author_name: str) -> str:
    import unicodedata
    norm = unicodedata.normalize('NFKC', text.lower())
    norm = re.sub(r'[\s\W]+', '', norm)[:120]
    author_key = re.sub(r'\s+', '', author_name.lower())
    return f'{author_key}|{norm}'


def _load_published_fingerprints(ss) -> set:
    try:
        from datetime import date
        today     = date.today()
        yesterday = today - timedelta(days=1)
        cutoff    = yesterday.strftime('%Y-%m-%d')

        data   = ss.worksheet('Посты').get_all_values()
        result = set()
        skipped = 0
        for row in data[1:]:
            ts          = row[0].strip() if row else ''
            text        = row[5].strip() if len(row) > 5 else ''
            author_name = row[2].strip() if len(row) > 2 else ''
            if ts[:10] < cutoff:
                skipped += 1
                continue
            if text:
                result.add(_post_fingerprint(text, author_name))
        log.info(f'Загружено {len(result)} fingerprint-ов из «Посты» (пропущено: {skipped})')
        return result
    except Exception as e:
        log.error(f'Ошибка загрузки fingerprints (Посты): {e}', exc_info=True)
        return set()


def _load_ai_rejected_fingerprints(ss) -> set:
    try:
        data = ss.worksheet('Отклонено ИИ').get_all_values()
        result = set()
        for row in data[1:]:
            text = row[3].strip() if len(row) > 3 else ''
            if text:
                result.add(_post_fingerprint(text, ''))
        log.info(f'Загружено {len(result)} fingerprint-ов из «Отклонено ИИ»')
        return result
    except Exception as e:
        log.error(f'Ошибка загрузки fingerprints (Отклонено ИИ): {e}', exc_info=True)
        return set()

def _mark_channel_delayed(ss, channel_names: set):
    """Ставит статус 'Задержка' в колонку D листа Каналы для указанных каналов."""
    if not channel_names:
        return
    try:
        ws   = ss.worksheet('Каналы')
        rows = ws.get_all_values()
        cells = []
        for i, row in enumerate(rows[1:], start=2):
            if not row:
                continue
            # Имя канала может быть в колонке A (username) или
            # мы сверяем по chat_name из кеша — ищем по username → chat_name
            username = row[0].strip()
            name_in_cache = ''
            for uname, meta in state.get('username_to_meta', {}).items():
                if _extract_username(username) == uname:
                    name_in_cache = meta.get('chat_name', '')
                    break
            if name_in_cache in channel_names:
                existing_status = row[3].strip() if len(row) > 3 else ''
                if existing_status != 'Задержка':
                    cells.append(gspread.Cell(i, 4, 'Задержка'))
        if cells:
            ws.update_cells(cells, value_input_option='USER_ENTERED')
            log.info(f'[каналы] Статус Задержка проставлен: {len(cells)} каналов')
    except Exception as e:
        log.error(f'Ошибка записи статуса задержки: {e}', exc_info=True)
