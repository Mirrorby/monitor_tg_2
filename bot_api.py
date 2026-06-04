"""
Telegram Bot API: транспорт, рассылка подписчикам, отправка фото/альбомов,
карточка модерации, построение текста и клавиатуры для бота.
"""
import asyncio
import json
import time
import urllib.request
import urllib.error

from config import state, _executor, metrics, BOT_BROADCAST_DELAY, log


# ══════════════════════════════════════════════════════════════════════════════
# Базовый транспорт
# ══════════════════════════════════════════════════════════════════════════════

def _tg_request(token: str, method: str, payload: dict, timeout: int = 10) -> dict:
    url  = f'https://api.telegram.org/bot{token}/{method}'
    data = json.dumps(payload).encode('utf-8')
    req  = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            if not result.get('ok'):
                log.error(f'TG API {method} не ок: {result.get("description")} '
                          f'(error_code={result.get("error_code")})')
            return result
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode('utf-8'))
        except Exception:
            body = {}
        description = body.get('description', str(e))
        if e.code in (409, 401, 403):
            return {'ok': False, 'error_code': e.code, 'description': description}
        log.error(f'TG API {method} HTTP {e.code}: {description}', exc_info=True)
        return {'ok': False, 'error_code': e.code, 'description': description}
    except Exception as e:
        log.error(f'TG API {method} error: {e}', exc_info=True)
        return {}


def _get_updates(token: str, offset: int, timeout: int = 30) -> list:
    result = _tg_request(token, 'getUpdates', {
        'offset':  offset,
        'timeout': timeout,
        'allowed_updates': ['message', 'callback_query'],
    }, timeout=timeout + 10)
    if not result.get('ok'):
        error_code  = result.get('error_code', 0)
        description = result.get('description', '')
        if error_code == 409 or 'Conflict' in description:
            raise RuntimeError(f'409:{description}')
        if error_code in (401, 403):
            raise RuntimeError(f'{error_code}:{description}')
        return []
    return result.get('result', [])


def _answer_callback(token: str, callback_query_id: str, text: str):
    _tg_request(token, 'answerCallbackQuery', {
        'callback_query_id': callback_query_id,
        'text': text,
        'show_alert': False,
    })


def _edit_message_reply_markup(token: str, chat_id: str, message_id: int, new_text: str):
    _tg_request(token, 'editMessageReplyMarkup', {
        'chat_id':      chat_id,
        'message_id':   message_id,
        'reply_markup': {'inline_keyboard': []},
    })
    _tg_request(token, 'sendMessage', {
        'chat_id':             chat_id,
        'text':                new_text,
        'reply_to_message_id': message_id,
    })


def _send_alert(token: str, moderator_chat_id: str, message: str):
    if not token or not moderator_chat_id:
        return
    _tg_request(token, 'sendMessage', {
        'chat_id': moderator_chat_id,
        'text':    message[:4096],
    })


def _bot_request_raw(url: str, data: bytes, content_type: str,
                     timeout: int = 30, label: str = '') -> dict:
    req = urllib.request.Request(url, data=data, headers={'Content-Type': content_type})
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                result = json.loads(resp.read().decode('utf-8'))
                if not result.get('ok'):
                    log.error(f'[{label}] Bot API не ок: {result.get("description")}')
                return result
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get('Retry-After', 15))
                log.warning(f'[{label}] Bot API 429 — жду {retry_after}s')
                time.sleep(retry_after + 1)
            else:
                log.error(f'[{label}] Bot API HTTP {e.code}: {e}', exc_info=True)
                return {}
        except Exception as e:
            log.error(f'[{label}] Bot API error: {e}', exc_info=True)
            return {}
    return {}


def _build_multipart(fields: dict, files: dict) -> tuple[bytes, str]:
    boundary = 'B' + str(int(time.time() * 1000))
    parts = []
    for name, value in fields.items():
        parts.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n'
            f'{value}'.encode('utf-8')
        )
    for name, (filename, data, mime) in files.items():
        header = (
            f'--{boundary}\r\n'
            f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'
            f'Content-Type: {mime}\r\n\r\n'
        ).encode('utf-8')
        parts.append(header + data)
    body = b'\r\n'.join(parts) + f'\r\n--{boundary}--'.encode('utf-8')
    return body, f'multipart/form-data; boundary={boundary}'

# ══════════════════════════════════════════════════════════════════════════════
# Построение текста и клавиатуры для бота
# ══════════════════════════════════════════════════════════════════════════════

def _build_bot_text(post: dict) -> str:
    chat_name   = post.get('chat_name', '').strip() or 'Источник'
    link        = post.get('link', '').strip()
    ai_decision = post.get('ai_decision', '')

    if ai_decision == 'approve_private':
        label = '👤 Частный клиент'
    elif ai_decision == 'approve_agent':
        label = '🏢 Риэлтор'
    else:
        label = ''

    header_parts = []
    safe_chat_name = (chat_name
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;'))

    if link:
        header_parts.append(f'<a href="{link}">{safe_chat_name}</a>')
    else:
        header_parts.append(safe_chat_name)
    if label:
        header_parts.append(label)

    header = '\n'.join(header_parts)

    raw_text = post.get('html_text') or (
        post['text']
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
    )
    max_text = 4096 - len(header) - 3
    if len(raw_text) > max_text:
        raw_text = raw_text[:max_text].rstrip() + '…'

    return f'{header}\n\n{raw_text}'


def _build_bot_keyboard(post: dict) -> dict:
    buttons = []
    source_link = post.get('link', '').strip()
    if source_link and source_link.startswith(('http://', 'https://', 'tg://')):
        buttons.append({'text': '📢 Источник', 'url': source_link})

    author_link = post.get('author_link', '').strip()
    if author_link and author_link.startswith(('http://', 'https://', 'tg://')):
        buttons.append({'text': '👤 Автор', 'url': author_link})

    if not buttons:
        return {}
    return {'inline_keyboard': [buttons]}


# ══════════════════════════════════════════════════════════════════════════════
# Отправка одному подписчику
# ══════════════════════════════════════════════════════════════════════════════

def _send_text_to_subscriber(token: str, chat_id: int, text: str, keyboard: dict) -> str:
    """Возвращает: 'ok' | 'blocked' | 'error: <описание>'"""
    import re
    text = re.sub(r'<[^>]+>', '', text)
    payload: dict = {
        'chat_id':                  chat_id,
        'text':                     text,
        'disable_web_page_preview': True,
    }
    if keyboard:
        payload['reply_markup'] = keyboard

    result = _tg_request(token, 'sendMessage', payload, timeout=15)
    if result.get('ok'):
        return 'ok'
    code = result.get('error_code', 0)
    desc = result.get('description', '')
    if code in (403, 400) and any(
        s in desc for s in ('blocked', 'deactivated', 'not found', 'kicked')
    ):
        return 'blocked'
    return f'error: [{code}] {desc}'


def _send_photo_to_subscriber(token: str, chat_id: int, caption: str,
                               photo: bytes, keyboard: dict) -> str:
    """Отправляет одно фото с подписью одному подписчику."""
    url = f'https://api.telegram.org/bot{token}/sendPhoto'
    fields: dict = {
        'chat_id':    str(chat_id),
        'caption':    caption[:1024],
        'parse_mode': 'HTML',
    }
    if keyboard:
        fields['reply_markup'] = json.dumps(keyboard)
    body, ct = _build_multipart(
        fields=fields,
        files={'photo': ('photo.jpg', photo, 'image/jpeg')},
    )
    result = _bot_request_raw(url, body, ct, timeout=30, label=f'sendPhoto→{chat_id}')
    if result.get('ok'):
        return 'ok'
    code = result.get('error_code', 0)
    desc = result.get('description', '')
    if code in (403, 400) and any(
        s in desc for s in ('blocked', 'deactivated', 'not found', 'kicked')
    ):
        return 'blocked'
    return f'error: [{code}] {desc}'


def _send_album_to_subscriber(token: str, chat_id: int, caption: str,
                               photos: list[bytes], keyboard: dict) -> str:
    """
    Отправляет альбом (2–10 фото) одному подписчику.
    Инлайн-кнопки прикрепляются к отдельному сообщению после альбома.
    """
    photos = photos[:10]
    url = f'https://api.telegram.org/bot{token}/sendMediaGroup'
    media_json = []
    for i in range(len(photos)):
        item: dict = {'type': 'photo', 'media': f'attach://p{i}'}
        if i == 0:
            item['caption']    = caption[:1024]
            item['parse_mode'] = 'HTML'
        media_json.append(item)
    fields = {'chat_id': str(chat_id), 'media': json.dumps(media_json)}
    files  = {f'p{i}': (f'p{i}.jpg', pb, 'image/jpeg') for i, pb in enumerate(photos)}
    body, ct = _build_multipart(fields, files)
    result = _bot_request_raw(url, body, ct, timeout=60, label=f'sendAlbum→{chat_id}')

    if not result.get('ok'):
        code = result.get('error_code', 0)
        desc = result.get('description', '')
        if code in (403, 400) and any(
            s in desc for s in ('blocked', 'deactivated', 'not found', 'kicked')
        ):
            return 'blocked'
        return f'error: [{code}] {desc}'

    # Кнопки отдельным сообщением (sendMediaGroup не поддерживает reply_markup)
    if keyboard:
        _tg_request(token, 'sendMessage', {
            'chat_id':      chat_id,
            'text':         '🔗',
            'reply_markup': keyboard,
        }, timeout=10)

    return 'ok'


# ══════════════════════════════════════════════════════════════════════════════
# Рассылка всем подписчикам
# ══════════════════════════════════════════════════════════════════════════════

async def _broadcast_to_bot(post: dict, photos: list[bytes], ss):
    """
    Рассылает пост всем подписчикам бота.
    Автоматически убирает заблокировавших из CRM.
    """
    from config import _state_lock
    from sheets import _safe_sheets_retry, _remove_bot_subscriber

    async with _state_lock:
        token       = state['tg_token']
        subscribers = dict(state['bot_subscribers'])

    if not token or not subscribers:
        return

    post_cities = [c.strip().lower() for c in post.get('channel_city', '').split(',') if c.strip()]

    text     = _build_bot_text(post)
    keyboard = _build_bot_keyboard(post)

    blocked_ids: list[int] = []
    error_ids:   list[tuple[int, str]] = []
    sent = errors = 0

    loop = asyncio.get_event_loop()

    for chat_id, sub_info in subscribers.items():
        sub_cities = [c.strip().lower() for c in sub_info.get('city', '').split(',') if c.strip()]
        if post_cities and sub_cities and not any(c in sub_cities for c in post_cities):
            continue
        if post_cities and not sub_cities:
            continue
        try:
            if photos and len(photos) == 1:
                status = await loop.run_in_executor(
                    _executor,
                    _send_photo_to_subscriber,
                    token, chat_id, text, photos[0], keyboard,
                )
            elif photos and len(photos) > 1:
                status = await loop.run_in_executor(
                    _executor,
                    _send_album_to_subscriber,
                    token, chat_id, text, photos, keyboard,
                )
            else:
                status = await loop.run_in_executor(
                    _executor,
                    _send_text_to_subscriber,
                    token, chat_id, text, keyboard,
                )

            if status == 'ok':
                sent += 1
            elif status == 'blocked':
                blocked_ids.append(chat_id)
                log.info(f'[бот] chat_id={chat_id} заблокировал бота — удаляем')
            elif status.startswith('error'):
                errors += 1
                error_ids.append((chat_id, status))

        except Exception as e:
            errors += 1
            error_ids.append((chat_id, str(e)[:120]))
            log.warning(f'[бот] Ошибка рассылки chat_id={chat_id}: {e}')

        await asyncio.sleep(BOT_BROADCAST_DELAY)

    metrics['bot_sent']    += sent
    metrics['bot_blocked'] += len(blocked_ids)

    log.info(
        f'[бот рассылка] отправлено: {sent} | '
        f'заблокировано: {len(blocked_ids)} | ошибок: {errors} | '
        f'всего подписчиков: {len(subscribers)}'
    )
    if error_ids:
        for cid, reason in error_ids:
            log.warning(f'[бот рассылка ⚠️] chat_id={cid} — {reason}')

    if blocked_ids:
        async with _state_lock:
            for cid in blocked_ids:
                state['bot_subscribers'].pop(cid, None)
        for cid in blocked_ids:
            await _safe_sheets_retry(_remove_bot_subscriber, ss, cid)


# ══════════════════════════════════════════════════════════════════════════════
# Карточка модерации
# ══════════════════════════════════════════════════════════════════════════════

def _send_moderation_card(post: dict, token: str, moderator_chat_id: str) -> int:
    author_str = '—'
    if post['author_name']:
        if post['author_link']:
            author_str = f'<a href="{post["author_link"]}">{post["author_name"]}</a>'
        else:
            author_str = post['author_name']

    pend_key = f'{post["src_chat_id"]}:{post["src_msg_id"]}'
    lines = [
        f'📢 <b>Источник:</b> {post["chat_name"]}',
        f'👤 <b>Автор:</b> {author_str}',
        f'🏆 <b>Скор:</b> {post["score"]}',
        '',
        post['html_text'][:3500],
        '',
        f'🔗 <a href="{post["link"]}">Открыть сообщение</a>',
    ]
    result = _tg_request(token, 'sendMessage', {
        'chat_id':    moderator_chat_id,
        'text':       '\n'.join(lines)[:4096],
        'parse_mode': 'HTML',
        'disable_web_page_preview': True,
        'reply_markup': {
            'inline_keyboard': [[
                {'text': '👤 Частный',    'callback_data': f'approve_private:{pend_key}'},
                {'text': '🏢 Агент',      'callback_data': f'approve_agent:{pend_key}'},
                {'text': '❌ Пропустить', 'callback_data': f'skip:{pend_key}'},
            ]]
        },
    })
    return result.get('result', {}).get('message_id', 0)
