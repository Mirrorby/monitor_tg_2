"""
Telegram Bot API: транспорт, рассылка подписчикам,
построение текста и клавиатуры для бота.
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


def _send_alert(token: str, moderator_chat_id: str, message: str):
    if not token or not moderator_chat_id:
        return
    _tg_request(token, 'sendMessage', {
        'chat_id': moderator_chat_id,
        'text':    message[:4096],
    })


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


# ══════════════════════════════════════════════════════════════════════════════
# Рассылка всем подписчикам
# ══════════════════════════════════════════════════════════════════════════════

async def _broadcast_to_bot(post: dict, ss):
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
            token_notify = state['tg_token']
            moderator = state['moderator_chat_id']
            blocked_info = {
                cid: dict(state['bot_subscribers'].get(cid, {}))
                for cid in blocked_ids
            }
            for cid in blocked_ids:
                state['bot_subscribers'].pop(cid, None)
    
        from sheets import _add_crm_comment
        for cid in blocked_ids:
            sub = blocked_info.get(cid, {})
            uname = sub.get('username', '') or str(cid)
            city = sub.get('city', '—')
    
            # Отписка в CRM + комментарий
            await _safe_sheets_retry(_remove_bot_subscriber, ss, cid)
            await loop.run_in_executor(
                _executor, _add_crm_comment, ss, cid,
                f'🚫 заблокировал бота'
            )
    
            # Уведомление модератора
            if token_notify and moderator:
                notify = (
                    f'🚫 <b>Пользователь заблокировал бота</b>\n\n'
                    f'👤 @{uname}\n'
                    f'🆔 Chat ID: <code>{cid}</code>\n'
                    f'📍 Город: {city}'
                )
                await loop.run_in_executor(
                    _executor, _tg_request, token_notify, 'sendMessage', {
                        'chat_id': moderator,
                        'text': notify,
                        'parse_mode': 'HTML',
                    }
                )
