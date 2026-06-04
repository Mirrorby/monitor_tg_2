"""
TG Parser v5 — точка входа.
Инициализирует клиентов, регистрирует хендлеры, запускает фоновые задачи.
"""

import asyncio
import re
import time
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageService, MessageEmpty

import config
from config import (
    API_ID_1, API_HASH_1, SESSION_1,
    API_ID_2, API_HASH_2, SESSION_2,
    GEMINI_API_KEY,
    state, seen_ids, published_fingerprints, ai_rejected_fingerprints,
    metrics, log,
)
from sheets import (
    _get_spreadsheet, _safe_sheets, _safe_sheets_result,
    _read_settings, _read_scoring_rules, _read_minus_words,
    _read_realtors_raw, _read_channels, _read_bot_subscribers,
    _read_entity_cache,
    _load_published_fingerprints, _load_ai_rejected_fingerprints,
    _write_log, _parse_excluded_accounts,
)
from sheets import _resolve_realtors
from channels import _update_watched_chats
from utils import _meta_by_abs_id, _build_link, _get_author_info, _text_to_html
from utils import _find_minus_word, _calc_score
from bot_api import _tg_request
from publisher import _process_and_publish
from tasks import (
    _settings_reload_loop, _bot_polling_loop,
    _cleanup_pending_loop, _heartbeat_loop,
)


# ══════════════════════════════════════════════════════════════════════════════
# Воркер очереди постов
# ══════════════════════════════════════════════════════════════════════════════

async def _post_worker(worker_id: int, queue: asyncio.Queue, clients: dict, ss):
    """
    Берёт посты из очереди и обрабатывает по одному.
    Хендлер кладёт в очередь мгновенно и сразу освобождается —
    Telethon не теряет входящие updates во время тяжёлой обработки.
    """
    log.info(f'[worker-{worker_id}] запущен')
    while True:
        try:
            item   = await queue.get()
            post   = item['post']
            msgs   = item['msgs']
            acc    = item['acc']
            client = clients.get(acc)

            if client is None:
                log.warning(f'[worker-{worker_id}] клиент {acc} не найден — пропуск')
                queue.task_done()
                continue

            try:
                await _process_and_publish(post, client, msgs, ss, acc)
            except Exception as e:
                metrics['errors'] += 1
                log.error(f'[worker-{worker_id}] ошибка: {e}', exc_info=True)
            finally:
                queue.task_done()

        except asyncio.CancelledError:
            log.info(f'[worker-{worker_id}] остановлен')
            break
        except Exception as e:
            log.error(f'[worker-{worker_id}] неожиданная ошибка: {e}', exc_info=True)
            await asyncio.sleep(1)


async def main():
    global _sheets_lock, _state_lock

    log.info('═══ TG Parser v5 стартует ═══')

    await asyncio.sleep(5)

    config._sheets_lock = asyncio.Lock()
    config._state_lock  = asyncio.Lock()
    loop = asyncio.get_event_loop()

    # ── Google Sheets ──────────────────────────────────────────────────────
    try:
        ss = await loop.run_in_executor(config._executor, _get_spreadsheet)
        log.info('Google Sheets: подключён')
    except Exception as e:
        log.error('Google Sheets: ошибка подключения: ' + str(e), exc_info=True)
        return

    settings    = await _safe_sheets_result(_read_settings, ss)
    rules       = await _safe_sheets_result(_read_scoring_rules, ss)
    minus       = await _safe_sheets_result(_read_minus_words, ss)
    resolved_r, to_resolve_r = await _safe_sheets_result(_read_realtors_raw, ss)
    channels    = await _safe_sheets_result(_read_channels, ss)
    subscribers = await _safe_sheets_result(_read_bot_subscribers, ss)

    # ── Fingerprints ───────────────────────────────────────────────────────
    initial_fps  = await _safe_sheets_result(_load_published_fingerprints, ss)
    rejected_fps = await _safe_sheets_result(_load_ai_rejected_fingerprints, ss)
    for fp in initial_fps:
        published_fingerprints.append(fp)
    for fp in rejected_fps:
        ai_rejected_fingerprints.append(fp)
    log.info(
        f'Дедупликация: {len(initial_fps)} из «Посты», '
        f'{len(rejected_fps)} из «Отклонено ИИ»'
    )

    if not settings:
        log.error('Не удалось прочитать настройки — проверьте лист «Настройки»')
        return

    state.update({
        'tg_token':             settings['tg_token'],
        'score_threshold':      settings['score_threshold'],
        'moderation_threshold': settings['moderation_threshold'],
        'min_length':           settings['min_length'],
        'moderator_chat_id':    settings['moderator_chat_id'],
        'scoring_rules':        rules,
        'minus_words':          minus,
        'excluded_accounts':    _parse_excluded_accounts(settings.get('excluded_accounts', '')),
        'bot_subscribers':      subscribers,
    })
    log.info(
        f'Настройки загружены | '
        f'порог публикации: {state["score_threshold"]} | '
        f'порог модерации: {state["moderation_threshold"]} | '
        f'мин.длина: {state["min_length"]} | '
        f'правил: {len(rules)} | минус-слов: {len(minus)} | '
        f'подписчиков бота: {len(subscribers)}'
    )

    if GEMINI_API_KEY:
        log.info('Gemini AI: ключ задан (3 класса: approve_private, approve_agent, skip)')
    else:
        log.warning('Gemini AI: GEMINI_API_KEY не задан — fallback: approve_private')

    if state['tg_token']:
        await loop.run_in_executor(
            config._executor, _tg_request,
            state['tg_token'], 'deleteWebhook', {'drop_pending_updates': False}
        )
        log.info('Webhook сброшен')

    # ── Telegram клиенты ───────────────────────────────────────────────────
    clients: dict[str, TelegramClient] = {}

    if SESSION_1 and API_ID_1 and API_HASH_1:
        c1 = TelegramClient(StringSession(SESSION_1), API_ID_1, API_HASH_1)
        await c1.start()
        clients['acc1'] = c1
        log.info('Аккаунт 1: подключён')
    else:
        log.warning('Аккаунт 1: пропущен (нет TG_SESSION_1 / TG_API_ID_1 / TG_API_HASH_1)')

    if SESSION_2 and API_ID_2 and API_HASH_2:
        c2 = TelegramClient(StringSession(SESSION_2), API_ID_2, API_HASH_2)
        await c2.start()
        clients['acc2'] = c2
        log.info('Аккаунт 2: подключён')
    else:
        log.info('Аккаунт 2: не задан (работаем с одним аккаунтом)')

    if not clients:
        log.error('Ни один аккаунт не подключён — выход')
        return

    # ── Резолв риэлторов ───────────────────────────────────────────────────
    realtors = await _resolve_realtors(ss, clients, resolved_r, to_resolve_r)
    state['realtors'] = realtors

    # ── Резолв каналов ─────────────────────────────────────────────────────
    from utils import _extract_username
    cached_meta = await _safe_sheets_result(_read_entity_cache, ss)
    state['username_to_meta'].update(cached_meta)

    if not channels:
        log.warning('Лист «Каналы» пуст — добавьте каналы')
    else:
        known = sum(1 for ch in channels if _extract_username(ch['username']) in cached_meta)
        log.info(f'Каналов: {len(channels)} | из кеша: {known} | новых: {len(channels) - known}')

    await _update_watched_chats(clients, channels or [], ss)
    log.info(
        f'Слежу за {len(state["watched_ids"])} ID-ключами '
        f'({len(state["username_to_meta"])} каналов)'
    )

    # ══════════════════════════════════════════════════════════════════════
    # Очередь постов и воркеры
    # ══════════════════════════════════════════════════════════════════════

    post_queue: asyncio.Queue = asyncio.Queue()
    config.post_queue = post_queue

    worker_tasks = []
    for i in range(config.PROCESS_WORKERS):
        t = asyncio.create_task(_post_worker(i + 1, post_queue, clients, ss))
        worker_tasks.append(t)
    log.info(f'Воркеры очереди: {config.PROCESS_WORKERS} запущено')

    # ══════════════════════════════════════════════════════════════════════
    # Буфер альбомов и хендлеры
    # ══════════════════════════════════════════════════════════════════════

    album_buffer: dict = {}

    async def _flush_album(grouped_id: int, _acc: str, _client: TelegramClient):
        entry = album_buffer.pop(grouped_id, None)
        if not entry:
            return
        try:
            msgs = sorted(entry['msgs'], key=lambda m: m.id)
            first = msgs[0]
            try:
                raw_id = first.chat_id
            except Exception:
                raw_id = -(getattr(first.peer_id, 'channel_id', 0))
            abs_id = abs(raw_id)

            async with config._state_lock:
                id_to_meta    = dict(state['id_to_meta'])
                minus_words   = list(state['minus_words'])
                scoring_rules = list(state['scoring_rules'])
                min_length    = state['min_length']
                mod_threshold = state['moderation_threshold']

            meta = _meta_by_abs_id(id_to_meta, abs_id)
            if meta is None:
                log.info(f'[{_acc}][альбом] abs_id={abs_id} не в списке каналов — пропуск')
                return

            text = ''
            text_entities = None
            for m in msgs:
                t = m.text or m.message or ''
                if hasattr(m, 'caption') and m.caption:
                    t = m.caption
                if t.strip():
                    text = t
                    text_entities = m.entities
                    break

            html_text = _text_to_html(text, text_entities)
            chat_name = meta.get('chat_name', str(abs_id))

            minus_hit = _find_minus_word(text, minus_words)
            if minus_hit:
                log.info(f'[{_acc}][альбом минус "{minus_hit}"] {chat_name}')
                return

            if len(text) < min_length:
                log.info(f'[{_acc}][альбом короткий {len(text)}<{min_length}] {chat_name}')
                return

            score = _calc_score(text, scoring_rules)
            if score < mod_threshold:
                log.info(f'[{_acc}][альбом скор:{score}<{mod_threshold}] {chat_name}')
                return

            try:
                chat = await _client.get_entity(raw_id)
            except Exception:
                chat = None
            link = _build_link(chat, first.id) if chat else f'https://t.me/c/{abs_id}/{first.id}'

            author_name, author_link, user_id = _get_author_info(first)

            async with config._state_lock:
                excluded = set(state.get('excluded_accounts', set()))
            if user_id and user_id in excluded:
                log.info(f'[{_acc}][альбом исключён] user_id={user_id} {chat_name}')
                return

            post = {
                'date':          first.date.replace(tzinfo=None),
                'chat_name':     chat_name,
                'author_name':   author_name,
                'author_link':   author_link,
                'user_id':       user_id,
                'link':          link,
                'text':          text,
                'html_text':     html_text,
                'score':         score,
                'account':       _acc,
                'src_chat_id':   raw_id,
                'src_msg_id':    first.id,
                'grouped_refs':  [(getattr(m, 'chat_id', None) or raw_id, m.id) for m in msgs],
                'added_at':      time.time(),
                'channel_city':  meta.get('city', ''),
                'channel_theme': meta.get('theme', ''),
            }
            metrics['processed'] += 1

            post_queue.put_nowait({'post': post, 'msgs': msgs, 'acc': _acc})
            log.info(
                f'[{_acc}][альбом→queue скор:{score}] {chat_name} '
                f'| очередь: {post_queue.qsize()}'
            )

        except Exception as e:
            metrics['errors'] += 1
            log.error(f'_flush_album error grouped_id={grouped_id}: {e}', exc_info=True)

    for acc_label, client in clients.items():

        @client.on(events.NewMessage)
        async def _on_new_message(event, _acc=acc_label, _client=client):
            try:
                msg = event.message
                if isinstance(msg, (MessageService, MessageEmpty)):
                    return
                if getattr(msg, 'action', None) is not None:
                    return

                raw_id = event.chat_id
                abs_id = abs(raw_id)

                async with config._state_lock:
                    id_to_meta    = dict(state['id_to_meta'])
                    minus_words   = list(state['minus_words'])
                    scoring_rules = list(state['scoring_rules'])
                    min_length    = state['min_length']
                    mod_threshold = state['moderation_threshold']

                meta = _meta_by_abs_id(id_to_meta, abs_id)
                if meta is None:
                    return

                dedup_key = (raw_id, msg.id)
                if dedup_key in seen_ids:
                    return
                seen_ids.append(dedup_key)

                chat_name = meta.get('chat_name', str(abs_id))

                # ── Альбом ─────────────────────────────────────────────────
                grouped_id = getattr(msg, 'grouped_id', None)
                if grouped_id:
                    if grouped_id not in album_buffer:
                        handle = asyncio.get_event_loop().call_later(
                            1.5, lambda gid=grouped_id: asyncio.ensure_future(
                                _flush_album(gid, _acc, _client)
                            )
                        )
                        album_buffer[grouped_id] = {
                            'msgs': [], 'timer': handle,
                            'acc': _acc, 'client': _client,
                        }
                    album_buffer[grouped_id]['msgs'].append(msg)
                    return

                # ── Одиночное сообщение ────────────────────────────────────
                text = msg.text or getattr(msg, 'message', '') or ''
                if hasattr(msg, 'caption') and msg.caption:
                    text = msg.caption
                text = re.sub(r'[^\S\n]+', ' ', text).strip()
                html_text = _text_to_html(text, msg.entities)

                minus_hit = _find_minus_word(text, minus_words)
                if minus_hit:
                    log.info(f'[{_acc}][минус "{minus_hit}"] {chat_name} | {repr(text[:80])}')
                    return

                if len(text) < min_length:
                    log.info(f'[{_acc}][короткий {len(text)}<{min_length}] {chat_name}')
                    return

                score = _calc_score(text, scoring_rules)
                if score < mod_threshold:
                    log.info(f'[{_acc}][скор:{score}<{mod_threshold}] {chat_name} | {repr(text[:60])}')
                    return

                chat = await event.get_chat()
                link = _build_link(chat, msg.id)
                author_name, author_link, user_id = _get_author_info(msg)

                async with config._state_lock:
                    excluded = set(state.get('excluded_accounts', set()))
                if user_id and user_id in excluded:
                    log.info(f'[{_acc}][исключён] user_id={user_id} {chat_name}')
                    return

                log.info(f'[{_acc}][принят скор:{score}] {chat_name} → {link}')

                post = {
                    'date':          msg.date.replace(tzinfo=None),
                    'chat_name':     chat_name,
                    'author_name':   author_name,
                    'author_link':   author_link,
                    'user_id':       user_id,
                    'link':          link,
                    'text':          text,
                    'html_text':     html_text,
                    'score':         score,
                    'account':       _acc,
                    'src_chat_id':   raw_id,
                    'src_msg_id':    msg.id,
                    'grouped_refs':  [(msg.chat_id, msg.id)],
                    'channel_city':  meta.get('city', ''),
                    'channel_theme': meta.get('theme', ''),
                }
                metrics['processed'] += 1

                post_queue.put_nowait({'post': post, 'msgs': [msg], 'acc': _acc})
                log.info(
                    f'[{_acc}][→queue скор:{score}] {chat_name} → {link} '
                    f'| очередь: {post_queue.qsize()}'
                )

            except Exception as e:
                metrics['errors'] += 1
                log.error(f'[{_acc}] Ошибка обработки сообщения: {e}', exc_info=True)

        log.info(f'[{acc_label}] Хендлер зарегистрирован')

    await _safe_sheets(_write_log, ss, 'INFO',
        f'Запущен v5 | аккаунтов: {len(clients)} | '
        f'каналов: {len(state["username_to_meta"])} | '
        f'правил: {len(state["scoring_rules"])} | '
        f'порог: {state["score_threshold"]} | '
        f'подписчиков бота: {len(subscribers)} | '
        f'канал частных: {state["dest_chat_id"] or "—"} | '
        f'канал агентов: {state["dest_chat_id_agent"] or "—"}'
    )

    asyncio.create_task(_settings_reload_loop(clients, ss))
    asyncio.create_task(_bot_polling_loop(clients, ss))
    asyncio.create_task(_cleanup_pending_loop())
    asyncio.create_task(_heartbeat_loop())

    log.info(
        f'Слушаю события. '
        f'Настройки обновляются каждые {config.SETTINGS_RELOAD_SEC}с. '
        f'Bot polling: /start /stop + модерация. '
        f'Рассылка в бот: {len(state["bot_subscribers"])} подписчиков. '
        f'Воркеры: {config.PROCESS_WORKERS}.'
    )

    try:
        await asyncio.gather(*[c.run_until_disconnected() for c in clients.values()])
    finally:
        for name, c in clients.items():
            try:
                await c.disconnect()
                log.info(f'{name}: отключён')
            except Exception:
                pass
        for t in worker_tasks:
            t.cancel()


if __name__ == '__main__':
    asyncio.run(main())
