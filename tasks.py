"""
Фоновые задачи: перезагрузка настроек, bot polling (/start /stop + модерация),
очистка устаревших pending-постов, heartbeat.
"""
import asyncio
import time

import config
from config import (
    SETTINGS_RELOAD_SEC, state, _executor, metrics, pending_moderation,
    published_fingerprints, log,
)
from sheets import (
    _safe_sheets, _safe_sheets_retry, _safe_sheets_result,
    _read_settings, _read_scoring_rules, _read_minus_words,
    _read_realtors_raw, _read_channels, _read_bot_subscribers,
    _write_post, _update_rejected_status, _add_realtor_to_sheet,
    _parse_excluded_accounts,
)
from sheets import _resolve_realtors
from channels import _update_watched_chats
from bot_api import (
    _tg_request, _get_updates, _answer_callback, _edit_message_reply_markup,
    _broadcast_to_bot, _send_moderation_card,
)
from publisher import _publish_to_channel
from sheets import _post_fingerprint


# ══════════════════════════════════════════════════════════════════════════════
# Перезагрузка настроек
# ══════════════════════════════════════════════════════════════════════════════

async def _settings_reload_loop(clients: dict, ss):
    while True:
        await asyncio.sleep(SETTINGS_RELOAD_SEC)
        try:
            log.info('Перезагрузка настроек...')
            new_settings    = await _safe_sheets_result(_read_settings,         ss)
            new_rules       = await _safe_sheets_result(_read_scoring_rules,    ss)
            new_minus       = await _safe_sheets_result(_read_minus_words,      ss)
            resolved_r, to_resolve_r = await _safe_sheets_result(_read_realtors_raw, ss)
            new_realtors    = await _resolve_realtors(ss, clients, resolved_r, to_resolve_r)
            new_channels    = await _safe_sheets_result(_read_channels,         ss)
            new_subscribers = await _safe_sheets_result(_read_bot_subscribers,  ss)

            if new_settings:
                async with config._state_lock:
                    state.update({
                        'tg_token':             new_settings['tg_token'],
                        'score_threshold':      new_settings['score_threshold'],
                        'moderation_threshold': new_settings['moderation_threshold'],
                        'min_length':           new_settings['min_length'],
                        'moderator_chat_id':    new_settings['moderator_chat_id'],
                        'dest_chat_id':         new_settings['dest_chat_id'],
                        'dest_chat_id_agent':   new_settings.get('dest_chat_id_agent', ''),
                        'excluded_accounts':    _parse_excluded_accounts(
                                                    new_settings.get('excluded_accounts', '')),
                    })
            if new_rules       is not None:
                async with config._state_lock: state['scoring_rules']   = new_rules
            if new_minus       is not None:
                async with config._state_lock: state['minus_words']     = new_minus
            if new_realtors    is not None:
                async with config._state_lock: state['realtors']        = new_realtors
            if new_subscribers is not None:
                async with config._state_lock: state['bot_subscribers'] = new_subscribers
            if new_channels    is not None:
                await _update_watched_chats(clients, new_channels, ss)

            log.info(
                f'Настройки применены | каналов: {len(state["watched_ids"])} | '
                f'правил: {len(state["scoring_rules"])} | '
                f'минус-слов: {len(state["minus_words"])} | '
                f'порог: {state["score_threshold"]} | '
                f'подписчиков бота: {len(state["bot_subscribers"])} | '
                f'канал частных: {state["dest_chat_id"] or "—"} | '
                f'канал агентов: {state["dest_chat_id_agent"] or "—"}'
            )
        except Exception as e:
            log.error('Ошибка перезагрузки настроек: ' + str(e), exc_info=True)


# ══════════════════════════════════════════════════════════════════════════════
# Очистка устаревших pending-постов
# ══════════════════════════════════════════════════════════════════════════════

async def _cleanup_pending_loop():
    while True:
        await asyncio.sleep(3600)
        try:
            cutoff = time.time() - 86400
            stale  = [k for k, v in pending_moderation.items()
                      if v.get('added_at', 0) < cutoff]
            for k in stale:
                pending_moderation.pop(k, None)
            if stale:
                log.info(f'Cleanup: удалено {len(stale)} устаревших pending-постов')
        except Exception as e:
            log.error(f'Ошибка cleanup pending: {e}', exc_info=True)


# ══════════════════════════════════════════════════════════════════════════════
# Heartbeat
# ══════════════════════════════════════════════════════════════════════════════

async def _heartbeat_loop():
    while True:
        await asyncio.sleep(300)
        log.info(
            f'[heartbeat] processed:{metrics["processed"]} '
            f'published:{metrics["published"]} '
            f'moderated:{metrics["moderated"]} '
            f'errors:{metrics["errors"]} '
            f'bot_sent:{metrics["bot_sent"]} '
            f'bot_blocked:{metrics["bot_blocked"]} '
            f'subscribers:{len(state["bot_subscribers"])} '
            f'pending_mod:{len(pending_moderation)}'
        )


# ══════════════════════════════════════════════════════════════════════════════
# Bot polling — /start, /stop + модерация
# ══════════════════════════════════════════════════════════════════════════════

async def _bot_polling_loop(clients: dict, ss):
    offset = 0
    loop   = asyncio.get_event_loop()

    def _first_client():
        return next(iter(clients.values()), None)

    # Сбрасываем webhook и ждём свободного слота
    token = state['tg_token']
    if token:
        for attempt in range(10):
            result = await loop.run_in_executor(
                _executor, _tg_request, token, 'getUpdates',
                {'offset': 0, 'timeout': 1, 'allowed_updates': ['message', 'callback_query']},
                15,
            )
            if result.get('ok'):
                log.info('[bot_polling] Слот getUpdates свободен — стартуем')
                break
            if result.get('error_code') == 409 or 'Conflict' in result.get('description', ''):
                wait = min(15 * (attempt + 1), 60)
                log.warning(f'[bot_polling] 409 при старте (попытка {attempt + 1}) — жду {wait}с')
                await asyncio.sleep(wait)
            else:
                break

    while True:
        async with config._state_lock:
            token     = state['tg_token']
            moderator = state['moderator_chat_id']

        if not token:
            await asyncio.sleep(5)
            continue

        try:
            updates = await loop.run_in_executor(_executor, _get_updates, token, offset, 30)
        except RuntimeError as e:
            err_str = str(e)
            if err_str.startswith('409:'):
                log.warning('[bot_polling] 409 Conflict — жду 15с + deleteWebhook')
                await asyncio.sleep(15)
                await loop.run_in_executor(
                    _executor, _tg_request,
                    token, 'deleteWebhook', {'drop_pending_updates': False}
                )
            elif err_str.startswith('401:') or err_str.startswith('403:'):
                log.critical(f'[bot_polling] Неверный токен бота ({err_str}) — останавливаемся')
                return
            else:
                log.error(f'[bot_polling] getUpdates RuntimeError: {e}', exc_info=True)
                await asyncio.sleep(5)
            continue
        except Exception as e:
            log.error(f'[bot_polling] getUpdates error: {e}', exc_info=True)
            await asyncio.sleep(5)
            continue

        for upd in updates:
            offset = upd['update_id'] + 1

            # ── /start и /stop ─────────────────────────────────────────────
            msg = upd.get('message')
            if msg:
                chat_id  = msg.get('chat', {}).get('id')
                text     = (msg.get('text') or '').strip()
                username = msg.get('from', {}).get('username', '')

                if text.startswith('/start'):
                    await _handle_start(loop, token, moderator, chat_id, username, ss)

                elif text.startswith('/stop'):
                    await _handle_stop(loop, token, chat_id, ss)

            # ── callback_query — модерация ──────────────────────────────────
            cq = upd.get('callback_query')
            if not cq:
                continue

            await _handle_callback(loop, cq, token, moderator, clients, ss)

        await asyncio.sleep(0)


# ── /start ─────────────────────────────────────────────────────────────────────

async def _handle_start(loop, token: str, moderator: str, chat_id: int, username: str, ss):
    from sheets import _safe_sheets_result, _safe_sheets_retry
    from sheets import _add_bot_subscriber

    async with config._state_lock:
        already = chat_id in state['bot_subscribers']

    if already:
        await loop.run_in_executor(
            _executor, _tg_request, token, 'sendMessage', {
                'chat_id': chat_id,
                'text':    '✅ Вы уже подписаны на рассылку объявлений.',
            }
        )
    else:
        async with config._state_lock:
            added = await _safe_sheets_result(_add_bot_subscriber, ss, chat_id, username)
        if added:
            async with config._state_lock:
                state['bot_subscribers'].add(chat_id)
            log.info(f'[бот] Новый подписчик: chat_id={chat_id} @{username}')

            if isinstance(added, dict) and added.get('is_new') and token and moderator:
                notify_text = (
                    f'🆕 <b>Новый подписчик без записи в CRM</b>\n\n'
                    f'👤 Username: @{username}\n'
                    f'🆔 Chat ID: <code>{chat_id}</code>\n'
                    f'📅 Триал до: {added["trial_end"]}\n\n'
                    f'Запись создана автоматически.'
                )
                await loop.run_in_executor(
                    _executor, _tg_request, token, 'sendMessage', {
                        'chat_id':    moderator,
                        'text':       notify_text,
                        'parse_mode': 'HTML',
                    }
                )
        await loop.run_in_executor(
            _executor, _tg_request, token, 'sendMessage', {
                'chat_id': chat_id,
                'text': (
                    '🏠 Добро пожаловать!\n\n'
                    'Вы подписаны на рассылку объявлений об аренде '
                    'недвижимости в Батуми.\n\n'
                    'Новые объявления будут приходить сюда автоматически.\n\n'
                    'Чтобы отписаться — отправьте /stop'
                ),
            }
        )


# ── /stop ──────────────────────────────────────────────────────────────────────

async def _handle_stop(loop, token: str, chat_id: int, ss):
    from sheets import _safe_sheets_result, _remove_bot_subscriber

    async with config._state_lock:
        was_subscribed = chat_id in state['bot_subscribers']

    if was_subscribed:
        removed = await _safe_sheets_result(_remove_bot_subscriber, ss, chat_id)
        if removed:
            async with config._state_lock:
                state['bot_subscribers'].discard(chat_id)
            log.info(f'[бот] Отписался: chat_id={chat_id}')
        await loop.run_in_executor(
            _executor, _tg_request, token, 'sendMessage', {
                'chat_id': chat_id,
                'text': (
                    '👋 Вы отписались от рассылки.\n\n'
                    'Чтобы подписаться снова — отправьте /start'
                ),
            }
        )
    else:
        await loop.run_in_executor(
            _executor, _tg_request, token, 'sendMessage', {
                'chat_id': chat_id,
                'text':    'ℹ️ Вы не были подписаны на рассылку.',
            }
        )


# ── callback_query (модерация) ─────────────────────────────────────────────────

async def _handle_callback(loop, cq: dict, token: str, moderator: str, clients: dict, ss):
    cq_id   = cq['id']
    data    = cq.get('data', '')
    from_id = cq.get('from', {}).get('id', '')
    msg_id  = cq.get('message', {}).get('message_id', 0)

    parts = data.split(':', 2)
    if len(parts) != 3 or parts[0] not in ('approve_private', 'approve_agent', 'skip'):
        await loop.run_in_executor(
            _executor, _answer_callback, token, cq_id, '⚠️ Неизвестная команда'
        )
        return

    action, src_chat_id_str, src_msg_id_str = parts
    pend_key = f'{src_chat_id_str}:{src_msg_id_str}'
    post     = pending_moderation.get(pend_key)

    if not post:
        await loop.run_in_executor(
            _executor, _answer_callback, token, cq_id,
            '⚠️ Пост уже обработан или не найден в памяти'
        )
        await loop.run_in_executor(
            _executor, _edit_message_reply_markup,
            token, moderator, msg_id, '⚠️ Пост не найден в очереди'
        )
        return

    if action in ('approve_private', 'approve_agent'):
        client = next(iter(clients.values()), None)
        async with config._state_lock:
            dest_private = state['dest_chat_id']
            dest_agent   = state.get('dest_chat_id_agent', '')
        target = dest_agent if (action == 'approve_agent' and dest_agent) else dest_private

        if client:
            try:
                if post.get('_processing'):
                    await loop.run_in_executor(
                        _executor, _answer_callback, token, cq_id, '⏳ Уже обрабатывается'
                    )
                    return
                post['_processing'] = True

                fp = _post_fingerprint(post['text'], post['author_name'])
                if fp in published_fingerprints:
                    log.info(f'[модерация ⛔ дубль] {post["chat_name"]}')
                    await loop.run_in_executor(
                        _executor, _answer_callback, token, cq_id,
                        '⛔ Дубль — такой пост уже опубликован'
                    )
                    await loop.run_in_executor(
                        _executor, _edit_message_reply_markup,
                        token, moderator, msg_id, '⛔ Дубль — публикация отменена'
                    )
                    pending_moderation.pop(pend_key, None)
                    return

                photos = post.pop('_photos', []) or []
                post['ai_decision'] = action
                label = '👤 частный' if action == 'approve_private' else '🏢 агент'

                if action == 'approve_agent':
                    user_id = post.get('user_id', 0)
                    async with config._state_lock:
                        known_realtors = set(state.get('realtors', set()))
                    if user_id and user_id not in known_realtors:
                        await _safe_sheets_retry(_add_realtor_to_sheet, ss, post, user_id)
                        async with config._state_lock:
                            state['realtors'].add(user_id)
                        log.info(f'[модерация] новый риэлтор записан user_id={user_id}')

                if target:
                    ok = await _publish_to_channel(client, post, target, photos or None)
                    if not ok:
                        metrics['errors'] += 1

                await _broadcast_to_bot(post, photos, ss)

                published_fingerprints.append(fp)
                metrics['published'] += 1
                log.info(
                    f'[модерация ✅ {label} фото:{len(photos)}] '
                    f'{post["chat_name"]} → {post["link"]}'
                )
                await _safe_sheets_retry(_write_post, ss, post)
                await _safe_sheets(
                    _update_rejected_status, ss,
                    post.get('bot_message_id', 0), f'одобрено ({label})'
                )
                await loop.run_in_executor(
                    _executor, _answer_callback, token, cq_id,
                    f'✅ Опубликовано ({label})!'
                )
                await loop.run_in_executor(
                    _executor, _edit_message_reply_markup,
                    token, moderator, msg_id,
                    f'✅ {label.capitalize()} — опубликовано модератором {from_id}'
                )
            except Exception as e:
                metrics['errors'] += 1
                log.error(f'[модерация] Ошибка: {e}', exc_info=True)
                await loop.run_in_executor(
                    _executor, _answer_callback, token, cq_id, f'❌ Ошибка: {e}'
                )
        else:
            await loop.run_in_executor(
                _executor, _answer_callback, token, cq_id,
                '⚠️ Нет активного Telethon-клиента'
            )

    elif action == 'skip':
        log.info(f'[модерация ❌ пропущено] {post["chat_name"]} → {post["link"]}')
        post.pop('_photos', None)
        await _safe_sheets(
            _update_rejected_status, ss, post.get('bot_message_id', 0), 'пропущено'
        )
        await loop.run_in_executor(
            _executor, _answer_callback, token, cq_id, '❌ Пост пропущен'
        )
        await loop.run_in_executor(
            _executor, _edit_message_reply_markup,
            token, moderator, msg_id,
            f'❌ Пропущено модератором {from_id}'
        )

    pending_moderation.pop(pend_key, None)
