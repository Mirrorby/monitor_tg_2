"""
Фоновые задачи: перезагрузка настроек, bot polling (/start /stop + модерация),
очистка устаревших pending-постов, heartbeat.
"""
import asyncio
import time

import config
from config import (
    SETTINGS_RELOAD_SEC, state, _executor, metrics,
    published_fingerprints, pending_moderation, log,
)
from sheets import (
    _safe_sheets, _safe_sheets_retry, _safe_sheets_result,
    _read_settings, _read_scoring_rules, _read_minus_words,
    _read_realtors_raw, _read_channels, _read_bot_subscribers,
    _write_post, _update_rejected_status, _add_realtor_to_sheet,
    _parse_excluded_accounts, _expire_crm_subscriptions,
    _add_crm_comment,
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
            loop = asyncio.get_event_loop()
            log.info('Перезагрузка настроек...')

            def _read_all_sheets(ss):
                return (
                    _read_settings(ss),
                    _read_scoring_rules(ss),
                    _read_minus_words(ss),
                    _read_realtors_raw(ss),
                    _read_channels(ss),
                    _read_bot_subscribers(ss),
                )

            async with config._sheets_lock:
                (
                    new_settings,
                    new_rules,
                    new_minus,
                    (resolved_r, to_resolve_r),
                    new_channels,
                    new_subscribers,
                ) = await loop.run_in_executor(_executor, _read_all_sheets, ss)

            new_realtors = await _resolve_realtors(ss, clients, resolved_r, to_resolve_r)

            if new_settings:
                async with config._state_lock:
                    state.update({
                        'tg_token':             new_settings['tg_token'],
                        'score_threshold':      new_settings['score_threshold'],
                        'moderation_threshold': new_settings['moderation_threshold'],
                        'min_length':           new_settings['min_length'],
                        'moderator_chat_id':    new_settings['moderator_chat_id'],
                        'dest_chat_id':         new_settings['dest_chat_id'],           # ← возвращено
                        'dest_chat_id_agent':   new_settings.get('dest_chat_id_agent', ''),  # ← возвращено
                        'excluded_accounts':    _parse_excluded_accounts(
                                                    new_settings.get('excluded_accounts', '')),
                    })
            if new_rules is not None:
                async with config._state_lock:
                    state['scoring_rules'] = new_rules
            if new_minus is not None:
                async with config._state_lock:
                    state['minus_words'] = new_minus
            if new_realtors is not None:
                async with config._state_lock:
                    state['realtors'] = new_realtors
            if new_subscribers is not None:
                async with config._state_lock:
                    state['bot_subscribers'] = new_subscribers
            if new_channels is not None:
                await _update_watched_chats(clients, new_channels, ss)

            # Проверка истёкших подписок
            expired_ids = await _safe_sheets_result(_expire_crm_subscriptions, ss)
            if expired_ids:
                async with config._state_lock:
                    for cid in expired_ids:
                        state['bot_subscribers'].pop(cid, None)
                token = state['tg_token']
                if token:
                    for cid in expired_ids:
                        await loop.run_in_executor(
                            _executor, _tg_request, token, 'sendMessage', {
                                'chat_id': cid,
                                'text': (
                                    '⏳ Ваш доступ к рассылке объявлений истёк.\n\n'
                                    'Чтобы продолжить получать объявления по аренде '
                                    'недвижимости в Батуми — напишите в бот @lead_vitrina_help_bot.'
                                ),
                            }
                        )

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
            stale = [k for k, v in pending_moderation.items()
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

            msg = upd.get('message')
            if msg:
                chat_id  = msg.get('chat', {}).get('id')
                text     = (msg.get('text') or '').strip()
                username = msg.get('from', {}).get('username', '')

                if text.startswith('/start'):
                    await _handle_start(loop, token, moderator, chat_id, username, ss)
                elif text.startswith('/stop'):
                    await _handle_stop(loop, token, chat_id, ss)

            cq = upd.get('callback_query')
            if not cq:
                continue

            await _handle_callback(loop, cq, token, moderator, clients, ss)

        await asyncio.sleep(0)


# ── /start ─────────────────────────────────────────────────────────────────────

async def _handle_start(loop, token: str, moderator: str, chat_id: int, username: str, ss):
    from sheets import _safe_sheets_result, _add_bot_subscriber

    async with config._state_lock:
        already = chat_id in state['bot_subscribers']

    if already:
        sub_info = state['bot_subscribers'].get(chat_id, {})
        city = sub_info.get('city', '')
        if city:
            await loop.run_in_executor(
                _executor, _tg_request, token, 'sendMessage', {
                    'chat_id':    chat_id,
                    'text':       f'✅ Вы уже подписаны. Выбранный город: <b>{city}</b>',
                    'parse_mode': 'HTML',
                }
            )
        else:
            await _send_city_selection(loop, token, chat_id)
        # ── уведомляем модератора даже при повторном /start ──────────────
        if token and moderator:
            await loop.run_in_executor(
                _executor, _tg_request, token, 'sendMessage', {
                    'chat_id':    moderator,
                    'text': (
                        f'🔄 Повторный /start\n\n'
                        f'👤 Username: @{username}\n'
                        f'🆔 Chat ID: <code>{chat_id}</code>\n'
                        f'📍 Город: {city or "не выбран"}'
                    ),
                    'parse_mode': 'HTML',
                }
            )
        await _safe_sheets(_add_crm_comment, ss, chat_id,
                           f'/start повторный — @{username}')
        return

    added = await _safe_sheets_result(_add_bot_subscriber, ss, chat_id, username)
    if not added:
        return

    trial_end = added.get('trial_end', '') if isinstance(added, dict) else ''

    # ── уведомляем модератора всегда ─────────────────────────────────────
    if token and moderator:
        is_new = isinstance(added, dict) and added.get('is_new')
        tag = '🆕 Новый подписчик (без записи в CRM)' if is_new else '🔄 Подписчик найден в CRM'
        notify_text = (
            f'{tag}\n\n'
            f'👤 Username: @{username}\n'
            f'🆔 Chat ID: <code>{chat_id}</code>\n'
            f'📅 Триал до: {trial_end}'
        )
        if is_new:
            notify_text += '\n\nЗапись создана автоматически.'
        await loop.run_in_executor(
            _executor, _tg_request, token, 'sendMessage', {
                'chat_id':    moderator,
                'text':       notify_text,
                'parse_mode': 'HTML',
            }
        )

    # ── комментарий в CRM ────────────────────────────────────────────────
    await _safe_sheets(_add_crm_comment, ss, chat_id,
                       f'/start — @{username} подключился к боту')

    # ── обновляем state с username ───────────────────────────────────────
    async with config._state_lock:
        state['bot_subscribers'][chat_id] = {'city': '', 'theme': '', 'username': username}

    await loop.run_in_executor(
        _executor, _tg_request, token, 'sendMessage', {
            'chat_id': chat_id,
            'text': (
                f'🏠 Добро пожаловать!\n\n'
                f'Вам активирован бесплатный триал на 3 дня — '
                f'до {trial_end} включительно.\n\n'
                f'Выберите город, по которому хотите получать объявления об аренде:'
            ),
        }
    )
    await _send_city_selection(loop, token, chat_id)


async def _send_city_selection(loop, token: str, chat_id: int):
    await loop.run_in_executor(
        _executor, _tg_request, token, 'sendMessage', {
            'chat_id': chat_id,
            'text': '🏙 Выберите город:',
            'reply_markup': {
                'inline_keyboard': [[
                    {'text': '📍 Батуми',  'callback_data': 'city:Батуми'},
                    {'text': '📍 Тбилиси', 'callback_data': 'city:Тбилиси'},
                ]]
            },
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
                state['bot_subscribers'].pop(chat_id, None)
            log.info(f'[бот] Отписался: chat_id={chat_id}')
            # ── комментарий в CRM только при успешной отписке ────────────
            await _safe_sheets(_add_crm_comment, ss, chat_id,
                               '/stop — пользователь отписался')
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


# ── callback_query (модерация + выбор города) ─────────────────────────────────

async def _handle_callback(loop, cq: dict, token: str, moderator: str, clients: dict, ss):
    cq_id  = cq['id']
    data   = cq.get('data', '')
    from_id = cq.get('from', {}).get('id', '')
    msg_id  = cq.get('message', {}).get('message_id', 0)

    # ── выбор города ─────────────────────────────────────────────────────
    if data.startswith('city:'):
        city = data.split(':', 1)[1].strip()
        await _handle_city_choice(loop, cq, token, city, ss)
        return

    # ── модерация постов ──────────────────────────────────────────────────
    parts = data.split(':', 2)
    if len(parts) != 3 or parts[0] not in ('approve_private', 'approve_agent', 'skip'):
        await loop.run_in_executor(
            _executor, _answer_callback, token, cq_id, '⚠️ Неизвестная команда'
        )
        return

    action, src_chat_id_str, src_msg_id_str = parts
    pend_key = f'{src_chat_id_str}:{src_msg_id_str}'
    post = pending_moderation.get(pend_key)

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


async def _handle_city_choice(loop, cq: dict, token: str, city: str, ss):
    from sheets import _safe_sheets_retry, _set_subscriber_city

    cq_id   = cq['id']
    chat_id = cq.get('from', {}).get('id')
    msg_id  = cq.get('message', {}).get('message_id', 0)

    await _safe_sheets_retry(_set_subscriber_city, ss, chat_id, city)

    async with config._state_lock:
        if chat_id in state['bot_subscribers']:
            state['bot_subscribers'][chat_id]['city'] = city
        else:
            state['bot_subscribers'][chat_id] = {'city': city, 'theme': ''}

    log.info(f'[бот] chat_id={chat_id} выбрал город: {city}')

    await loop.run_in_executor(
        _executor, _tg_request, token, 'editMessageReplyMarkup', {
            'chat_id':      chat_id,
            'message_id':   msg_id,
            'reply_markup': {'inline_keyboard': []},
        }
    )
    await loop.run_in_executor(
        _executor, _tg_request, token, 'sendMessage', {
            'chat_id': chat_id,
            'text': (
                f'✅ Настройки сохранены — выбран город <b>{city}</b>.\n\n'
                f'Новые объявления об аренде недвижимости в {city} '
                f'будут приходить сюда автоматически.\n\n'
                f'Для подключения второго города напишите в бот @lead_vitrina_help_bot.\n\n'
                f'Чтобы отписаться — отправьте /stop'
            ),
            'parse_mode': 'HTML',
        }
    )
    await loop.run_in_executor(
        _executor, _answer_callback, token, cq_id, f'✅ Город {city} выбран'
    )
