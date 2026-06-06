"""
Фоновые задачи: перезагрузка настроек, bot polling (/start /stop + модерация),
очистка устаревших pending-постов, heartbeat.
"""
import asyncio
import time

import config
from config import (
    SETTINGS_RELOAD_SEC, state, _executor, metrics,
    published_fingerprints, log,
)
from sheets import (
    _safe_sheets, _safe_sheets_retry, _safe_sheets_result,
    _read_settings, _read_scoring_rules, _read_minus_words,
    _read_realtors_raw, _read_channels, _read_bot_subscribers,
    _write_post, _add_realtor_to_sheet,
    _parse_excluded_accounts, _expire_crm_subscriptions,
    _add_crm_comment,
)
from sheets import _resolve_realtors
from channels import _update_watched_chats
from bot_api import (
    _tg_request, _get_updates, _broadcast_to_bot,
)
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

            # Читаем все листы одним батчем внутри одного lock-окна.
            # Раньше было 6 отдельных _safe_sheets_result — каждый захватывал
            # _sheets_lock на 1-3 сек, итого 6-18 сек блокировки для воркеров.
            # Теперь один захват lock на всё чтение (~3-5 сек суммарно).
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

            # Резолв риэлторов — без lock, это Telethon-запросы
            new_realtors = await _resolve_realtors(ss, clients, resolved_r, to_resolve_r)

            if new_settings:
                async with config._state_lock:
                    state.update({
                        'tg_token':             new_settings['tg_token'],
                        'score_threshold':      new_settings['score_threshold'],
                        'moderation_threshold': new_settings['moderation_threshold'],
                        'min_length':           new_settings['min_length'],
                        'moderator_chat_id':    new_settings['moderator_chat_id'],
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
            )
        except Exception as e:
            log.error('Ошибка перезагрузки настроек: ' + str(e), exc_info=True)

# ══════════════════════════════════════════════════════════════════════════════
# Heartbeat
# ══════════════════════════════════════════════════════════════════════════════

async def _heartbeat_loop():
    loop = asyncio.get_event_loop()
    while True:
        await asyncio.sleep(300)
        try:
            qsize = config.post_queue.qsize() if config.post_queue is not None else 0

            log.info(
                f'[heartbeat] processed:{metrics["processed"]} '
                f'published:{metrics["published"]} '
                f'errors:{metrics["errors"]} '
                f'bot_sent:{metrics["bot_sent"]} '
                f'bot_blocked:{metrics["bot_blocked"]} '
                f'subscribers:{len(state["bot_subscribers"])} '
                f'queue:{qsize}'
            )

            async with config._state_lock:
                token     = state.get('tg_token', '')
                moderator = state.get('moderator_chat_id', '')

            if qsize >= config.QUEUE_ALERT_THRESHOLD and not config.queue_alert_sent:
                config.queue_alert_sent = True
                log.warning(f'[heartbeat] ⚠️ очередь постов: {qsize}')
                if token and moderator:
                    await loop.run_in_executor(
                        _executor, config._tg_request_alert, token, moderator, qsize
                    )

            if qsize < config.QUEUE_ALERT_THRESHOLD // 2 and config.queue_alert_sent:
                config.queue_alert_sent = False
                log.info(f'[heartbeat] очередь нормализовалась: {qsize}')
                if token and moderator:
                    await loop.run_in_executor(
                        _executor, config._tg_request_alert_ok, token, moderator, qsize
                    )

        except Exception as e:
            log.error(f'Ошибка heartbeat: {e}', exc_info=True)


# ══════════════════════════════════════════════════════════════════════════════
# Bot polling — /start, /stop + модерация
# ══════════════════════════════════════════════════════════════════════════════

async def _bot_polling_loop(clients: dict, ss):
    offset = 0
    loop   = asyncio.get_event_loop()

    def _first_client():
        return next(iter(clients.values()), None)

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
            moderator = state.get('moderator_chat_id', '')

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
    from sheets import _safe_sheets_result, _safe_sheets_retry
    from sheets import _add_bot_subscriber

    async with config._state_lock:
        already = chat_id in state['bot_subscribers']

    if already:
        sub_info = state['bot_subscribers'].get(chat_id, {})
        city = sub_info.get('city', '')
        if city:
            await loop.run_in_executor(
                _executor, _tg_request, token, 'sendMessage', {
                    'chat_id': chat_id,
                    'text':    f'✅ Вы уже подписаны. Выбранный город: <b>{city}</b>',
                    'parse_mode': 'HTML',
                }
            )
        else:
            await _send_city_selection(loop, token, chat_id)
        return

    added = await _safe_sheets_result(_add_bot_subscriber, ss, chat_id, username)
    if not added:
        return

    trial_end = added.get('trial_end', '') if isinstance(added, dict) else ''

    if token and moderator:
        is_new = isinstance(added, dict) and added.get('is_new')
        tag = '🆕 Новый подписчик' if is_new else '🔄 Повторный /start'
        notify_text = (
            f'{tag}\n\n'
            f'👤 Username: @{username}\n'
            f'🆔 Chat ID: <code>{chat_id}</code>\n'
            f'📅 Триал до: {trial_end}\n'
        )
        await loop.run_in_executor(
            _executor, _tg_request, token, 'sendMessage', {
                'chat_id': moderator,
                'text': notify_text,
                'parse_mode': 'HTML',
            }
        )

    await _safe_sheets(_add_crm_comment, ss, chat_id,
                   f'/start — @{username} подключился к боту')

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
                    {'text': '📍 Батуми',   'callback_data': 'city:Батуми'},
                    {'text': '📍 Тбилиси',  'callback_data': 'city:Тбилиси'},
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
        await _safe_sheets(_add_crm_comment, ss, chat_id, '/stop — пользователь отписался')
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
    cq_id = cq['id']
    data = cq.get('data', '')

    if data.startswith('city:'):
        city = data.split(':', 1)[1].strip()
        await _handle_city_choice(loop, cq, token, city, ss)
        return
    # неизвестная callback — молча игнорируем


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
