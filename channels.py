"""
Управление каналами: резолв entity, обновление watched_ids.
"""
import asyncio
import re
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError, UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.types import PeerChannel
from config import state, _executor, log
import config
from sheets import (
    _safe_sheets, _safe_sheets_retry, _write_entity_cache,
    _write_log, _mark_channel_unavailable,
from bot_api import _send_alert
from utils import _all_id_variants, _extract_username, _normalize_chat_id


async def _resolve_entity(clients: dict, username: str, ss) -> dict | None:
    errors = {}
    final_status = 'Недоступен'

    m = re.match(r'^-?100(\d+)$', str(username))
    if m:
        peer = PeerChannel(int(m.group(1)))
    else:
        peer = username

    for acc_name, client in clients.items():
        try:
            entity    = await client.get_entity(peer)
            eid       = _normalize_chat_id(abs(entity.id))
            chat_name = getattr(entity, 'title', None) or username
            log.info(f'Резолв [{acc_name}]: {username} → {eid} ({chat_name})')
            return {'entity_id': eid, 'chat_name': chat_name, 'username': username}

        except FloodWaitError as e:
            log.warning(f'[{acc_name}] FloodWait при резолве {username}: жду {e.seconds}s')
            await asyncio.sleep(e.seconds + 2)
            try:
                entity    = await client.get_entity(peer)
                eid       = _normalize_chat_id(abs(entity.id))
                chat_name = getattr(entity, 'title', None) or username
                return {'entity_id': eid, 'chat_name': chat_name, 'username': username}
            except FloodWaitError:
                errors[acc_name] = f'FloodWait повторный'
                final_status = 'Флуд (временно)'
            except ChannelPrivateError:
                errors[acc_name] = 'Канал приватный / аккаунт кикнут'
                final_status = 'Аккаунт кикнут'
            except (UsernameNotOccupiedError, UsernameInvalidError) as e2:
                errors[acc_name] = str(e2)
                final_status = 'Username изменился'
            except Exception as e2:
                errors[acc_name] = str(e2)
                final_status = 'Ошибка сети'

        except ChannelPrivateError as e:
            errors[acc_name] = str(e)
            final_status = 'Аккаунт кикнут'
            log.warning(f'[{acc_name}] Канал приватный / кикнут {username}: {e}')

        except (UsernameNotOccupiedError, UsernameInvalidError) as e:
            errors[acc_name] = str(e)
            final_status = 'Username изменился'
            log.warning(f'[{acc_name}] Username не найден {username}: {e}')

        except Exception as e:
            errors[acc_name] = str(e)
            final_status = 'Ошибка сети'
            log.error(f'[{acc_name}] Ошибка резолва {username}: {e}', exc_info=True)

        await asyncio.sleep(0.5)

    # Все аккаунты не смогли резолвнуть — пишем статус и алертим
    msg = (
        f'🚫 Канал недоступен ({final_status}): {username}\n' +
        '\n'.join(f'{k}: {v}' for k, v in errors.items())
    )
    log.error(msg)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _send_alert,
                               state['tg_token'], state['moderator_chat_id'], msg)
    await _safe_sheets(_write_log, ss, 'ERROR', msg)
    await _safe_sheets_retry(_mark_channel_unavailable, ss, username, final_status)
    return None
    
async def _update_watched_chats(clients: dict, channels: list, ss):
    new_ids     = set()
    new_id_meta = {}

    for ch in channels:
        username = ch['username']
        cached   = state['username_to_meta'].get(username)
        if cached and 'entity_id' in cached:
            cached['city']  = ch.get('city', '')
            cached['theme'] = ch.get('theme', '')
            eid = cached['entity_id']
            for vid in _all_id_variants(eid):
                new_ids.add(vid)
                new_id_meta[vid] = cached
            continue

        meta = await _resolve_entity(clients, username, ss)
        if meta:
            meta['city']  = ch.get('city', '')
            meta['theme'] = ch.get('theme', '')
            eid = meta['entity_id']
            for vid in _all_id_variants(eid):
                new_ids.add(vid)
                new_id_meta[vid] = meta
            state['username_to_meta'][username] = meta
        await asyncio.sleep(0.8)

    new_city_map = {}
    for ch in channels:
        uname = ch.get('username', '')
        city  = ch.get('city', '')
        if uname and city:
            new_city_map[uname] = city

    async with config._state_lock:
        # Мёржим со старыми данными — новые перезаписывают старые,
        # но каналы у которых резолв упал остаются с прошлого раза
        merged_ids  = state['watched_ids'] | new_ids
        merged_meta = {**state['id_to_meta'], **new_id_meta}

        added   = new_ids - state['watched_ids']
        removed = state['watched_ids'] - new_ids  # информационно, реально не удаляем

        state['watched_ids']      = merged_ids
        state['id_to_meta']       = merged_meta
        state['channel_city_map'] = new_city_map

    log.info(f'Каналов в watched_ids: {len(merged_ids)}')
    if added:   log.info(f'Добавлено ID-ключей: {len(added)}')
    if removed: log.info(f'Убрано ID-ключей (в Sheets, но резолв не прошёл или удалены): {len(removed)}')

    await _safe_sheets(_write_entity_cache, ss)
