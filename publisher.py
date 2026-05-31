"""
Публикация постов: скачивание фото, отправка в канал, основная логика обработки.
"""
import asyncio
import io
import time

from telethon import TelegramClient

import config
from config import (
    state, _executor, metrics, pending_moderation,
    published_fingerprints, ai_rejected_fingerprints,
    log,
)
from sheets import (
    _safe_sheets, _safe_sheets_retry,
    _write_post, _write_rejected, _write_ai_rejected, _add_realtor_to_sheet,
    _update_rejected_status,
)
from sheets import _post_fingerprint
from ai import _ai_moderate, _pick_dest_chat
from bot_api import (
    _tg_request, _send_alert, _send_photo_bot, _send_album_bot,
    _build_caption, _send_moderation_card, _broadcast_to_bot,
)
from utils import _get_sender_bio


# ══════════════════════════════════════════════════════════════════════════════
# Скачивание фото из Telegram
# ══════════════════════════════════════════════════════════════════════════════

def _is_image_doc(msg) -> bool:
    doc = getattr(msg, 'document', None)
    if not doc:
        return False
    mime = getattr(doc, 'mime_type', '') or ''
    return mime.startswith('image/')


async def _download_photos(client: TelegramClient, messages: list) -> list[bytes]:
    photos = []
    for m in messages:
        media = m.photo or (m.document if _is_image_doc(m) else None)
        if not media:
            continue
        try:
            buf = io.BytesIO()
            await asyncio.wait_for(client.download_media(m, file=buf), timeout=30)
            photos.append(buf.getvalue())
        except asyncio.TimeoutError:
            log.warning(f'download_media timeout msg_id={m.id}')
        except Exception as e:
            log.warning(f'download_media error msg_id={m.id}: {e}', exc_info=True)
    return photos


async def _fetch_messages_by_refs(client: TelegramClient, refs: list[tuple]) -> list:
    msgs = []
    for chat_id, msg_id in refs:
        try:
            msg = await client.get_messages(chat_id, ids=msg_id)
            if msg:
                msgs.append(msg)
        except Exception as e:
            log.warning(f'Не удалось загрузить сообщение {chat_id}/{msg_id}: {e}', exc_info=True)
    return msgs


# ══════════════════════════════════════════════════════════════════════════════
# Публикация в канал (режим дублирования)
# ══════════════════════════════════════════════════════════════════════════════

async def _publish_to_channel(
    client: TelegramClient,
    post: dict,
    dest_chat: str,
    photos: list[bytes] | None = None,
) -> bool:
    """Публикация в канал (режим дублирования)."""
    caption = _build_caption(post)
    token   = state.get('tg_token', '')
    loop    = asyncio.get_event_loop()
    try:
        if photos and token:
            if len(photos) == 1:
                await loop.run_in_executor(_executor, _send_photo_bot,
                                           token, dest_chat, caption, photos[0])
            else:
                await loop.run_in_executor(_executor, _send_album_bot,
                                           token, dest_chat, caption, photos)
        elif photos and not token:
            await client.send_file(
                entity=int(dest_chat),
                file=photos if len(photos) > 1 else photos[0],
                caption=caption,
                parse_mode='html',
            )
        else:
            await client.send_message(
                entity=int(dest_chat),
                message=caption,
                parse_mode='html',
                link_preview=True,
            )
        return True
    except Exception as e:
        log.error(f'Ошибка публикации в канал: {e}', exc_info=True)
        return False


# ══════════════════════════════════════════════════════════════════════════════
# Финальная публикация
# ══════════════════════════════════════════════════════════════════════════════

async def _do_publish(
    post: dict,
    client: TelegramClient,
    ss,
    acc: str,
    ai_decision: str,
    photos: list[bytes] | None = None,
):
    """
    Финальная публикация:
      1. В канал (если dest_chat_id задан) — режим дублирования
      2. В бот всем подписчикам — всегда
    """
    user_id = post.get('user_id', 0)
    async with config._state_lock:
        realtors = set(state.get('realtors', set()))
    if user_id and user_id in realtors and ai_decision not in ('approve_agent', 'skip'):
        log.info(f'[{acc}][риэлтор из списка] user_id={user_id} → approve_agent')
        ai_decision = 'approve_agent'

    post['ai_decision'] = ai_decision
    fp = _post_fingerprint(post['text'], post['author_name'])

    # 1. Публикация в канал (дублирование)
    target = _pick_dest_chat(ai_decision)
    if target:
        ok = await _publish_to_channel(client, post, target, photos)
        if not ok:
            metrics['errors'] += 1

    # 2. Рассылка в бот
    await _broadcast_to_bot(post, photos or [], ss)

    # 3. Запись в Sheets
    published_fingerprints.append(fp)
    await _safe_sheets_retry(_write_post, ss, post)
    metrics['published'] += 1

    log.info(
        f'[✅ AI:{ai_decision} acc:{acc}] {post["chat_name"]} → {post["link"]} '
        f'| канал: {target or "—"} '
        f'| бот: {len(state["bot_subscribers"])} подписчиков'
    )


# ══════════════════════════════════════════════════════════════════════════════
# Основная функция обработки поста
# ══════════════════════════════════════════════════════════════════════════════

async def _process_and_publish(
    post: dict,
    client: TelegramClient,
    msgs_for_photos: list,
    ss,
    acc: str,
):
    text        = post['text']
    author_name = post['author_name']
    chat_name   = post['chat_name']
    score       = post['score']
    link        = post['link']
    user_id     = post.get('user_id', 0)

    # 1. Дедупликация: уже опубликован
    fp = _post_fingerprint(text, author_name)
    if fp in published_fingerprints:
        log.info(f'[{acc}][дубль опубликован ⛔] {chat_name}')
        return

    # 2. Дедупликация: уже отклонён AI
    fp_anon = _post_fingerprint(text, '')
    if fp_anon in ai_rejected_fingerprints:
        log.info(f'[{acc}][дубль AI-rejected ⛔] {chat_name}')
        return

    # 3. Bio автора
    sender_bio = ''
    try:
        source_msg = msgs_for_photos[0] if msgs_for_photos else None
        if source_msg:
            sender_bio = await _get_sender_bio(client, source_msg)
            if sender_bio:
                log.info(f'[{acc}][bio] {sender_bio[:80]}')
    except Exception as e:
        log.warning(f'[{acc}] Не удалось получить bio: {e}')

    # 4. AI-модерация
    async with config._state_lock:
        realtors = set(state.get('realtors', set()))
    is_known_realtor = bool(user_id and user_id in realtors)
    ai_decision = await _ai_moderate(text, score, sender_bio, is_known_realtor)

    # Проверка списка риэлторов
    if user_id and user_id in realtors and ai_decision != 'skip':
        if ai_decision != 'approve_agent':
            log.info(f'[{acc}][риэлтор из списка] user_id={user_id} '
                     f'{ai_decision} → approve_agent')
        ai_decision = 'approve_agent'

    log.info(f'[{acc}][AI:{ai_decision} скор:{score}] {chat_name} → {link}')
    post['ai_decision'] = ai_decision

    # Если AI впервые определил агента — записываем в Риэлторы
    if ai_decision == 'approve_agent' and user_id and user_id not in realtors:
        await _safe_sheets_retry(_add_realtor_to_sheet, ss, post, user_id)
        async with config._state_lock:
            state['realtors'].add(user_id)
        log.info(f'[{acc}][новый риэлтор записан] user_id={user_id}')

    # 5. skip
    if ai_decision == 'skip':
        ai_rejected_fingerprints.append(fp_anon)
        await _safe_sheets_retry(_write_ai_rejected, ss, post, ai_decision)
        return

    # 6. Скачиваем фото
    photos = await _download_photos(client, msgs_for_photos) if msgs_for_photos else []

    # 7. approve_private / approve_agent
    if ai_decision in ('approve_private', 'approve_agent'):
        await _do_publish(post, client, ss, acc, ai_decision, photos or None)
        return

    # 8. MODERATION_NEEDED (Gemini недоступен)
    if ai_decision == 'MODERATION_NEEDED':
        async with config._state_lock:
            token     = state['tg_token']
            moderator = state['moderator_chat_id']
        if token and moderator:
            pend_key = f'{post["src_chat_id"]}:{post["src_msg_id"]}'
            post['added_at'] = time.time()
            pending_moderation[pend_key] = post
            post['_photos'] = photos
            loop = asyncio.get_event_loop()
            bot_message_id = await loop.run_in_executor(
                _executor, _send_moderation_card, post, token, moderator
            )
            post['bot_message_id'] = bot_message_id
            await _safe_sheets_retry(_write_rejected, ss, post, bot_message_id)
            metrics['moderated'] += 1
            log.info(f'[{acc}][gemini недоступен → модерация] {chat_name} → {link}')
        else:
            log.warning(f'[{acc}][gemini недоступен, модератор не задан → approve_private] {link}')
            await _do_publish(post, client, ss, acc, 'approve_private', photos or None)
        return

    # Неизвестный ответ AI → approve_private
    log.warning(f'[{acc}][AI неизвестный ответ: {ai_decision}] → approve_private')
    await _do_publish(post, client, ss, acc, 'approve_private', photos or None)
