"""
Утилиты: парсинг username-ов, построение ссылок, информация об авторе,
конвертация текста в HTML, фильтрация и скоринг.
"""
import re

from telethon import TelegramClient

from config import log


# ══════════════════════════════════════════════════════════════════════════════
# Разбор идентификаторов
# ══════════════════════════════════════════════════════════════════════════════

def _extract_username(raw: str):
    if not raw:
        return None
    m = re.match(r'(?:https?://)?t\.me/([a-zA-Z0-9_]+)', raw)
    if m:
        return m.group(1)
    if raw.startswith('@'):
        return raw[1:]
    if re.match(r'^[a-zA-Z0-9_]+$', raw):
        return raw
    if re.match(r'^-?\d+$', raw):
        return raw
    return None

def _normalize_chat_id(raw_id: int) -> int:
    """Всегда возвращает короткий вариант ID без префикса 100."""
    eid = abs(raw_id)
    s = str(eid)
    if s.startswith('100') and len(s) > 12:
        return int(s[3:])
    return eid

def _all_id_variants(entity_id: int) -> list:
    return [_normalize_chat_id(entity_id)]


def _meta_by_abs_id(id_to_meta: dict, abs_id: int):
    return id_to_meta.get(_normalize_chat_id(abs_id))


# ══════════════════════════════════════════════════════════════════════════════
# Построение ссылок и информация об авторе
# ══════════════════════════════════════════════════════════════════════════════

def _build_link(chat, msg_id: int) -> str:
    username = getattr(chat, 'username', None)
    if username:
        return f'https://t.me/{username}/{msg_id}'
    chat_id = str(chat.id)
    if chat_id.startswith('-100'):
        chat_id = chat_id[4:]
    elif chat_id.startswith('-'):
        chat_id = chat_id[1:]
    return f'https://t.me/c/{chat_id}/{msg_id}'


def _get_author_info(msg):
    try:
        sender = msg.sender
        if not sender:
            return '', '', 0
        first    = getattr(sender, 'first_name', '') or ''
        last     = getattr(sender, 'last_name',  '') or ''
        username = getattr(sender, 'username',   '') or ''
        name     = (first + ' ' + last).strip()
        link     = f'https://t.me/{username}' if username else ''
        user_id  = getattr(sender, 'id', 0) or 0
        return name, link, user_id
    except Exception:
        return '', '', 0


async def _get_sender_bio(client: TelegramClient, msg) -> str:
    try:
        sender = await msg.get_sender()
        if sender is None:
            return ''
        from telethon.tl.functions.users import GetFullUserRequest
        full = await client(GetFullUserRequest(sender))
        return (full.full_user.about or '').strip()
    except Exception:
        return ''


# ══════════════════════════════════════════════════════════════════════════════
# Конвертация текста в HTML (Telegram entities)
# ══════════════════════════════════════════════════════════════════════════════

def _utf16_to_unicode_idx(text: str, utf16_offset: int) -> int:
    idx = 0
    u16 = 0
    for ch in text:
        if u16 >= utf16_offset:
            break
        u16 += 2 if ord(ch) > 0xFFFF else 1
        idx += 1
    return idx


def _text_to_html(text: str, entities) -> str:
    if not text:
        return ''
    escaped = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    if not entities:
        return escaped

    from telethon.tl.types import (
        MessageEntityBold, MessageEntityItalic, MessageEntityUnderline,
        MessageEntityStrike, MessageEntityCode, MessageEntityPre,
        MessageEntityTextUrl, MessageEntityUrl, MessageEntityMention,
    )

    chars         = list(text)
    escaped_chars = list(escaped)
    opens  = {}
    closes = {}

    for ent in sorted(entities, key=lambda e: (e.offset, -e.length)):
        o = _utf16_to_unicode_idx(text, ent.offset)
        c = _utf16_to_unicode_idx(text, ent.offset + ent.length)

        if isinstance(ent, MessageEntityBold):
            tag_o, tag_c = '<b>', '</b>'
        elif isinstance(ent, MessageEntityItalic):
            tag_o, tag_c = '<i>', '</i>'
        elif isinstance(ent, MessageEntityUnderline):
            tag_o, tag_c = '<u>', '</u>'
        elif isinstance(ent, MessageEntityStrike):
            tag_o, tag_c = '<s>', '</s>'
        elif isinstance(ent, MessageEntityCode):
            tag_o, tag_c = '<code>', '</code>'
        elif isinstance(ent, MessageEntityPre):
            tag_o, tag_c = '<pre>', '</pre>'
        elif isinstance(ent, MessageEntityTextUrl):
            url = ent.url.replace('"', '&quot;')
            tag_o, tag_c = f'<a href="{url}">', '</a>'
        elif isinstance(ent, MessageEntityUrl):
            raw_url = ''.join(chars[o:c]).replace('"', '&quot;')
            tag_o, tag_c = f'<a href="{raw_url}">', '</a>'
        elif isinstance(ent, MessageEntityMention):
            mention = ''.join(chars[o:c]).lstrip('@')
            tag_o, tag_c = f'<a href="https://t.me/{mention}">', '</a>'
        else:
            continue

        opens.setdefault(o, []).append(tag_o)
        closes.setdefault(c, []).insert(0, tag_c)

    result = []
    for i in range(len(chars)):
        for tag in closes.get(i, []):
            result.append(tag)
        for tag in opens.get(i, []):
            result.append(tag)
        result.append(escaped_chars[i])
    for tag in closes.get(len(chars), []):
        result.append(tag)

    return ''.join(result)


# ══════════════════════════════════════════════════════════════════════════════
# Фильтрация и скоринг
# ══════════════════════════════════════════════════════════════════════════════

def _find_minus_word(text: str, minus_words: list) -> str | None:
    lower = text.lower()
    for w in minus_words:
        if not w:
            continue
        if len(w) <= 4:
            if re.search(r'(?<![а-яёa-z])' + re.escape(w) + r'(?![а-яёa-z])', lower):
                return w
        else:
            if w in lower:
                return w
    return None


def _calc_score(text: str, rules: list) -> int:
    lower = text.lower()
    total = 0
    for rule in rules:
        for kw in rule['keywords']:
            if kw.endswith('*'):
                if kw[:-1] in lower:
                    total += rule['weight']
                    break
            else:
                if kw in lower:
                    total += rule['weight']
                    break
    return total
