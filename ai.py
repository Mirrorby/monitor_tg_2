"""
AI-модерация через Gemini: промпт, вызов API, маршрутизация решений.
Используем aiohttp вместо urllib — не блокируем тред-пул.
"""

import asyncio
import json
import aiohttp

from config import GEMINI_API_KEY, state, log

# ══════════════════════════════════════════════════════════════════════════════
# Промпт
# ══════════════════════════════════════════════════════════════════════════════

def _build_prompt(text: str, score: int, sender_bio: str, is_known_realtor: bool = False) -> str:
    bio_block = (
        f'Описание аккаунта автора (bio):\n{sender_bio[:500]}'
        if sender_bio else
        'Описание аккаунта автора (bio): не указано'
    )
    realtor_block = (
        '⚠️ ПРИМЕЧАНИЕ: этот пользователь ранее был определён как риэлтор/агент. '
        'Если пост — запрос жилья для клиента → approve_agent. '
        'Если пост — объявление о сдаче/продаже → skip (как обычно).'
        if is_known_realtor else ''
    )
    return f"""Ты модератор доски объявлений по аренде и продаже недвижимости.
Твоя задача: определить тип поста и маршрут публикации.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ТОЧНО ЧАСТНЫЙ → approve_private
Человек ищет жильё для себя. Достаточно ЛЮБОГО из признаков:
- Явный запрос аренды: «сниму», «ищу квартиру», «нужна квартира», «ищу комнату»,
  «нужно жильё», «ищу студию», «ищу апартаменты»
- Вопрос в сторону тех, кто сдаёт: «может кто сдает», «кто сдаёт?», «сдаёте?»,
  «кто сдаёт студию», «есть кто сдаёт квартиру» — это ВОПРОС/ПОИСК со стороны
  арендатора, а НЕ объявление о сдаче → approve_private
- Конкретные условия поиска: бюджет («бюджет 300$», «до 500 лари»),
  даты («с 10 июня», «на 2 недели», «с завтрашнего дня», «посуточно»),
  район («в центре», «на бульваре», «рядом с морем»)
- Личные маркеры: «мы с женой», «я с семьёй», «без вредных привычек»,
  «не курю», «работаем удалённо», «для себя», «переезжаю», «прилетаю»
- Ищет соседа для совместной аренды: «ищу соседку», «ищу соседа»,
  «сниму комнату», «ищу комнату» — это тоже запрос жилья → approve_private
- Ищет квартиру/студию/помещение под бизнес или личное использование
  («под кабинет косметолога», «под массажный кабинет», «под мастерскую»,
  «под офис») — это всё равно ЗАПРОС АРЕНДЫ ЖИЛОГО ПОМЕЩЕНИЯ
  → approve_private, если в тексте нет признаков объявления о сдаче/продаже
  от собственника

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ТОЧНО АГЕНТ → approve_agent
Риелтор или агент — есть хотя бы один профессиональный маркер:
- Явный поиск для клиента: «для клиента», «клиенту», «под клиента»,
  «ищу для клиента», «помощник агента», «ассистент риелтора»
- Профессиональная идентификация: «риелтор», «агент», «broker», «estate», «realty»,
  «управляющая компания», «агентство»
- Профессиональные формулировки: «готов к сотрудничеству»,
  «сотрудничаю с собственниками», «гарантирую быструю сдачу»,
  «работаю по договору с описью», «договор + опись»
- Ищет несколько объектов с разными бюджетами одновременно
- В bio упоминается риелторская деятельность
{realtor_block}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ОТКЛОНИТЬ → skip
- Объявление о СДАЧЕ или ПРОДАЖЕ от собственника/владельца: «сдаю», «продаю»,
  «сдаётся», «for rent», «есть варианты», «предлагаю квартиру», «в наличии»
  (ВАЖНО: «сдаю» от первого лица = объявление → skip. Но «кто сдаёт?»,
  «может кто сдает» = вопрос ищущего жильё → approve_private, см. выше)
- Реклама агентства без конкретного запроса жилья
- НЕ про недвижимость: вакансии, скутеры, велосипеды, животные, уборка,
  крипта, инвестиции, знакомства, любые услуги не связанные с жильём
  (но запрос самого ПОМЕЩЕНИЯ под услугу/бизнес — это запрос жилья,
  см. правило про «под кабинет/офис» выше)
- Туристические вопросы без запроса жилья
- Флуд, жалобы, поздравления, вопросы не по теме

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ВАЖНЫЕ ПРАВИЛА:
1. Короткий запрос без личных деталей, но явно ищет жильё → approve_private.
   Пример: «Сниму посуточно квартиру бюджет 40 лари» → approve_private.
2. Есть агентский маркер → approve_agent (приоритет над личными деталями).
3. «Сдаю», «сдаётся», «продаю» от владельца (от первого лица, как объявление)
   → skip. А вопрос «кто сдаёт?», «может кто сдает» (от лица ищущего)
   → approve_private.
4. Запросы на нескольких языках (русский + грузинский + английский) — одобряй.
5. Пост не про недвижимость вообще → skip независимо от скора.
   Но если ищут ПОМЕЩЕНИЕ/КВАРТИРУ под бизнес (кабинет, офис, мастерская
   и т.п.) — это запрос аренды жилья → approve_private, не skip.
6. При любом сомнении между approve_private и skip → approve_private.

Скоринг системы: {score} (выше = больше ключевых слов поиска жилья)
{bio_block}

Отвечай СТРОГО одним словом: approve_private, approve_agent или skip.

Текст сообщения:
{text[:2000]}"""
    
# ══════════════════════════════════════════════════════════════════════════════
# Вызов Gemini API (async, не блокирует тред-пул)
# ══════════════════════════════════════════════════════════════════════════════

_GEMINI_URL_TEMPLATE = (
    'https://generativelanguage.googleapis.com/v1beta/models/'
    'gemini-2.5-flash:generateContent?key={key}'
)

# Один aiohttp.ClientSession на весь процесс — создаётся при первом вызове
_aiohttp_session: aiohttp.ClientSession | None = None


def _get_session() -> aiohttp.ClientSession:
    global _aiohttp_session
    if _aiohttp_session is None or _aiohttp_session.closed:
        timeout = aiohttp.ClientTimeout(total=20)
        _aiohttp_session = aiohttp.ClientSession(timeout=timeout)
    return _aiohttp_session


async def _call_gemini_async(prompt: str) -> str:
    """Один HTTP-запрос к Gemini. Возвращает решение или бросает исключение."""
    url = _GEMINI_URL_TEMPLATE.format(key=GEMINI_API_KEY)
    payload = {
        'contents': [{'parts': [{'text': prompt}]}],
        'generationConfig': {
            'maxOutputTokens': 10,
            'temperature': 0,
            'thinkingConfig': {'thinkingBudget': 0},
        },
    }
    session = _get_session()
    async with session.post(url, json=payload) as resp:
        if resp.status == 503:
            raise RuntimeError('GEMINI_503')
        if resp.status != 200:
            body = await resp.text()
            log.warning(f'[gemini] HTTP {resp.status}: {body[:200]} — fallback: moderation')
            return 'approve_private'
        data = await resp.json(content_type=None)

    candidate = data.get('candidates', [{}])[0]
    parts = candidate.get('content', {}).get('parts', [])
    answer = ''
    for part in parts:
        if part.get('type') == 'thought':
            continue
        text_val = part.get('text', '')
        if text_val:
            answer = text_val.strip().lower()
            break

    log.debug(f'[gemini] raw answer: {repr(answer)}')
    if 'approve_agent'   in answer: return 'approve_agent'
    if 'approve_private' in answer: return 'approve_private'
    if answer == 'approve':         return 'approve_private'
    if 'skip'            in answer: return 'skip'
    return 'approve_private'


async def _ai_moderate(
    text: str, score: int, sender_bio: str = '', is_known_realtor: bool = False
) -> str:
    if not GEMINI_API_KEY:
        log.warning('[gemini] GEMINI_API_KEY не задан — fallback: approve_private')
        return 'approve_private'

    prompt = _build_prompt(text, score, sender_bio, is_known_realtor)

    for attempt in range(3):
        try:
            return await _call_gemini_async(prompt)
        except RuntimeError as e:
            if 'GEMINI_503' in str(e):
                wait = (attempt + 1) * 10
                log.warning(f'[gemini] 503 — sleep {wait}s (попытка {attempt + 1}/3)')
                await asyncio.sleep(wait)
            else:
                log.warning(f'[gemini] RuntimeError: {e} — fallback: moderation')
                return 'approve_private'
        except asyncio.TimeoutError:
            wait = (attempt + 1) * 5
            log.warning(f'[gemini] timeout — sleep {wait}s (попытка {attempt + 1}/3)')
            await asyncio.sleep(wait)
        except Exception as e:
            log.warning(f'[gemini] ошибка: {e} — fallback: moderation')
            return 'approve_private'

    log.warning('[gemini] все попытки исчерпаны → approve_private')
    return 'approve_private'


# ══════════════════════════════════════════════════════════════════════════════
# Выбор канала назначения по решению AI
# ══════════════════════════════════════════════════════════════════════════════

def _pick_dest_chat(ai_decision: str) -> str:
    if ai_decision == 'approve_agent' and state.get('dest_chat_id_agent'):
        return state['dest_chat_id_agent']
    return state['dest_chat_id']
