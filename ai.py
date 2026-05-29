"""
AI-модерация через Gemini: промпт, вызов API, маршрутизация решений.
"""
import asyncio
import json
import urllib.request
import urllib.error

from config import GEMINI_API_KEY, state, _executor, log


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

    return f"""Ты модератор доски объявлений по аренде и продаже недвижимости в Батуми (Грузия).

Твоя задача: определить тип поста и маршрут публикации.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ТОЧНО ЧАСТНЫЙ → approve_private

Человек явно ищет жильё для себя — есть хотя бы один личный маркер:

• Описывает себя или свою ситуацию: «мы с женой», «я с семьёй»,
  «без вредных привычек», «не курю», «работаем удалённо»,
  «для себя», «для нас», «переезжаю», «приезжаю», «прилетаю»

• Пишет от первого лица про личные обстоятельства: «работаю в батуми», «учусь здесь»,
  «нас двое», «я один», «живу один»

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ТОЧНО АГЕНТ → approve_agent

Риелтор или агент — есть хотя бы один профессиональный маркер:

• Явный поиск для клиента: «для клиента», «клиенту», «под клиента», «сниму клиенту»,
  «ищу для клиента», «помощник агента», «ассистент риелтора», «работаю с агентом»

• Профессиональная идентификация: «риелтор», «агент», «broker», «estate», «realty»,
  «управляющая компания», «агентство»

• Профессиональные формулировки: «готов к сотрудничеству», «открыт к сотрудничеству»,
  «сотрудничаю с собственниками», «гарантирую быструю сдачу»,
  «работаю по договору с описью», «договор + опись», «сотрудничаю»

• Ищет несколько объектов с разными бюджетами: «1+1 за 600$, 2+1 за 900$»

• В bio упоминается риелторская деятельность: «риелтор», «агент по недвижимости»,
  «помогу с арендой», «подбор недвижимости», «broker», «estate», «realty»

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
НЕЙТРАЛЬНЫЙ или НЕОПРЕДЕЛЁННЫЙ → approve_private

Пост выглядит как запрос жилья, но нет чётких агентских маркеров.
При любом сомнении — approve_private (безопасный выбор).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ОТКЛОНИТЬ → skip

• Объявление о СДАЧЕ или ПРОДАЖЕ: «сдаю», «продаю», «сдаётся», «есть варианты»
• Реклама агентства без конкретного запроса на поиск жилья
• Не связано с недвижимостью: вакансии, услуги, личные вопросы
• Ищет соседа или подселение
• Туристические вопросы без запроса жилья
• Флуд, жалобы, поздравления

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ВАЖНЫЕ ПРАВИЛА:

1. Есть личный маркер → approve_private, даже если пост короткий.
2. Есть агентский маркер → approve_agent (приоритет над личными деталями).
3. Нет ни тех ни других маркеров, но явный запрос жилья → approve_private.
4. «Сдаю», «сдаётся», «продаю» от владельца → skip.
5. Ищут в Батуми и окрестностях (Кабулети, Гонио, Квариати, Сарпи, Махинджаури,
   Букнари, Чакви, Цихисдзири) — одобряй.
6. Запросы на нескольких языках (русский + грузинский + английский) — одобряй.
7. Bio с риелторской деятельностью → approve_agent.

{realtor_block}

Скоринг системы: {score} (выше = больше ключевых слов поиска жилья)

{bio_block}

Отвечай СТРОГО одним словом: approve_private, approve_agent или skip.

Текст сообщения:
{text[:2000]}"""


# ══════════════════════════════════════════════════════════════════════════════
# Вызов Gemini API
# ══════════════════════════════════════════════════════════════════════════════

async def _ai_moderate(text: str, score: int, sender_bio: str = '', is_known_realtor: bool = False) -> str:
    if not GEMINI_API_KEY:
        log.warning('[gemini] GEMINI_API_KEY не задан — fallback: approve_private')
        return 'approve_private'

    prompt = _build_prompt(text, score, sender_bio, is_known_realtor)
    loop   = asyncio.get_event_loop()

    def _call_gemini() -> str:
        url = (
            'https://generativelanguage.googleapis.com/v1beta/models/'
            f'gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}'
        )
        payload = json.dumps({
            'contents': [{'parts': [{'text': prompt}]}],
            'generationConfig': {
                'maxOutputTokens': 10,
                'temperature': 0,
                'thinkingConfig': {'thinkingBudget': 0},
            },
        }).encode('utf-8')
        req = urllib.request.Request(
            url, data=payload,
            headers={'Content-Type': 'application/json'},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data      = json.loads(resp.read().decode('utf-8'))
                candidate = data.get('candidates', [{}])[0]
                parts     = candidate.get('content', {}).get('parts', [])
                answer    = ''
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
                if answer == 'approve':          return 'approve_private'
                if 'skip'            in answer: return 'skip'
                return 'approve_private'
        except urllib.error.HTTPError as e:
            body = ''
            try:
                body = e.read().decode('utf-8')
            except Exception:
                pass
            if e.code == 503:
                raise RuntimeError('GEMINI_503')
            log.warning(f'[gemini] HTTP {e.code}: {body[:200]} — fallback: moderation')
            return 'MODERATION_NEEDED'
        except Exception as e:
            log.warning(f'[gemini] ошибка: {e} — fallback: moderation')
            return 'MODERATION_NEEDED'

    for attempt in range(3):
        try:
            result = await loop.run_in_executor(_executor, _call_gemini)
            return result
        except RuntimeError as e:
            if 'GEMINI_503' in str(e):
                wait = (attempt + 1) * 10
                log.warning(f'[gemini] 503 — sleep {wait}s (попытка {attempt + 1}/3)')
                await asyncio.sleep(wait)
            else:
                raise

    log.warning('[gemini] все попытки исчерпаны → модерация')
    return 'MODERATION_NEEDED'


# ══════════════════════════════════════════════════════════════════════════════
# Выбор канала назначения по решению AI
# ══════════════════════════════════════════════════════════════════════════════

def _pick_dest_chat(ai_decision: str) -> str:
    if ai_decision == 'approve_agent' and state.get('dest_chat_id_agent'):
        return state['dest_chat_id_agent']
    return state['dest_chat_id']
