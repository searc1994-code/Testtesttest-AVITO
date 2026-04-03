import hashlib
import json
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import common
import background_jobs
from safe_logs import log_event

# Эти ссылки и пути tenant_manager обновляет при переключении кабинета.
PRIVATE_DIR = common.PRIVATE_DIR
QUESTION_SNAPSHOT_FILE = common.QUESTION_SNAPSHOT_FILE
QUESTION_DRAFTS_FILE = common.QUESTION_DRAFTS_FILE
QUESTION_QUEUE_FILE = common.QUESTION_QUEUE_FILE
QUESTION_ARCHIVE_FILE = common.QUESTION_ARCHIVE_FILE
QUESTION_CLUSTERS_FILE = common.QUESTION_CLUSTERS_FILE
QUESTION_SYNC_META_FILE = common.QUESTION_SYNC_META_FILE
QUESTION_IGNORED_FILE = common.QUESTION_IGNORED_FILE

QUESTION_SNAPSHOT_TTL_SECONDS = int(getattr(common, "QUESTION_SNAPSHOT_TTL_SECONDS", 120))
QUESTION_RAW_BATCH_SIZE = int(getattr(common, "QUESTION_RAW_BATCH_SIZE", 300))
QUESTION_MAX_RAW_BATCHES = int(getattr(common, "QUESTION_MAX_RAW_BATCHES", 34))
QUESTION_API_SEND_DELAY_SECONDS = float(getattr(common, "QUESTION_API_SEND_DELAY_SECONDS", 0.45))
QUESTION_MAX_REPLY_LENGTH = int(getattr(common, "QUESTION_MAX_REPLY_LENGTH", 5000))
QUESTION_MIN_REPLY_LENGTH = 2
QUESTION_DETAIL_TARGET_LENGTH = int(getattr(common, "QUESTION_DETAIL_TARGET_LENGTH", 420))
QUESTION_MANAGER_COMMENT_LIMIT = int(getattr(common, "QUESTION_MANAGER_COMMENT_LIMIT", 4000))
QUESTION_MANAGER_COMMENT_PREVIEW_LIMIT = int(getattr(common, "QUESTION_MANAGER_COMMENT_PREVIEW_LIMIT", 240))
QUESTION_PAGE_SIZE_OPTIONS = [20, 50, 100, 300]
QUESTION_PROCESSED_STATUSES = {"sent", "rejected", "submitted"}
QUESTION_QUEUE_OPEN_STATUSES = {"queued", "processing", "failed"}
QUESTION_IMPORTED_ASSIGNMENT_SOURCE = "import_cluster_map"


def _log(event: str, level: str = "info", **data: Any) -> None:
    try:
        payload = dict(data)
        payload.setdefault('job_id', background_jobs.current_job_id())
        if payload.get('job_id') and not payload.get('run_id'):
            payload['run_id'] = payload.get('job_id')
        log_event("questions", event, tenant_id=getattr(common, "ACTIVE_TENANT_ID", ""), level=level, **payload)
    except Exception:
        pass

_STOPWORDS = {
    "и", "в", "во", "не", "что", "он", "на", "я", "с", "со", "как", "а", "то", "все", "она", "так",
    "его", "но", "да", "ты", "к", "у", "же", "вы", "за", "бы", "по", "ее", "мне", "было", "вот",
    "от", "меня", "еще", "нет", "о", "из", "ему", "теперь", "когда", "даже", "ну", "вдруг", "ли",
    "если", "уже", "или", "ни", "быть", "был", "него", "до", "вас", "нибудь", "опять", "уж", "вам",
    "ведь", "там", "потом", "себя", "ничего", "ей", "может", "они", "тут", "где", "есть", "надо",
    "ней", "для", "мы", "тебя", "их", "чем", "была", "сам", "чтоб", "без", "будто", "чего", "раз",
    "тоже", "себе", "под", "будет", "ж", "тогда", "кто", "этот", "того", "потому", "этого", "какой",
    "совсем", "ним", "здесь", "этом", "один", "почти", "мой", "тем", "чтобы", "нее", "сейчас", "были",
    "куда", "зачем", "всех", "никогда", "можно", "при", "наконец", "два", "об", "другой", "хоть", "после",
    "над", "больше", "тот", "через", "эти", "нас", "про", "них", "какая", "много", "разве", "три",
    "эту", "моя", "впрочем", "хорошо", "свою", "этой", "перед", "иногда", "лучше", "чуть", "том", "нельзя",
    "такой", "им", "более", "всегда", "конечно", "всю", "между", "либо", "это", "этот", "эта", "эти",
    "товар", "вопрос", "пожалуйста", "спасибо", "скажите", "подскажите", "можно", "ли", "есть", "будет",
}

_INTENT_PATTERNS: List[Dict[str, Any]] = [
    {
        "key": "dimensions",
        "title": "Размеры и габариты",
        "keywords": ["размер", "размеры", "высот", "диаметр", "ширин", "длин", "глубин", "толщин", "объем", "объём", "вес", "габарит", "см", "мм"],
        "fact_sensitive": True,
    },
    {
        "key": "material",
        "title": "Материал и состав",
        "keywords": ["материал", "состав", "из чего", "стекл", "пластик", "дерев", "металл", "хлоп", "ткан", "силикон", "керамик"],
        "fact_sensitive": True,
    },
    {
        "key": "kit",
        "title": "Комплектация",
        "keywords": ["комплект", "комплектац", "что входит", "в набор", "в комплект", "сколько штук", "сколько предметов", "набор"],
        "fact_sensitive": True,
    },
    {
        "key": "care",
        "title": "Уход и чистка",
        "keywords": ["ухаж", "уход", "мыть", "чист", "стирать", "сушить", "гладить", "уходить"],
        "fact_sensitive": False,
    },
    {
        "key": "activation",
        "title": "Активация и использование",
        "keywords": ["активир", "включ", "настро", "использ", "собрат", "установ", "подключ"],
        "fact_sensitive": False,
    },
    {
        "key": "compatibility",
        "title": "Совместимость и назначение",
        "keywords": ["подойдет", "подойдёт", "совмест", "подходит", "для айфон", "для iphone", "для samsung", "для кого", "для чего"],
        "fact_sensitive": True,
    },
    {
        "key": "delivery",
        "title": "Сроки доставки",
        "keywords": ["когда достав", "когда привез", "срок достав", "доставят", "привезут", "приедет", "доставка"],
        "fact_sensitive": False,
    },
    {
        "key": "availability",
        "title": "Наличие и поступление",
        "keywords": ["когда будет", "появ", "в наличии", "наличие", "поступлен", "будет ли", "ожидается"],
        "fact_sensitive": False,
    },
    {
        "key": "discount",
        "title": "Скидки и цена",
        "keywords": ["скидк", "дешев", "дешевле", "цена", "уценк", "акция"],
        "fact_sensitive": False,
    },
    {
        "key": "appearance_issue",
        "title": "Внешний вид и состояние",
        "keywords": ["мутн", "грязн", "царап", "трещ", "пятн", "скол", "крив", "ржав", "помят"],
        "fact_sensitive": True,
    },
    {
        "key": "other",
        "title": "Прочие вопросы",
        "keywords": [],
        "fact_sensitive": False,
    },
]

_INTENT_BY_KEY = {item["key"]: item for item in _INTENT_PATTERNS}

_DEFAULT_FALLBACKS = {
    "dimensions": "Спасибо за вопрос. Актуальные размеры, габариты и другие параметры товара указаны в карточке товара в описании и характеристиках. Если для вас важен конкретный размер или пропорция, рекомендуем ориентироваться именно на текущие данные карточки, так как они являются актуальными для заказа.",
    "material": "Спасибо за вопрос. Актуальные материалы, состав и основные характеристики товара указаны в карточке товара. Рекомендуем ориентироваться на описание и блок характеристик, так как именно там размещена актуальная информация по данной позиции.",
    "kit": "Спасибо за вопрос. Актуальная комплектация указана в карточке товара на текущий момент. Перед оформлением заказа рекомендуем свериться с описанием и характеристиками, так как именно они отражают состав комплекта для текущей поставки.",
    "compatibility": "Спасибо за вопрос. Актуальные характеристики товара и его назначение указаны в карточке товара. Если для вас важна совместимость с конкретной моделью или сценарием использования, лучше ориентироваться на текущие характеристики, размещённые в карточке.",
    "delivery": "Спасибо за вопрос. Актуальный срок доставки рассчитывается при оформлении заказа и зависит от вашего региона, склада и выбранного способа получения. Точную дату система показывает непосредственно перед подтверждением заказа.",
    "availability": "Спасибо за вопрос. Актуальное наличие товара, вариантов и комплектующих отображается в карточке товара в момент оформления заказа. Если нужный вариант доступен к выбору, значит его уже можно оформить; если вариант не отображается, значит на текущий момент он недоступен.",
    "discount": "Спасибо за вопрос. Актуальная цена и действующие скидки отображаются в карточке товара на текущий момент. Стоимость может меняться динамически, поэтому рекомендуем ориентироваться на цену, которую система показывает непосредственно перед оформлением заказа.",
    "default": "Спасибо за вопрос. Актуальные характеристики, описание и доступные варианты указаны в карточке товара на текущий момент. Рекомендуем ориентироваться именно на текущие данные карточки, так как они являются актуальными для оформления заказа.",
}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return default



def _clean(value: Any) -> str:
    return common.clean_text(value)



def _clean_lines(value: Any) -> str:
    return common.clean_text_preserve_lines(value)



def _normalize_question_action(value: Any) -> str:
    action = _clean(value).lower()
    if action in {"reject", "skip"}:
        return action
    return "answer"


def _normalize_search_text(value: Any) -> str:
    text = _clean_lines(value).lower().replace("ё", "е")
    text = re.sub(r"[^0-9a-zа-я]+", " ", text, flags=re.I)
    return re.sub(r"\s+", " ", text).strip()



def _tokenize(text: Any) -> List[str]:
    source = _normalize_search_text(text)
    tokens = [tok for tok in source.split() if tok and tok not in _STOPWORDS and len(tok) > 1]
    return tokens


def normalize_question_for_clustering(value: Any) -> str:
    text = _normalize_search_text(value)
    if not text:
        return ""
    text = re.sub(
        r"^(?:здравствуй(?:те)?|добрый день|добрый вечер|доброе утро|подскажите|скажите|пожалуйста|добрый|а|и|ну)\s+",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_supplier_article_for_group(value: Any) -> str:
    text = _clean_lines(value).lower().replace("ё", "е")
    if not text:
        return ""
    text = re.sub(r"(?<=[a-zа-я])(?=\d)|(?<=\d)(?=[a-zа-я])", " ", text, flags=re.I)
    text = re.sub(r"[_\-/]+", " ", text)
    text = re.sub(r"\d+", " ", text)
    text = re.sub(r"[^0-9a-zа-я]+", " ", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _meaningful_name_hint(value: Any, limit: int = 6) -> str:
    tokens = _tokenize(value)
    if not tokens:
        return ""
    seen: set[str] = set()
    result: List[str] = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        result.append(token)
        if len(result) >= limit:
            break
    return " ".join(result)


def build_product_group_hint(question: Dict[str, Any]) -> str:
    question = common.normalize_question(question)
    product = question.get("productDetails", {}) or {}
    product_name_hint = _meaningful_name_hint(product.get("productName"))
    article_hint = normalize_supplier_article_for_group(product.get("supplierArticle"))
    subject_hint = _meaningful_name_hint(question.get("subjectName"))
    for candidate in [product_name_hint, article_hint, subject_hint]:
        if candidate:
            return candidate
    return _clean(product.get("supplierArticle")).lower()


def build_question_clustering_export_row(
    question: Dict[str, Any],
    tenant_id: str = "",
    tenant_name: str = "",
    snapshot_fetched_at: Any = "",
) -> Dict[str, Any]:
    question = common.normalize_question(question)
    product = question.get("productDetails", {}) or {}
    question_id = _clean(question.get("id"))
    supplier_article_raw = _clean(product.get("supplierArticle"))
    return {
        "tenant_id": _clean(tenant_id),
        "tenant_name": _clean(tenant_name),
        "tenant_question_key": f"{_clean(tenant_id)}::{question_id}" if _clean(tenant_id) else question_id,
        "question_id": question_id,
        "created_at": _clean(question.get("createdDate")),
        "current_status": "unanswered",
        "question_text": _clean_lines(question.get("text")),
        "normalized_question": normalize_question_for_clustering(question.get("text")),
        "product_group_hint": build_product_group_hint(question),
        "article_group_hint": normalize_supplier_article_for_group(supplier_article_raw),
        "product_name": _clean(product.get("productName")),
        "supplier_article_raw": supplier_article_raw,
        "supplier_article_norm": _normalize_search_text(supplier_article_raw),
        "nm_id": _safe_int(product.get("nmId")),
        "brand_name": _clean(product.get("brandName")),
        "subject_name": _clean(question.get("subjectName")),
        "size": _clean(product.get("size")),
        "wb_state": _clean(question.get("state")),
        "was_viewed": bool(question.get("wasViewed")),
        "is_warned": bool(question.get("isWarned")),
        "snapshot_fetched_at": _clean(snapshot_fetched_at),
    }



def _text_signature(text: str) -> str:
    return hashlib.sha256(_clean_lines(text).encode("utf-8")).hexdigest()



def _rules_signature(rules: Dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(rules, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()



def _parse_created_date(value: Any) -> datetime:
    text = _clean(value)
    if not text:
        return datetime.min.replace(tzinfo=timezone.utc)
    for candidate in [text, text.replace("Z", "+00:00")]:
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return datetime.min.replace(tzinfo=timezone.utc)



def _snapshot_is_fresh(snapshot: Dict[str, Any]) -> bool:
    fetched_at = _parse_created_date(snapshot.get("fetched_at"))
    age = datetime.now(timezone.utc) - fetched_at
    return age.total_seconds() <= QUESTION_SNAPSHOT_TTL_SECONDS



def question_signature(question: Dict[str, Any]) -> str:
    question = common.normalize_question(question)
    payload = {
        "id": _clean(question.get("id")),
        "text": _clean(question.get("text")),
        "createdDate": _clean(question.get("createdDate")),
        "state": _clean(question.get("state")),
        "answer": _clean((question.get("answer") or {}).get("text")),
        "product": _clean(question.get("productDetails", {}).get("productName")),
        "article": _clean(question.get("productDetails", {}).get("supplierArticle")),
        "nmId": _safe_int(question.get("productDetails", {}).get("nmId")),
    }
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()



def trim_question_reply(text: str, limit: int = QUESTION_MAX_REPLY_LENGTH) -> str:
    text = _clean_lines(text)
    if len(text) <= limit:
        return text
    chunks = re.split(r"(?<=[.!?])\s+", text)
    result = ""
    for chunk in chunks:
        candidate = f"{result} {chunk}".strip()
        if len(candidate) > limit:
            break
        result = candidate
    if result:
        return result
    return text[: limit - 1].rstrip() + "…"



def _trim_first_sentence(text: str) -> str:
    text = _clean_lines(text)
    if not text:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", text, maxsplit=1)
    return parts[0].strip()


def _strip_leading_courtesy(text: str) -> str:
    text = _clean_lines(text)
    if not text:
        return ""
    return re.sub(r"^(?:спасибо[^.?!]*[.?!]\s*)+", "", text, flags=re.I).strip()


def load_question_ignored_ids() -> set[str]:
    data = common.read_json(QUESTION_IGNORED_FILE, [])
    if isinstance(data, dict):
        data = data.get("ids") or []
    if not isinstance(data, list):
        data = []
    return {_clean(item) for item in data if _clean(item)}


def save_question_ignored_ids(ids: Iterable[str]) -> None:
    ordered = sorted({_clean(item) for item in ids if _clean(item)})
    common.write_json(QUESTION_IGNORED_FILE, ordered)


def _simplify_question_text(question_text: str) -> str:
    text = _clean_lines(question_text)
    if not text:
        return ""
    text = re.sub(r"^(здравствуйте|добрый день|добрый вечер|доброе утро)\s*[!,.:-]*\s*", "", text, flags=re.I)
    text = re.sub(r"^(подскажите(?:\s+пожалуйста)?|скажите(?:\s+пожалуйста)?|а|и)\s+", "", text, flags=re.I)
    return text.strip(" ?!.,")


def _extract_subject_fragment(question_text: str) -> str:
    original = _simplify_question_text(question_text)
    if not original:
        return ""
    lower = original.lower().replace("ё", "е")
    patterns = [
        r"(.+?)\s+(?:тоже\s+)?ид[её]т$",
        r"(.+?)\s+входит(?:\s+в\s+комплект)?$",
        r"есть\s+ли\s+(.+)$",
        r"будет\s+ли\s+(.+)$",
        r"(.+?)\s+в наличии$",
        r"какой\s+(?:способ|метод)\s+(.+)$",
        r"какая\s+(.+)$",
        r"какой\s+(.+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, lower, flags=re.I)
        if match:
            subject = match.group(1).strip(" ?!.,")
            subject = re.sub(r"^(это|этот|эта|эти)\s+", "", subject, flags=re.I)
            return subject
    return ""


def _to_masculine_adjective(word: str) -> str:
    token = _clean(word)
    low = token.lower()
    if low.endswith("ая") and len(token) > 2:
        return token[:-2] + "ый"
    if low.endswith("яя") and len(token) > 2:
        return token[:-2] + "ий"
    return token


def _manager_comment_seed(question: Dict[str, Any], manager_comment: str, cluster_key: str) -> str:
    comment = trim_question_reply(_clean_lines(manager_comment), QUESTION_MANAGER_COMMENT_LIMIT)
    if not comment:
        return ""
    comment_norm = _normalize_search_text(comment)
    question_text = _clean_lines(question.get("text"))
    question_norm = _normalize_search_text(question_text)
    subject = _extract_subject_fragment(question_text) or "это"

    def with_period(value: str) -> str:
        value = _clean_lines(value)
        if not value:
            return ""
        if value[-1] not in ".!?":
            value += "."
        return value

    if comment_norm.startswith("да"):
        if any(token in question_norm for token in ["курьер", "достав", "пвз", "пункт выдачи"]):
            return with_period("Да, если для вашего заказа доступен этот способ, получить его можно через курьера Wildberries или в пункте выдачи Wildberries")
        if any(token in question_norm for token in ["идет", "идет", "идёт", "входит", "в комплект", "комплект"]):
            return with_period(f"Да, {subject} входит в комплект")
        if "в наличии" in question_norm or "налич" in question_norm:
            return with_period(f"Да, {subject} есть в наличии")
        if any(token in question_norm for token in ["можно", "подойдет", "подойдёт", "подходит"]):
            return with_period("Да, это возможно")
        return with_period("Да, это предусмотрено")

    if comment_norm.startswith("нет"):
        if any(token in question_norm for token in ["курьер", "достав", "пвз", "пункт выдачи"]):
            return with_period("Нет, ориентироваться нужно на варианты получения, которые Wildberries показывает при оформлении заказа")
        if any(token in question_norm for token in ["идет", "идет", "идёт", "входит", "в комплект", "комплект"]):
            return with_period(f"Нет, {subject} не входит в комплект")
        if "в наличии" in question_norm or "налич" in question_norm:
            return with_period(f"Нет, {subject} сейчас нет в наличии")
        if any(token in question_norm for token in ["можно", "подойдет", "подойдёт", "подходит"]):
            return with_period("Нет, это не предусмотрено")
        return with_period("Нет, это не предусмотрено")

    if "глицерин" in comment_norm and "стабилизац" in question_norm:
        first = comment.split()[0]
        return with_period(f"Для данного товара используется {_to_masculine_adjective(first).lower()} способ стабилизации")

    if "скоро" in comment_norm:
        return with_period("Поступление ожидается, а актуальное наличие и дата доставки отображаются на Wildberries при оформлении заказа")

    if "ждите" in comment_norm:
        return with_period("Рекомендуем ориентироваться на карточку товара на Wildberries и дату, которую система показывает при оформлении заказа")

    if "карточк" in comment_norm:
        return with_period("Актуальная информация по этому вопросу указана в карточке товара на Wildberries")

    if "комплект" in comment_norm and any(token in question_norm for token in ["идет", "идёт", "входит", "комплект"]):
        return with_period(f"{subject.capitalize()} входит в комплект")

    short_words = comment.split()
    if len(short_words) <= 3:
        if "материал" in question_norm or "состав" in question_norm or "из чего" in question_norm:
            return with_period(f"Материал товара — {comment}")
        if "высот" in question_norm:
            return with_period(f"Высота товара — {comment}")
        if "диаметр" in question_norm:
            return with_period(f"Диаметр товара — {comment}")
        if "размер" in question_norm:
            return with_period(f"Размер товара — {comment}")
        if "способ" in question_norm or "метод" in question_norm:
            return with_period(f"Для данного товара используется {comment.lower()} способ")

    first_sentence = _trim_first_sentence(comment)
    return with_period(first_sentence)


def _answer_matches_manager_comment(answer_text: str, manager_comment: str) -> bool:
    comment = _normalize_search_text(manager_comment)
    if not comment:
        return True
    answer = _normalize_search_text(answer_text[:280])
    if comment.startswith("да"):
        return answer.startswith("да")
    if comment.startswith("нет"):
        return answer.startswith("нет")
    significant = [
        tok for tok in _tokenize(comment)
        if tok not in {"подтвердить", "подробно", "объяснить", "вежливо", "дать", "пояснить", "подробный", "ответ", "объяснить", "нужно", "следует"}
    ]
    if not significant:
        return True
    return all(tok in answer for tok in significant[:2]) or any(tok in answer for tok in significant[:3])


def _merge_seed_with_answer(seed: str, answer_text: str) -> str:
    seed = _clean_lines(seed)
    answer_text = _clean_lines(answer_text)
    if not seed:
        return answer_text
    if _normalize_search_text(seed) in _normalize_search_text(answer_text):
        return answer_text
    remainder = _strip_leading_courtesy(answer_text)
    if not remainder:
        return trim_question_reply(f"Спасибо за вопрос. {seed}")
    merged = f"Спасибо за вопрос. {seed} {remainder}"
    return trim_question_reply(re.sub(r"\s+", " ", merged).strip(), QUESTION_MAX_REPLY_LENGTH)


def _sanitize_marketplace_answer(answer_text: str) -> str:
    text = _clean_lines(answer_text)
    if not text:
        return ""
    substitutions = [
        (r"почтов\w+\s+служб\w+\s+или\s+курьерск\w+\s+доставк\w+", "пункт выдачи Wildberries или доставку курьером Wildberries"),
        (r"почт[аы]\s+россии", "службу доставки Wildberries"),
        (r"почтов\w+\s+служб\w+", "службу доставки Wildberries"),
        (r"в нашем магазине", "на Wildberries"),
        (r"в нашем интернет-магазине", "на Wildberries"),
        (r"на нашем сайте", "в карточке товара на Wildberries"),
        (r"на сайте магазина", "в карточке товара на Wildberries"),
        (r"в магазине", "на Wildberries"),
    ]
    for pattern, repl in substitutions:
        text = re.sub(pattern, repl, text, flags=re.I)
    text = re.sub(r"\bслужбы доставки\b", "системы Wildberries", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _finalize_question_answer(question: Dict[str, Any], cluster_key: str, manager_comment: str, answer_text: str) -> str:
    text = trim_question_reply(_clean_lines(answer_text), QUESTION_MAX_REPLY_LENGTH)
    if not text:
        return text
    question_norm = _normalize_search_text(question.get("text"))
    seed = _manager_comment_seed(question, manager_comment, cluster_key)
    if seed and not _answer_matches_manager_comment(text, manager_comment):
        text = _merge_seed_with_answer(seed, text)
    text = _sanitize_marketplace_answer(text)
    if any(token in question_norm for token in ["курьер", "достав", "пвз", "пункт выдачи"]) and "wildberries" not in text.lower():
        text = trim_question_reply(text + " Точные варианты получения и срок доставки система Wildberries показывает при оформлении заказа.", QUESTION_MAX_REPLY_LENGTH)
    return trim_question_reply(text, QUESTION_MAX_REPLY_LENGTH)



def load_question_snapshot() -> Dict[str, Any]:
    data = common.read_json(QUESTION_SNAPSHOT_FILE, {})
    return data if isinstance(data, dict) else {}



def save_question_snapshot(data: Dict[str, Any]) -> None:
    common.write_json(QUESTION_SNAPSHOT_FILE, data)



def load_question_drafts() -> Dict[str, Dict[str, Any]]:
    data = common.read_json(QUESTION_DRAFTS_FILE, {})
    return data if isinstance(data, dict) else {}



def save_question_drafts(data: Dict[str, Dict[str, Any]]) -> None:
    common.write_json(QUESTION_DRAFTS_FILE, data)



def _extract_question_ids_from_form(form: Any) -> List[str]:
    ids: List[str] = []
    seen: set[str] = set()
    try:
        keys = list(form.keys())
    except Exception:
        keys = []
    for key in keys:
        for prefix in ("manager_comment__", "reply__", "action__"):
            if key.startswith(prefix):
                question_id = _clean(key.split("__", 1)[1])
                if question_id and question_id not in seen:
                    seen.add(question_id)
                    ids.append(question_id)
    try:
        for question_id in form.getlist("selected_ids"):
            qid = _clean(question_id)
            if qid and qid not in seen:
                seen.add(qid)
                ids.append(qid)
    except Exception:
        pass
    return ids



def save_question_form_edits(form: Any, question_ids: Optional[List[str]] = None) -> int:
    ids = [_clean(item) for item in (question_ids or _extract_question_ids_from_form(form)) if _clean(item)]
    if not ids:
        return 0
    drafts = load_question_drafts()
    updated = 0
    for question_id in ids:
        draft = dict(drafts.get(question_id) or {})
        before = json.dumps(draft, ensure_ascii=False, sort_keys=True)
        manager_comment = trim_question_reply(_clean_lines(form.get(f"manager_comment__{question_id}")), QUESTION_MANAGER_COMMENT_LIMIT)
        reply_text = trim_question_reply(_clean_lines(form.get(f"reply__{question_id}")), QUESTION_MAX_REPLY_LENGTH)
        action = _normalize_question_action(form.get(f"action__{question_id}") or draft.get("manual_action") or draft.get("action") or "answer")

        previous_comment = _clean_lines(draft.get("manager_comment"))
        previous_reply = _clean_lines(draft.get("reply"))
        comment_changed = manager_comment != previous_comment
        reply_changed = reply_text != previous_reply

        if manager_comment:
            draft["manager_comment"] = manager_comment
        else:
            draft.pop("manager_comment", None)

        draft["manual_action"] = action
        draft["action"] = action

        if reply_text:
            draft["reply"] = reply_text
            draft["source"] = "manual_edit"
            draft["needs_regeneration"] = False
        elif reply_changed:
            draft["reply"] = ""
            if comment_changed:
                draft["needs_regeneration"] = True

        if comment_changed and not reply_changed:
            draft["needs_regeneration"] = True

        if comment_changed or reply_changed:
            draft["updated_at"] = common.utc_now_iso()

        after = json.dumps(draft, ensure_ascii=False, sort_keys=True)
        if after != before:
            drafts[question_id] = draft
            updated += 1
    if updated:
        save_question_drafts(drafts)
    return updated



def load_question_queue() -> List[Dict[str, Any]]:
    data = common.read_json(QUESTION_QUEUE_FILE, [])
    return data if isinstance(data, list) else []



def save_question_queue(data: List[Dict[str, Any]]) -> None:
    common.write_json(QUESTION_QUEUE_FILE, data)



def load_question_archive() -> List[Dict[str, Any]]:
    data = common.read_json(QUESTION_ARCHIVE_FILE, [])
    return data if isinstance(data, list) else []



def save_question_archive(data: List[Dict[str, Any]]) -> None:
    common.write_json(QUESTION_ARCHIVE_FILE, data)



def ignore_question_ids(question_ids: Iterable[str], reason: str = "ignored_by_manager") -> int:
    _log("ignore_start", reason=reason)
    ids = {_clean(item) for item in question_ids if _clean(item)}
    if not ids:
        return 0
    ignored = load_question_ignored_ids()
    ignored.update(ids)
    save_question_ignored_ids(ignored)

    drafts = load_question_drafts()
    queue = load_question_queue()
    archive = load_question_archive()
    clusters = load_question_clusters()

    for qid in ids:
        drafts.pop(qid, None)
    queue = [item for item in queue if _clean(item.get("question_id")) not in ids]
    clusters["assignments"] = {qid: payload for qid, payload in (clusters.get("assignments") or {}).items() if _clean(qid) not in ids}

    archive_map = {_clean(item.get("id")): dict(item) for item in archive if _clean(item.get("id"))}
    now = common.utc_now_iso()
    for qid in ids:
        entry = archive_map.get(qid, {"id": qid})
        entry.update({"status": "ignored", "sent_at": now, "action": "ignore", "reply": "", "reply_source": reason})
        archive_map[qid] = entry

    save_question_drafts(drafts)
    save_question_queue(queue)
    save_question_archive(list(archive_map.values()))
    save_question_clusters(clusters)
    _log("ignore_finish", removed=len(ids), reason=reason)
    return len(ids)



def load_question_sync_meta() -> Dict[str, Any]:
    data = common.read_json(QUESTION_SYNC_META_FILE, {})
    return data if isinstance(data, dict) else {}



def save_question_sync_meta(data: Dict[str, Any]) -> None:
    common.write_json(QUESTION_SYNC_META_FILE, data)



def load_question_clusters() -> Dict[str, Any]:
    data = common.read_json(QUESTION_CLUSTERS_FILE, {})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("assignments", {})
    data.setdefault("cluster_meta", {})
    return data



def save_question_clusters(data: Dict[str, Any]) -> None:
    common.write_json(QUESTION_CLUSTERS_FILE, data)



def _import_cluster_source_tag(value: Any) -> str:
    source = _clean(value)
    if source.startswith(QUESTION_IMPORTED_ASSIGNMENT_SOURCE):
        return source
    return ""



def normalize_imported_cluster_key(value: Any, fallback_title: Any = "") -> str:
    raw = _clean(value).lower().replace("ё", "е")
    if raw:
        raw = re.sub(r"[^0-9a-zа-я._-]+", ".", raw, flags=re.I)
        raw = re.sub(r"\.{2,}", ".", raw).strip("._-")
    if raw:
        return raw[:160]
    title = _normalize_search_text(fallback_title)
    if title:
        slug = ".".join(title.split()[:8])
        slug = re.sub(r"\.{2,}", ".", slug).strip("._-")
        if slug:
            return f"import.{slug}"[:160]
    fallback = hashlib.sha1(_clean_lines(fallback_title or value or "cluster").encode("utf-8")).hexdigest()[:12]
    return f"import.uncategorized.{fallback}"



def normalize_imported_cluster_title(value: Any, cluster_key: Any = "") -> str:
    title = _clean_lines(value)
    if title:
        return trim_question_reply(title, 180)
    key = _clean(cluster_key)
    if not key:
        return "Импортированный кластер"
    human = key.replace(".", " · ").replace("_", " ").replace("-", " ")
    human = re.sub(r"\s+", " ", human).strip()
    return trim_question_reply(human or "Импортированный кластер", 180)



def _normalize_cluster_sort_order(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return None



def apply_imported_clusters_for_active_tenant(
    rows: List[Dict[str, Any]],
    *,
    source_name: str = "",
    overwrite_manual: bool = False,
    clear_previous_imported: bool = True,
    batch_id: str = "",
) -> Dict[str, Any]:
    state = load_question_clusters()
    assignments = state.setdefault("assignments", {})
    cluster_meta = state.setdefault("cluster_meta", {})
    drafts = load_question_drafts()
    snapshot = load_question_snapshot()
    current_ids = {_clean((item or {}).get("id")) for item in (snapshot.get("questions") or []) if _clean((item or {}).get("id"))}
    now = common.utc_now_iso()
    batch_id = _clean(batch_id) or hashlib.sha1(f"{source_name}|{now}".encode("utf-8")).hexdigest()[:12]

    if clear_previous_imported:
        stale_ids = [
            question_id
            for question_id, payload in list(assignments.items())
            if _import_cluster_source_tag((payload or {}).get("source"))
        ]
        for question_id in stale_ids:
            assignments.pop(question_id, None)
        stale_cluster_keys = [
            cluster_key
            for cluster_key, meta in list(cluster_meta.items())
            if _import_cluster_source_tag((meta or {}).get("source"))
        ]
        for cluster_key in stale_cluster_keys:
            cluster_meta.pop(cluster_key, None)

    applied = 0
    preserved_manual = 0
    invalid_rows = 0
    missing_in_snapshot = 0
    imported_cluster_keys: set[str] = set()
    cleaned_ai_drafts = 0
    imported_question_ids: set[str] = set()

    for row in rows:
        question_id = _clean(row.get("question_id"))
        cluster_key = normalize_imported_cluster_key(row.get("cluster_key"), row.get("cluster_title"))
        cluster_title = normalize_imported_cluster_title(row.get("cluster_title"), cluster_key)
        sort_order = _normalize_cluster_sort_order(row.get("cluster_order"))
        if not question_id or not cluster_key:
            invalid_rows += 1
            continue
        current_assignment = assignments.get(question_id) or {}
        current_source = _clean((current_assignment or {}).get("source"))
        if current_source == "manual_move" and not overwrite_manual:
            preserved_manual += 1
            continue
        assignments[question_id] = {
            "cluster_key": cluster_key,
            "updated_at": now,
            "source": QUESTION_IMPORTED_ASSIGNMENT_SOURCE,
            "import_batch_id": batch_id,
            "cluster_title": cluster_title,
        }
        if sort_order is not None:
            assignments[question_id]["sort_order"] = sort_order
        meta = cluster_meta.setdefault(cluster_key, {})
        meta.update(
            {
                "title_override": cluster_title,
                "updated_at": now,
                "source": QUESTION_IMPORTED_ASSIGNMENT_SOURCE,
                "import_batch_id": batch_id,
            }
        )
        if sort_order is not None:
            meta["sort_order"] = sort_order
        imported_cluster_keys.add(cluster_key)
        imported_question_ids.add(question_id)
        if question_id not in current_ids:
            missing_in_snapshot += 1
        draft = drafts.get(question_id) or {}
        draft_source = _clean(draft.get("source")).lower()
        if draft and draft_source not in {"manual", "manual_edit", "queued"}:
            draft_cluster = _clean(draft.get("cluster_key"))
            if draft_cluster and draft_cluster != cluster_key:
                drafts.pop(question_id, None)
                cleaned_ai_drafts += 1
        applied += 1

    if imported_cluster_keys:
        used_cluster_keys = {
            _clean((payload or {}).get("cluster_key"))
            for payload in assignments.values()
            if _clean((payload or {}).get("cluster_key"))
        }
        for cluster_key in list(cluster_meta.keys()):
            meta = cluster_meta.get(cluster_key) or {}
            if _import_cluster_source_tag(meta.get("source")) and cluster_key not in used_cluster_keys:
                cluster_meta.pop(cluster_key, None)

    save_question_clusters(state)
    save_question_drafts(drafts)
    sync_meta = load_question_sync_meta()
    sync_meta["last_cluster_import"] = {
        "imported_at": now,
        "source_name": _clean(source_name),
        "batch_id": batch_id,
        "applied": applied,
        "preserved_manual": preserved_manual,
        "invalid_rows": invalid_rows,
        "missing_in_snapshot": missing_in_snapshot,
        "cluster_count": len(imported_cluster_keys),
        "overwrite_manual": bool(overwrite_manual),
        "clear_previous_imported": bool(clear_previous_imported),
        "cleaned_ai_drafts": cleaned_ai_drafts,
    }
    save_question_sync_meta(sync_meta)
    return {
        "applied": applied,
        "preserved_manual": preserved_manual,
        "invalid_rows": invalid_rows,
        "missing_in_snapshot": missing_in_snapshot,
        "cluster_count": len(imported_cluster_keys),
        "cleaned_ai_drafts": cleaned_ai_drafts,
        "question_ids": sorted(imported_question_ids),
        "cluster_keys": sorted(imported_cluster_keys),
        "batch_id": batch_id,
    }



def load_question_rules() -> Dict[str, Any]:
    return common.load_question_rules()



def save_question_rules(data: Dict[str, Any]) -> None:
    common.save_question_rules(data)



def load_question_prompt() -> str:
    return common.load_question_prompt()



def save_question_prompt(text: str) -> None:
    common.save_question_prompt(text)



def _question_snapshot_map(snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    snapshot = snapshot or load_question_snapshot()
    mapping: Dict[str, Dict[str, Any]] = {}
    for item in snapshot.get("questions") or []:
        question = common.normalize_question(item)
        qid = _clean(question.get("id"))
        if qid:
            mapping[qid] = question
    return mapping



def get_locally_processed_question_ids() -> set[str]:
    return {
        _clean(item.get("id"))
        for item in load_question_archive()
        if _clean(item.get("status")) in QUESTION_PROCESSED_STATUSES
    }



def refresh_question_snapshot() -> Dict[str, Any]:
    _log("snapshot_refresh_start")
    unseen = {}
    try:
        unseen = common.fetch_unseen_feedbacks_questions()
    except Exception:
        unseen = {}

    all_questions: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    count_unanswered = 0
    count_archive = 0
    truncated = False

    for batch_idx in range(QUESTION_MAX_RAW_BATCHES):
        skip = batch_idx * QUESTION_RAW_BATCH_SIZE
        questions, count_unanswered, count_archive, _ = common.fetch_questions_page(
            False,
            skip=skip,
            take=QUESTION_RAW_BATCH_SIZE,
            order="dateDesc",
        )
        if not questions:
            break
        for row in questions:
            normalized = common.normalize_question(row)
            qid = _clean(normalized.get("id"))
            if not qid or qid in seen_ids:
                continue
            seen_ids.add(qid)
            all_questions.append(normalized)
        if len(all_questions) >= count_unanswered:
            break
        if skip + QUESTION_RAW_BATCH_SIZE >= 10000:
            truncated = len(all_questions) < count_unanswered
            break
        time.sleep(0.34)

    if len(all_questions) < count_unanswered and len(all_questions) >= QUESTION_RAW_BATCH_SIZE * QUESTION_MAX_RAW_BATCHES:
        truncated = True

    snapshot = {
        "fetched_at": common.utc_now_iso(),
        "questions": all_questions,
        "count_unanswered": int(count_unanswered or len(all_questions)),
        "count_archive": int(count_archive or 0),
        "has_new_questions": bool(unseen.get("hasNewQuestions")),
        "raw_scanned": len(all_questions),
        "truncated": bool(truncated or (count_unanswered and len(all_questions) < min(int(count_unanswered), 10000))),
    }
    save_question_snapshot(snapshot)
    _log("snapshot_refresh_finish", count_unanswered=snapshot.get("count_unanswered"), question_count=len(snapshot.get("questions") or []), fetched_at=snapshot.get("fetched_at"))
    return snapshot



def get_question_snapshot(force_refresh: bool = False) -> Dict[str, Any]:
    snapshot = load_question_snapshot()
    if snapshot and not force_refresh and _snapshot_is_fresh(snapshot):
        return snapshot
    return refresh_question_snapshot()



def _detect_intent_key(question_text: str) -> str:
    normalized = _normalize_search_text(question_text)
    if not normalized:
        return "other"
    for pattern in _INTENT_PATTERNS:
        if pattern["key"] == "other":
            continue
        for keyword in pattern["keywords"]:
            if keyword and keyword in normalized:
                return pattern["key"]
    return "other"



def _unknown_cluster_key(question_text: str) -> str:
    tokens = _tokenize(question_text)
    if not tokens:
        return "freeform::empty"
    prefix = " ".join(tokens[:6])
    return "freeform::" + hashlib.sha1(prefix.encode("utf-8")).hexdigest()[:12]



def _jaccard_similarity(left: Iterable[str], right: Iterable[str]) -> float:
    a = set(left)
    b = set(right)
    if not a or not b:
        return 0.0
    return len(a & b) / max(1, len(a | b))



def _auto_cluster_assignments(questions: List[Dict[str, Any]]) -> Dict[str, str]:
    assignments: Dict[str, str] = {}
    freeform_index: Dict[str, Dict[str, Any]] = {}
    for question in sorted(questions, key=lambda item: _parse_created_date(item.get("createdDate"))):
        qid = _clean(question.get("id"))
        if not qid:
            continue
        question_text = _clean_lines(question.get("text"))
        intent_key = _detect_intent_key(question_text)
        if intent_key != "other":
            assignments[qid] = intent_key
            continue
        tokens = _tokenize(question_text)
        matched_key = ""
        if tokens:
            for candidate_key, meta in freeform_index.items():
                similarity = _jaccard_similarity(tokens, meta.get("tokens") or [])
                if similarity >= 0.68:
                    matched_key = candidate_key
                    break
        if not matched_key:
            matched_key = _unknown_cluster_key(question_text)
            freeform_index[matched_key] = {"tokens": tokens, "sample": question_text}
        assignments[qid] = matched_key
    return assignments



def _cluster_title_from_key(cluster_key: str, members: List[Dict[str, Any]], cluster_state: Dict[str, Any]) -> str:
    cluster_meta = (cluster_state.get("cluster_meta") or {}).get(cluster_key) or {}
    title_override = _clean(cluster_meta.get("title_override"))
    if title_override:
        return title_override
    if cluster_key in _INTENT_BY_KEY:
        return _INTENT_BY_KEY[cluster_key]["title"]
    if cluster_key.startswith("manual::"):
        return _clean(cluster_meta.get("title_override")) or "Подкластер"
    if cluster_key.startswith("freeform::"):
        sample = _clean_lines((members[0] if members else {}).get("text"))
        if sample:
            return trim_question_reply(sample, 80)
    return "Прочие вопросы"



def _cluster_sort_order(cluster_key: str, cluster_state: Dict[str, Any]) -> Optional[int]:
    cluster_meta = (cluster_state.get("cluster_meta") or {}).get(cluster_key) or {}
    return _normalize_cluster_sort_order(cluster_meta.get("sort_order"))



def _build_cluster_assignments(questions: List[Dict[str, Any]]) -> Tuple[Dict[str, str], Dict[str, Any]]:
    cluster_state = load_question_clusters()
    auto_assignments = _auto_cluster_assignments(questions)
    final_assignments = dict(auto_assignments)
    for qid, payload in (cluster_state.get("assignments") or {}).items():
        cluster_key = _clean((payload or {}).get("cluster_key"))
        if qid in final_assignments and cluster_key:
            final_assignments[qid] = cluster_key
    cluster_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for question in questions:
        qid = _clean(question.get("id"))
        if not qid:
            continue
        cluster_key = final_assignments.get(qid) or auto_assignments.get(qid) or "other"
        cluster_map[cluster_key].append(question)
    cluster_info: Dict[str, Any] = {}
    rules = load_question_rules()
    for cluster_key, members in cluster_map.items():
        cluster_info[cluster_key] = {
            "key": cluster_key,
            "title": _cluster_title_from_key(cluster_key, members, cluster_state),
            "sort_order": _cluster_sort_order(cluster_key, cluster_state),
            "count": len(members),
            "members": members,
            "article_count": len({_clean(m.get("productDetails", {}).get("supplierArticle")) for m in members if _clean(m.get("productDetails", {}).get("supplierArticle"))}),
            "matching_rules": _matching_cluster_rules(cluster_key, rules),
        }
    return final_assignments, cluster_info



def _matching_cluster_rules(cluster_key: str, rules: Dict[str, Any]) -> List[Dict[str, Any]]:
    matched: List[Tuple[int, int, Dict[str, Any]]] = []
    for rule in rules.get("rules", []):
        if not isinstance(rule, dict) or not rule.get("enabled", True):
            continue
        rule_cluster = _clean(rule.get("cluster_key"))
        if rule_cluster and rule_cluster != cluster_key:
            continue
        specificity = 0
        if rule_cluster:
            specificity += 10
        specificity += len(rule.get("article_keywords_all") or []) * 3
        specificity += len(rule.get("article_keywords_any") or []) * 2
        specificity += len(rule.get("question_keywords_all") or []) * 2
        specificity += len(rule.get("question_keywords_any") or [])
        matched.append((_safe_int(rule.get("priority"), 0), specificity, rule))
    matched.sort(key=lambda item: (item[0], item[1], _clean(item[2].get("updated_at")) or _clean(item[2].get("created_at"))), reverse=True)
    return [item[2] for item in matched]



def _split_keywords(raw: Any) -> List[str]:
    text = _clean_lines(raw)
    if not text:
        return []
    parts = re.split(r"[,;\n]+", text)
    seen: set[str] = set()
    result: List[str] = []
    for part in parts:
        token = _normalize_search_text(part)
        if token and token not in seen:
            seen.add(token)
            result.append(token)
    return result



def _rule_matches_question(rule: Dict[str, Any], question: Dict[str, Any], cluster_key: str) -> Tuple[bool, int]:
    if not rule.get("enabled", True):
        return False, 0
    rule_cluster = _clean(rule.get("cluster_key"))
    if rule_cluster and rule_cluster != cluster_key:
        return False, 0

    question_text = _normalize_search_text(question.get("text"))
    article_text = _normalize_search_text(question.get("productDetails", {}).get("supplierArticle"))

    q_any = [
        _normalize_search_text(x)
        for x in (rule.get("question_keywords_any") or [])
        if _normalize_search_text(x)
    ]
    q_all = [
        _normalize_search_text(x)
        for x in (rule.get("question_keywords_all") or [])
        if _normalize_search_text(x)
    ]
    a_any = [
        _normalize_search_text(x)
        for x in (rule.get("article_keywords_any") or [])
        if _normalize_search_text(x)
    ]
    a_all = [
        _normalize_search_text(x)
        for x in (rule.get("article_keywords_all") or [])
        if _normalize_search_text(x)
    ]

    if q_all and not all(token in question_text for token in q_all):
        return False, 0
    if q_any and not any(token in question_text for token in q_any):
        return False, 0
    if a_all and not all(token in article_text for token in a_all):
        return False, 0
    if a_any and not any(token in article_text for token in a_any):
        return False, 0

    specificity = 0
    if rule_cluster:
        specificity += 10
    if q_any:
        specificity += len(q_any)
    if q_all:
        specificity += len(q_all) * 2
    if a_any:
        specificity += len(a_any) * 3
    if a_all:
        specificity += len(a_all) * 4
    return True, specificity



def match_question_rules(question: Dict[str, Any], cluster_key: str, rules: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    rules = rules or load_question_rules()
    matched: List[Tuple[int, int, Dict[str, Any]]] = []
    for rule in rules.get("rules", []):
        if not isinstance(rule, dict):
            continue
        ok, specificity = _rule_matches_question(rule, question, cluster_key)
        if not ok:
            continue
        priority = _safe_int(rule.get("priority"), 0)
        matched.append((priority, specificity, rule))
    matched.sort(key=lambda item: (item[0], item[1], _clean(item[2].get("updated_at")) or _clean(item[2].get("created_at"))), reverse=True)
    return [item[2] for item in matched]



def _render_template(template: str, question: Dict[str, Any]) -> str:
    product = question.get("productDetails", {}) or {}
    answer = template or ""
    replacements = {
        "product_name": _clean(product.get("productName")),
        "supplier_article": _clean(product.get("supplierArticle")),
        "brand_name": _clean(product.get("brandName")),
        "question_text": _clean_lines(question.get("text")),
        "nm_id": str(_safe_int(product.get("nmId"))),
    }
    for key, value in replacements.items():
        answer = answer.replace("{" + key + "}", value)
    return trim_question_reply(answer, QUESTION_MAX_REPLY_LENGTH)



def _default_manager_notes(rules: Dict[str, Any]) -> List[str]:
    return [_clean_lines(item) for item in (rules.get("default_instructions") or []) if _clean_lines(item)]



def _intent_fact_sensitive(cluster_key: str) -> bool:
    return bool((_INTENT_BY_KEY.get(cluster_key) or {}).get("fact_sensitive"))



def _fallback_text_for_cluster(cluster_key: str) -> str:
    return _DEFAULT_FALLBACKS.get(cluster_key) or _DEFAULT_FALLBACKS["default"]



def _expand_question_reply_if_needed(
    question: Dict[str, Any],
    cluster_title: str,
    cluster_key: str,
    matched_rules: List[Dict[str, Any]],
    prompt_text: str,
    draft_text: str,
    manager_comment: str = "",
    cross_sell_items: Optional[List[str]] = None,
) -> str:
    draft_text = trim_question_reply(draft_text, QUESTION_MAX_REPLY_LENGTH)
    if len(draft_text) >= QUESTION_DETAIL_TARGET_LENGTH:
        return draft_text

    manager_notes = [
        _clean_lines(rule.get("manager_instruction"))
        for rule in matched_rules[:5]
        if _clean_lines(rule.get("manager_instruction"))
    ]
    product = question.get("productDetails", {}) or {}
    cross_sell_items = [item for item in (cross_sell_items or []) if _clean(item)]
    user_prompt = f"""
Ниже слишком короткий черновик ответа. Перепиши его так, чтобы ответ стал более подробным, полезным и явно учитывал комментарий менеджера.

Комментарий менеджера по строке:
{trim_question_reply(_clean_lines(manager_comment), QUESTION_MANAGER_COMMENT_LIMIT) or '—'}

Вопрос покупателя:
{_clean_lines(question.get('text'))}

Кластер: {cluster_title}
Товар: {_clean(product.get('productName'))}
Артикул продавца: {_clean(product.get('supplierArticle'))}

Инструкции менеджера по кластеру:
{os.linesep.join('- ' + note for note in manager_notes) if manager_notes else '- Отдельной инструкции нет.'}

Список связанных товаров для мягкой рекомендации:
{os.linesep.join('- ' + item for item in cross_sell_items) if cross_sell_items else '- Пока без рекомендации.'}

Текущий короткий черновик:
{draft_text}

Сделай ответ более развёрнутым: обычно около 400–900 символов и 4–7 предложений. Не выдумывай факты. Если фактов мало, раскрой безопасную формулировку нормальным человеческим ответом.
""".strip()
    try:
        expanded = common.call_ai(
            [
                {"role": "system", "content": prompt_text},
                {"role": "user", "content": user_prompt},
            ],
            model=common.OPENAI_MODEL,
            temperature=0.3,
        )
        expanded = trim_question_reply(expanded, QUESTION_MAX_REPLY_LENGTH)
        if len(expanded) >= max(len(draft_text), QUESTION_DETAIL_TARGET_LENGTH - 40):
            return expanded
    except Exception:
        pass

    fallback = _fallback_text_for_cluster(cluster_key)
    if len(fallback) > len(draft_text):
        return fallback
    return draft_text


def is_question_draft_compatible(
    draft_entry: Optional[Dict[str, Any]],
    question_sig: str,
    prompt_sig: str,
    rules_sig: str,
    cluster_key: str,
) -> bool:
    if not draft_entry:
        return False
    if bool(draft_entry.get("needs_regeneration")):
        return False
    if _clean(draft_entry.get("signature")) != _clean(question_sig):
        return False
    source = _clean(draft_entry.get("source")).lower()
    if source in {"manual", "manual_edit", "queued"}:
        return True
    if _clean(draft_entry.get("cluster_key")) != _clean(cluster_key):
        return False
    return _clean(draft_entry.get("prompt_signature")) == _clean(prompt_sig) and _clean(draft_entry.get("rules_signature")) == _clean(rules_sig)



def _choose_question_cross_sell_items(question: Dict[str, Any], cluster_key: str) -> List[str]:
    try:
        review_rules = common.load_rules()
    except Exception:
        review_rules = {}
    catalog = review_rules.get("cross_sell_catalog") or []
    if not isinstance(catalog, list) or not catalog:
        return []
    product = question.get("productDetails", {}) or {}
    combined = " ".join(
        [
            _normalize_search_text(question.get("text")),
            _normalize_search_text(product.get("productName")),
            _normalize_search_text(product.get("supplierArticle")),
            _normalize_search_text(product.get("brandName")),
            _normalize_search_text(cluster_key),
        ]
    )
    current_article = _clean(product.get("supplierArticle"))
    suggestions: List[str] = []
    for item in catalog:
        if not isinstance(item, dict):
            continue
        title = _clean(item.get("title"))
        article = _clean(item.get("article"))
        if not title or not article or "ЗАМЕНИТЬ" in article or article == current_article:
            continue
        tags = [_normalize_search_text(tag) for tag in (item.get("tags") or []) if _normalize_search_text(tag)]
        if tags and not any(tag in combined for tag in tags):
            if cluster_key not in {"availability", "delivery"}:
                continue
        candidate = f"арт. {article} — {title}"
        if candidate not in suggestions:
            suggestions.append(candidate)
        if len(suggestions) >= 2:
            break
    return suggestions



def _build_question_user_prompt(
    question: Dict[str, Any],
    cluster_title: str,
    cluster_key: str,
    matched_rules: List[Dict[str, Any]],
    rules: Dict[str, Any],
    manager_comment: str = "",
    cross_sell_items: Optional[List[str]] = None,
) -> str:
    product = question.get("productDetails", {}) or {}
    default_notes = _default_manager_notes(rules)
    manager_instructions: List[str] = []
    rule_summaries: List[str] = []
    for rule in matched_rules[:5]:
        title = _clean(rule.get("title") or rule.get("id") or "Правило")
        instruction = _clean_lines(rule.get("manager_instruction"))
        if instruction:
            manager_instructions.append(f"{title}: {instruction}")
        mode = _clean(rule.get("answer_mode") or "ai")
        rule_summaries.append(f"{title} (режим: {mode})")

    manager_comment = trim_question_reply(_clean_lines(manager_comment), QUESTION_MANAGER_COMMENT_LIMIT)
    manager_comment_block = manager_comment or "— Менеджер комментарий по строке не добавил."
    manager_block = os.linesep.join('- ' + note for note in manager_instructions) if manager_instructions else '- Для этого кластера отдельной инструкции менеджера пока нет.'
    default_block = os.linesep.join('- ' + note for note in default_notes) if default_notes else '- Общих правил нет.'
    rule_block = os.linesep.join('- ' + note for note in rule_summaries) if rule_summaries else '- Совпавших правил нет.'
    fallback_text = _fallback_text_for_cluster(cluster_key)
    cross_sell_items = [item for item in (cross_sell_items or []) if _clean(item)]
    cross_sell_block = os.linesep.join('- ' + item for item in cross_sell_items) if cross_sell_items else '- Подходящих рекомендаций сейчас нет.'
    return f"""
ГЛАВНЫЙ ПРИОРИТЕТ — КОММЕНТАРИЙ МЕНЕДЖЕРА ПО ЭТОЙ СТРОКЕ:
{manager_comment_block}

ВТОРОЙ ПРИОРИТЕТ — ИНСТРУКЦИИ МЕНЕДЖЕРА ПО КЛАСТЕРУ И СРАБОТАВШИЕ ПРАВИЛА:
{manager_block}

ДОПОЛНИТЕЛЬНЫЕ ОБЩИЕ УКАЗАНИЯ:
{default_block}

СРАБОТАВШИЕ ПРАВИЛА:
{rule_block}

КЛАСТЕР ВОПРОСА: {cluster_title}
ВОПРОС ПОКУПАТЕЛЯ:
{_clean_lines(question.get('text'))}

ДАННЫЕ ТОВАРА:
- Товар: {_clean(product.get('productName'))}
- Артикул продавца: {_clean(product.get('supplierArticle'))}
- Бренд: {_clean(product.get('brandName'))}
- nmID: {_safe_int(product.get('nmId'))}
- Размер / вариант: {_clean(product.get('size'))}
- Дата вопроса: {_clean(question.get('createdDate'))}

СВЯЗАННЫЕ ТОВАРЫ ДЛЯ МЯГКОЙ РЕКОМЕНДАЦИИ (если реально уместно):
{cross_sell_block}

ОБЯЗАТЕЛЬНЫЙ ПЕРВЫЙ СМЫСЛОВОЙ ТЕЗИС ОТВЕТА:
{_manager_comment_seed(question, manager_comment, cluster_key) or "— Если короткий комментарий менеджера не даёт готового тезиса, сам аккуратно раскрой его смысл без искажения."}

ТРЕБОВАНИЯ К ОТВЕТУ:
- преврати даже короткий комментарий менеджера вроде «да», «нет», «скоро», «ждите», «в карточке», «в комплекте» в полноценный и подробный ответ покупателю;
- если комментарий менеджера однозначный, первый смысловой тезис ответа обязан прямо отражать именно его;
- ответ должен быть развёрнутым, обычно около 400–900 символов и 4–7 предложений;
- в первую очередь раскрой и соблюди комментарий менеджера по строке;
- затем учти инструкцию менеджера по кластеру, исключения и логику по артикулам;
- если менеджер дал факты или готовые формулировки, обязательно используй их;
- отвечай только по товару и только на вопрос покупателя;
- все ответы относятся к маркетплейсу Wildberries: не упоминай Почту России, сторонние службы доставки, внешний сайт, интернет-магазин или «наш магазин»;
- если речь о доставке или получении, допускаются только Wildberries, пункт выдачи Wildberries и курьер Wildberries;
- не придумывай факты, которых нет в данных товара, вопросе или инструкциях менеджера;
- если точных фактов мало, используй безопасную формулировку ниже, но всё равно сделай ответ полезным и подробным;
- если это уместно, можно очень мягко в последнем предложении порекомендовать 1 связанный товар из списка выше, без навязчивой продажи;
- не добавляй служебных пояснений, заголовков и комментариев для менеджера.

Безопасная формулировка на случай нехватки фактов:
{fallback_text}

Верни только готовый текст ответа покупателю.
""".strip()



# os нужен только в этой функции, чтобы не засорять модуль сверху.
import os  # noqa: E402



def _draft_action_from_rule(rule: Optional[Dict[str, Any]]) -> str:
    mode = _clean((rule or {}).get("answer_mode")).lower()
    if mode == "reject":
        return "reject"
    if mode == "skip":
        return "skip"
    return "answer"



def _estimate_confidence(cluster_key: str, matched_rules: List[Dict[str, Any]], action: str) -> float:
    if matched_rules:
        top = matched_rules[0]
        article_specific = bool((top.get("article_keywords_any") or []) or (top.get("article_keywords_all") or []))
        if action in {"reject", "skip"}:
            return 0.97 if article_specific else 0.94
        if _clean(top.get("answer_mode")) == "template":
            return 0.96 if article_specific else 0.92
        return 0.93 if article_specific else 0.86
    if cluster_key in {"delivery", "availability", "discount"}:
        return 0.72
    if cluster_key in _INTENT_BY_KEY and cluster_key != "other":
        return 0.58 if _intent_fact_sensitive(cluster_key) else 0.66
    return 0.45



def generate_question_draft(question: Dict[str, Any], force: bool = False) -> Dict[str, Any]:
    _log("draft_prepare_start", question_id=_clean((question or {}).get("id")), force=bool(force))
    question = common.normalize_question(question)
    question_id = _clean(question.get("id"))
    question_sig = question_signature(question)
    drafts = load_question_drafts()
    rules = load_question_rules()
    prompt_text = load_question_prompt()
    prompt_sig = _text_signature(prompt_text)
    rules_sig = _rules_signature(rules)
    assignments, cluster_info = _build_cluster_assignments([question])
    cluster_key = assignments.get(question_id, "other")
    cluster_title = (cluster_info.get(cluster_key) or {}).get("title") or _cluster_title_from_key(cluster_key, [question], load_question_clusters())
    cached = drafts.get(question_id) or {}
    if not force and is_question_draft_compatible(cached, question_sig, prompt_sig, rules_sig, cluster_key):
        _log("draft_prepare_cached", question_id=question_id)
        return cached

    matched_rules = match_question_rules(question, cluster_key, rules)
    top_rule = matched_rules[0] if matched_rules else None
    manual_action = _normalize_question_action(cached.get("manual_action") or cached.get("action") or "answer")
    rule_action = _draft_action_from_rule(top_rule if matched_rules else None)
    action = manual_action if manual_action in {"reject", "skip"} else rule_action
    if action == "answer":
        action = "answer"
    confidence = _estimate_confidence(cluster_key, matched_rules, action)
    auto_threshold = float(rules.get("auto_queue_confidence") or 0.92)

    manager_comment = trim_question_reply(_clean_lines(cached.get("manager_comment")), QUESTION_MANAGER_COMMENT_LIMIT)
    cross_sell_items = _choose_question_cross_sell_items(question, cluster_key)

    reply_text = ""
    source = "ai"
    explanation = ""
    ai_error_code = ""
    ai_error_message = ""

    if action == "skip":
        source = "skip_rule" if manual_action == "answer" else "skip_manual"
        explanation = "Вопрос помечен как пропускаемый и не будет ставиться в очередь."
    elif action == "reject":
        source = "reject_manual" if manual_action == "reject" else "reject_rule"
        reject_template = (
            _clean_lines((top_rule or {}).get("answer_template"))
            or manager_comment
            or "Вопрос отклонён"
        )
        reply_text = _render_template(reject_template, question)
        reply_text = _finalize_question_answer(question, cluster_key, manager_comment, reply_text)
        explanation = "Для вопроса выбран режим отклонения."
    elif _clean((top_rule or {}).get("answer_mode")) == "template" and _clean_lines((top_rule or {}).get("answer_template")) and not manager_comment:
        source = "template"
        reply_text = _render_template(_clean_lines(top_rule.get("answer_template")), question)
        reply_text = _finalize_question_answer(question, cluster_key, manager_comment, reply_text)
        explanation = "К вопросу применён готовый шаблон менеджера."
    else:
        user_prompt = _build_question_user_prompt(
            question,
            cluster_title,
            cluster_key,
            matched_rules,
            rules,
            manager_comment=manager_comment,
            cross_sell_items=cross_sell_items,
        )
        try:
            reply_text = trim_question_reply(
                common.call_ai(
                    [
                        {"role": "system", "content": prompt_text},
                        {"role": "user", "content": user_prompt},
                    ],
                    model=common.OPENAI_MODEL,
                    temperature=0.35,
                ),
                QUESTION_MAX_REPLY_LENGTH,
            )
            reply_text = _expand_question_reply_if_needed(
                question,
                cluster_title,
                cluster_key,
                matched_rules,
                prompt_text,
                reply_text,
                manager_comment=manager_comment,
                cross_sell_items=cross_sell_items,
            )
            reply_text = _finalize_question_answer(question, cluster_key, manager_comment, reply_text)
            source = "ai_comment" if manager_comment else "ai"
            explanation = "Черновик сгенерирован AI на основе комментария менеджера по строке и логики кластера." if manager_comment else "Черновик сгенерирован AI по логике кластера и правилам."
        except Exception as exc:
            ai_info = common.describe_ai_failure(exc, model=common.OPENAI_MODEL)
            ai_error_code = _clean(ai_info.get("code"))
            ai_error_message = _clean_lines(ai_info.get("public_message") or ai_info.get("raw_message"))
            reply_text = _fallback_text_for_cluster(cluster_key)
            reply_text = _expand_question_reply_if_needed(
                question,
                cluster_title,
                cluster_key,
                matched_rules,
                prompt_text,
                reply_text,
                manager_comment=manager_comment,
                cross_sell_items=cross_sell_items,
            )
            reply_text = _finalize_question_answer(question, cluster_key, manager_comment, reply_text)
            source = "fallback"
            explanation = f"{ai_error_message or 'AI недоступен.'} Использована безопасная fallback-формулировка."
            _log('draft_prepare_fallback', question_id=question_id, cluster_key=cluster_key, error_type=ai_error_code or 'ai_unavailable', error=ai_error_message or str(exc))

    if action == "answer" and len(reply_text) < QUESTION_MIN_REPLY_LENGTH:
        reply_text = _fallback_text_for_cluster(cluster_key)
        reply_text = _expand_question_reply_if_needed(
            question,
            cluster_title,
            cluster_key,
            matched_rules,
            prompt_text,
            reply_text,
            manager_comment=manager_comment,
            cross_sell_items=cross_sell_items,
        )
        reply_text = _finalize_question_answer(question, cluster_key, manager_comment, reply_text)
        source = "fallback"
        explanation = "Черновик был пустым, использована безопасная fallback-формулировка."
        ai_error_code = ai_error_code or 'empty_reply'
        _log('draft_prepare_fallback', question_id=question_id, cluster_key=cluster_key, error_type='empty_reply')

    auto_ready = bool(
        action == "answer"
        and not manager_comment
        and matched_rules
        and bool((matched_rules[0] or {}).get("allow_auto_send"))
        and confidence >= auto_threshold
        and len(reply_text) >= QUESTION_MIN_REPLY_LENGTH
    )

    entry = {
        **cached,
        "reply": reply_text,
        "action": action,
        "manual_action": manual_action,
        "manager_comment": manager_comment,
        "confidence": round(confidence, 3),
        "auto_ready": auto_ready,
        "cluster_key": cluster_key,
        "cluster_title": cluster_title,
        "signature": question_sig,
        "generated_at": common.utc_now_iso(),
        "rule_ids": [_clean(rule.get("id")) for rule in matched_rules if _clean(rule.get("id"))],
        "rule_titles": [_clean(rule.get("title") or rule.get("id")) for rule in matched_rules[:5] if _clean(rule.get("title") or rule.get("id"))],
        "source": source,
        "prompt_signature": prompt_sig,
        "rules_signature": rules_sig,
        "explanation": explanation,
        "ai_error_code": ai_error_code,
        "ai_error_message": ai_error_message,
        "cross_sell_items": cross_sell_items,
        "needs_regeneration": False,
    }
    drafts[question_id] = entry
    save_question_drafts(drafts)
    _log("draft_prepare_finish", question_id=question_id, action=entry.get("action"), cluster_key=entry.get("cluster_key"), confidence=entry.get("confidence"), auto_ready=bool(entry.get("auto_ready")), source=entry.get("source"))
    return entry


def remove_question_draft(question_id: str) -> None:
    drafts = load_question_drafts()
    if question_id in drafts:
        del drafts[question_id]
        save_question_drafts(drafts)



def _sort_question_rows(rows: List[Dict[str, Any]], sort_by: str) -> List[Dict[str, Any]]:
    sort_by = _clean(sort_by) or "newest"
    if sort_by == "oldest":
        return sorted(rows, key=lambda row: _parse_created_date(row.get("created_date")))
    if sort_by == "cluster":
        return sorted(
            rows,
            key=lambda row: (
                row.get("cluster_sort_order") is None,
                row.get("cluster_sort_order") if row.get("cluster_sort_order") is not None else 10**9,
                _clean(row.get("cluster_title")),
                -_parse_created_date(row.get("created_date")).timestamp(),
            ),
        )
    return sorted(rows, key=lambda row: _parse_created_date(row.get("created_date")), reverse=True)



def _question_matches_text(row: Dict[str, Any], query: str) -> bool:
    query = _normalize_search_text(query)
    if not query:
        return True
    haystack = " ".join(
        [
            _clean(row.get("id")),
            _clean(row.get("product_name")),
            _clean(row.get("supplier_article")),
            _clean(row.get("brand_name")),
            _clean(row.get("question_text")),
            _clean(row.get("draft_reply")),
            _clean(row.get("cluster_title")),
            _clean(row.get("rule_title")),
            str(row.get("nm_id") or ""),
        ]
    )
    return query in _normalize_search_text(haystack)



def _row_question_is_processed(row: Dict[str, Any]) -> bool:
    queue_status = _clean(row.get("queue_status"))
    archive_status = _clean(row.get("archive_status"))
    return queue_status in QUESTION_PROCESSED_STATUSES or archive_status in QUESTION_PROCESSED_STATUSES



def _row_matches_filters(
    row: Dict[str, Any],
    draft_filter: str = "all",
    queue_filter: str = "all",
    search_query: str = "",
    hide_submitted: bool = True,
    cluster_filter: str = "",
) -> bool:
    if cluster_filter and _clean(row.get("cluster_key")) != _clean(cluster_filter):
        return False
    has_draft = bool(_clean(row.get("draft_reply")) or _clean(row.get("draft_action")) in {"reject", "skip"})
    if draft_filter == "with_draft" and not has_draft:
        return False
    if draft_filter == "without_draft" and has_draft:
        return False
    if draft_filter == "auto_ready" and not bool(row.get("draft_auto_ready")):
        return False
    if draft_filter == "rule_based" and not bool(row.get("rule_title")):
        return False
    queue_status = _clean(row.get("queue_status"))
    if queue_filter == "ready" and (not has_draft or queue_status in {"queued", "processing"}):
        return False
    if queue_filter == "queued" and queue_status != "queued":
        return False
    if queue_filter == "processing" and queue_status != "processing":
        return False
    if queue_filter == "failed" and queue_status != "failed":
        return False
    if queue_filter == "sent" and not _row_question_is_processed(row):
        return False
    if queue_filter == "not_queued" and queue_status in {"queued", "processing"}:
        return False
    if hide_submitted and _row_question_is_processed(row):
        return False
    if not _question_matches_text(row, search_query):
        return False
    return True



def build_question_rows(
    page: int = 1,
    page_size: int = 50,
    sort_by: str = "newest",
    draft_filter: str = "all",
    queue_filter: str = "all",
    search_query: str = "",
    hide_submitted: bool = True,
    cluster_filter: str = "",
    force_refresh: bool = False,
) -> Dict[str, Any]:
    page = max(1, int(page or 1))
    page_size = page_size if page_size in QUESTION_PAGE_SIZE_OPTIONS else 50
    snapshot = get_question_snapshot(force_refresh=force_refresh)
    ignored_ids = load_question_ignored_ids()
    questions = [common.normalize_question(item) for item in snapshot.get("questions") or [] if _clean((item or {}).get("id")) not in ignored_ids]
    assignments, cluster_info = _build_cluster_assignments(questions)

    drafts = load_question_drafts()
    rules = load_question_rules()
    rules_sig = _rules_signature(rules)
    prompt_sig = _text_signature(load_question_prompt())
    queue = load_question_queue()
    queue_map = {_clean(item.get("question_id")): item for item in queue if _clean(item.get("question_id"))}
    archive = load_question_archive()
    archive_map = {_clean(item.get("id")): item for item in archive if _clean(item.get("id"))}

    all_rows: List[Dict[str, Any]] = []
    submitted_hidden = 0
    for question in questions:
        qid = _clean(question.get("id"))
        if not qid:
            continue
        cluster_key = assignments.get(qid) or "other"
        cluster_title = (cluster_info.get(cluster_key) or {}).get("title") or _cluster_title_from_key(cluster_key, [question], load_question_clusters())
        cluster_sort_order = (cluster_info.get(cluster_key) or {}).get("sort_order")
        question_sig = question_signature(question)
        draft_entry = drafts.get(qid) or {}
        compatible_draft = is_question_draft_compatible(draft_entry, question_sig, prompt_sig, rules_sig, cluster_key)
        reply_text = _clean_lines(draft_entry.get("reply"))
        draft_action = _normalize_question_action(draft_entry.get("action") or draft_entry.get("manual_action") or "answer")
        draft_confidence = float(draft_entry.get("confidence") or 0.0)
        draft_auto_ready = bool(draft_entry.get("auto_ready")) if compatible_draft else False
        draft_source = _clean(draft_entry.get("source"))
        draft_explanation = _clean_lines(draft_entry.get("explanation"))
        draft_ai_error_code = _clean(draft_entry.get("ai_error_code"))
        draft_ai_error_message = _clean_lines(draft_entry.get("ai_error_message"))
        manager_comment = _clean_lines(draft_entry.get("manager_comment"))
        draft_stale = bool(draft_entry.get("needs_regeneration")) or not compatible_draft
        cross_sell_items = [
            _clean(item)
            for item in (draft_entry.get("cross_sell_items") or [])
            if _clean(item)
        ]
        rule_titles: List[str] = [_clean(item) for item in (draft_entry.get("rule_titles") or []) if _clean(item)]
        rule_title = rule_titles[0] if rule_titles else ""
        if not rule_titles:
            matched_rules = match_question_rules(question, cluster_key, rules)
            if matched_rules:
                rule_titles = [_clean(item.get("title") or item.get("id")) for item in matched_rules[:5] if _clean(item.get("title") or item.get("id"))]
                rule_title = rule_titles[0] if rule_titles else ""

        product = question.get("productDetails", {}) or {}
        queue_entry = queue_map.get(qid, {})
        archive_entry = archive_map.get(qid, {})
        answer = question.get("answer") or {}
        row = {
            "id": qid,
            "product_name": _clean(product.get("productName")),
            "supplier_article": _clean(product.get("supplierArticle")),
            "brand_name": _clean(product.get("brandName")),
            "nm_id": _safe_int(product.get("nmId")),
            "question_text": _clean_lines(question.get("text")),
            "created_date": _clean(question.get("createdDate")),
            "state": _clean(question.get("state")),
            "answer_text": _clean_lines(answer.get("text")),
            "answer_state": _clean(answer.get("state")),
            "answer_editable": bool(answer.get("editable")),
            "answer_create_date": _clean(answer.get("createDate")),
            "was_viewed": bool(question.get("wasViewed")),
            "is_warned": bool(question.get("isWarned")),
            "cluster_key": cluster_key,
            "cluster_title": cluster_title,
            "cluster_sort_order": cluster_sort_order,
            "rule_title": rule_title,
            "rule_titles": rule_titles,
            "draft_reply": reply_text,
            "draft_action": draft_action or "answer",
            "draft_confidence": draft_confidence,
            "draft_auto_ready": draft_auto_ready,
            "draft_source": draft_source,
            "draft_explanation": draft_explanation,
            "draft_ai_error_code": draft_ai_error_code,
            "draft_ai_error_message": draft_ai_error_message,
            "draft_stale": draft_stale,
            "manager_comment": manager_comment,
            "cross_sell_items": cross_sell_items,
            "queue_status": _clean(queue_entry.get("status")),
            "queue_error": _clean(queue_entry.get("error")),
            "queue_sent_at": _clean(queue_entry.get("sent_at")),
            "archive_status": _clean(archive_entry.get("status")),
            "archive_sent_at": _clean(archive_entry.get("sent_at")),
            "source": "unanswered",
        }
        if hide_submitted and _row_question_is_processed(row):
            submitted_hidden += 1
        if _row_matches_filters(
            row,
            draft_filter=draft_filter,
            queue_filter=queue_filter,
            search_query=search_query,
            hide_submitted=hide_submitted,
            cluster_filter=cluster_filter,
        ):
            all_rows.append(row)

    filtered_rows = _sort_question_rows(all_rows, sort_by)
    total_filtered = len(filtered_rows)
    page_count = max(1, (total_filtered + page_size - 1) // page_size) if total_filtered else 1
    if page > page_count:
        page = page_count
    start = (page - 1) * page_size
    end = start + page_size
    page_rows = filtered_rows[start:end]

    cluster_rows: List[Dict[str, Any]] = []
    grouped_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in filtered_rows:
        grouped_rows[row["cluster_key"]].append(row)
    for cluster_key, members in grouped_rows.items():
        articles_counter = Counter(_clean(item.get("supplier_article")) for item in members if _clean(item.get("supplier_article")))
        matching_rules = (cluster_info.get(cluster_key) or {}).get("matching_rules") or []
        primary_rule = matching_rules[0] if matching_rules else None
        cluster_rows.append(
            {
                "key": cluster_key,
                "title": (cluster_info.get(cluster_key) or {}).get("title") or members[0].get("cluster_title") or "Кластер",
                "sort_order": (cluster_info.get(cluster_key) or {}).get("sort_order"),
                "count": len(members),
                "article_count": len(articles_counter),
                "articles": articles_counter.most_common(8),
                "sample_questions": [item.get("question_text") for item in members[:5]],
                "members": members[:12],
                "members_total": members,
                "with_draft": sum(1 for item in members if _clean(item.get("draft_reply")) or item.get("draft_action") in {"reject", "skip"}),
                "auto_ready": sum(1 for item in members if bool(item.get("draft_auto_ready"))),
                "needs_rule": not bool(matching_rules),
                "matching_rules": matching_rules[:6],
                "primary_rule": primary_rule,
            }
        )
    if any(item.get("sort_order") is not None for item in cluster_rows):
        cluster_rows.sort(
            key=lambda item: (
                item.get("sort_order") is None,
                item.get("sort_order") if item.get("sort_order") is not None else 10**9,
                _clean(item.get("title")),
            )
        )
    else:
        cluster_rows.sort(key=lambda item: (-int(item.get("count") or 0), _clean(item.get("title"))))

    return {
        "rows": page_rows,
        "all_rows": filtered_rows,
        "clusters": cluster_rows,
        "page": page,
        "page_size": page_size,
        "page_count": page_count,
        "has_prev": page > 1,
        "has_next": page < page_count,
        "total_filtered": total_filtered,
        "submitted_hidden": submitted_hidden,
        "count_unanswered": _safe_int(snapshot.get("count_unanswered")),
        "count_archive": _safe_int(snapshot.get("count_archive")),
        "draft_total": sum(1 for row in filtered_rows if _clean(row.get("draft_reply")) or row.get("draft_action") in {"reject", "skip"}),
        "queue_total": sum(1 for item in queue if _clean(item.get("status")) in QUESTION_QUEUE_OPEN_STATUSES),
        "cluster_total": len(cluster_rows),
        "snapshot_fetched_at": _clean(snapshot.get("fetched_at")),
        "raw_scanned": _safe_int(snapshot.get("raw_scanned")),
        "truncated": bool(snapshot.get("truncated")),
        "has_new_questions": bool(snapshot.get("has_new_questions")),
    }



def build_questions_context(
    page: int = 1,
    page_size: int = 50,
    sort_by: str = "cluster",
    draft_filter: str = "all",
    queue_filter: str = "all",
    search_query: str = "",
    hide_submitted: bool = True,
    cluster_filter: str = "",
    mode: str = "questions",
    force_refresh: bool = False,
) -> Dict[str, Any]:
    context = build_question_rows(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        draft_filter=draft_filter,
        queue_filter=queue_filter,
        search_query=search_query,
        hide_submitted=hide_submitted,
        cluster_filter=cluster_filter,
        force_refresh=force_refresh,
    )
    context["filters"] = {
        "sort": sort_by or "cluster",
        "draft": draft_filter or "all",
        "queue": queue_filter or "all",
        "q": search_query or "",
        "hide_submitted": bool(hide_submitted),
        "cluster": cluster_filter or "",
        "mode": mode or "questions",
        "page_size": page_size,
    }
    context["page_size_options"] = QUESTION_PAGE_SIZE_OPTIONS
    context["question_prompt_text"] = load_question_prompt()
    context["question_rules"] = load_question_rules()
    context["question_sync_meta"] = load_question_sync_meta()
    context["last_cluster_import"] = (context.get("question_sync_meta") or {}).get("last_cluster_import") or {}
    context["queue_open"] = context.get("queue_total", 0)
    context["draft_ready_total"] = sum(1 for row in context.get("all_rows", []) if row.get("draft_auto_ready"))
    context["archive_total"] = len(load_question_archive())
    selected_cluster = None
    if cluster_filter:
        for cluster in context.get("clusters", []):
            if _clean(cluster.get("key")) == _clean(cluster_filter):
                selected_cluster = cluster
                break
    context["selected_cluster"] = selected_cluster
    context["selected_cluster_rule"] = (selected_cluster or {}).get("primary_rule") or {}
    return context



def list_question_ids_for_cluster(cluster_key: str, hide_submitted: bool = False) -> List[str]:
    cluster_key = _clean(cluster_key)
    if not cluster_key:
        return []
    rows_ctx = build_question_rows(
        page=1,
        page_size=QUESTION_PAGE_SIZE_OPTIONS[-1],
        sort_by="newest",
        draft_filter="all",
        queue_filter="all",
        search_query="",
        hide_submitted=hide_submitted,
        cluster_filter=cluster_key,
        force_refresh=False,
    )
    return [_clean(row.get("id")) for row in rows_ctx.get("all_rows", []) if _clean(row.get("id"))]


def _question_for_queue_payload(question: Dict[str, Any]) -> Dict[str, Any]:
    product = question.get("productDetails", {}) or {}
    return {
        "product_name": _clean(product.get("productName")),
        "supplier_article": _clean(product.get("supplierArticle")),
        "brand_name": _clean(product.get("brandName")),
        "nm_id": _safe_int(product.get("nmId")),
        "created_date": _clean(question.get("createdDate")),
        "question_text": _clean_lines(question.get("text")),
        "state": _clean(question.get("state")),
        "was_viewed": bool(question.get("wasViewed")),
        "is_warned": bool(question.get("isWarned")),
    }



def queue_questions_from_form(form: Any) -> Tuple[int, List[str]]:
    _log("queue_add_start")
    selected_ids = [_clean(x) for x in form.getlist("selected_ids") if _clean(x)]
    if not selected_ids:
        return 0, ["Не выбрано ни одного вопроса."]
    snapshot = get_question_snapshot(force_refresh=False)
    snapshot_map = _question_snapshot_map(snapshot)
    queue = load_question_queue()
    queue_map = {_clean(item.get("question_id")): item for item in queue}
    drafts = load_question_drafts()
    notes: List[str] = []
    added = 0

    for question_id in selected_ids:
        question = snapshot_map.get(question_id)
        if not question:
            notes.append(f"{question_id}: вопрос не найден в текущем снимке.")
            continue
        draft = drafts.get(question_id, {})
        manager_comment = trim_question_reply(_clean_lines(form.get(f"manager_comment__{question_id}") or draft.get("manager_comment")), QUESTION_MANAGER_COMMENT_LIMIT)
        reply_text = trim_question_reply(_clean_lines(form.get(f"reply__{question_id}") or draft.get("reply")))
        action = _normalize_question_action(form.get(f"action__{question_id}") or draft.get("manual_action") or draft.get("action") or "answer")
        if action == "skip":
            notes.append(f"{question_id}: вопрос помечен как пропускаемый, в очередь не добавлен.")
            continue
        if action == "answer" and bool(draft.get("needs_regeneration")):
            notes.append(f"{question_id}: комментарий менеджера изменён — сначала перегенерируйте ответ или отредактируйте текст вручную.")
            continue
        if action == "answer" and len(reply_text) < QUESTION_MIN_REPLY_LENGTH:
            notes.append(f"{question_id}: сначала подготовьте или введите текст ответа.")
            continue
        if action == "reject" and not reply_text:
            reply_text = manager_comment or "Вопрос отклонён"

        updated_draft = {
            **draft,
            "reply": reply_text,
            "action": action,
            "manual_action": action,
            "manager_comment": manager_comment,
            "signature": question_signature(question),
            "generated_at": common.utc_now_iso(),
            "source": _clean(draft.get("source") or ("manual_edit" if reply_text else "manual")) or "manual_edit",
            "needs_regeneration": False,
        }
        drafts[question_id] = updated_draft

        payload = {
            "question_id": question_id,
            "signature": question_signature(question),
            "status": "queued",
            "queued_at": common.utc_now_iso(),
            "reply": reply_text,
            "action": action,
            "confidence": float(updated_draft.get("confidence") or 0.0),
            "auto_ready": bool(updated_draft.get("auto_ready")),
            "reply_source": _clean(updated_draft.get("source") or "queued"),
            "rule_ids": list(updated_draft.get("rule_ids") or []),
            "cluster_key": _clean(updated_draft.get("cluster_key")),
            "cluster_title": _clean(updated_draft.get("cluster_title")),
            "manager_comment": manager_comment,
            "cross_sell_items": list(updated_draft.get("cross_sell_items") or []),
            "question": _question_for_queue_payload(question),
        }

        if question_id in queue_map and _clean(queue_map[question_id].get("status")) in {"queued", "processing", "failed"}:
            queue_map[question_id].update(payload)
            notes.append(f"{question_id}: запись в очереди обновлена.")
        else:
            queue.append(payload)
            queue_map[question_id] = payload
            added += 1

    save_question_queue(queue)
    save_question_drafts(drafts)
    _log("queue_add_finish", added=added, notes_count=len(notes), queued_total=len(queue))
    return added, notes



def upsert_question_archive_record(record: Dict[str, Any]) -> None:
    archive = load_question_archive()
    question_id = _clean(record.get("id"))
    updated = False
    for idx, item in enumerate(archive):
        if _clean(item.get("id")) == question_id:
            archive[idx] = {**item, **record}
            updated = True
            break
    if not updated:
        archive.append(record)
    save_question_archive(archive)



def _send_question_action(question_id: str, action: str, reply_text: str) -> None:
    if action == "reject":
        common.patch_question({"id": question_id, "text": reply_text or "Вопрос отклонён", "state": "none"})
        return
    common.patch_question({"id": question_id, "text": reply_text, "state": "wbRu"})



def process_question_queue(max_items: int = 0, auto_only: bool = False) -> Dict[str, Any]:
    _log("process_queue_start", max_items=max_items, auto_only=bool(auto_only))
    queue = load_question_queue()
    pending_indexes = [idx for idx, item in enumerate(queue) if _clean(item.get("status")) in {"queued", "failed"}]
    if auto_only:
        pending_indexes = [idx for idx in pending_indexes if bool(queue[idx].get("auto_ready"))]
    if max_items > 0:
        pending_indexes = pending_indexes[:max_items]

    total = len(pending_indexes)
    sent = 0
    failed = 0
    rejected = 0
    processed = 0
    if total <= 0:
        background_jobs.progress(
            stage='process_questions_done',
            message='В очереди вопросов нет задач для отправки.',
            percent=100,
            total=0,
            processed=0,
            sent=0,
            rejected=0,
            failed=0,
        )
        message = 'Очередь вопросов обработана. Ответов отправлено: 0. Отклонено: 0. Ошибок: 0. Всего: 0.'
        _log("process_queue_finish", processed=0, sent=0, rejected=0, failed=0)
        return {
            "message": message,
            "sent": 0,
            "rejected": 0,
            "failed": 0,
            "processed": 0,
        }

    background_jobs.progress(
        stage='process_questions_start',
        message='Отправка очереди вопросов запущена.',
        percent=0,
        total=total,
        processed=0,
        sent=0,
        rejected=0,
        failed=0,
    )

    for index, idx in enumerate(pending_indexes, start=1):
        item = queue[idx]
        question_id = _clean(item.get("question_id"))
        action = _clean(item.get("action") or "answer") or "answer"
        reply_text = trim_question_reply(_clean_lines(item.get("reply")), QUESTION_MAX_REPLY_LENGTH)
        if action == "answer" and len(reply_text) < QUESTION_MIN_REPLY_LENGTH:
            item["status"] = "failed"
            item["error"] = "Текст ответа пустой или слишком короткий."
            failed += 1
            processed += 1
            save_question_queue(queue)
            background_jobs.progress(
                stage='process_questions_item_error',
                message=f'Вопрос {question_id}: текст ответа пустой или слишком короткий.',
                percent=round(index * 100 / total, 2),
                current=index,
                total=total,
                processed=processed,
                sent=sent,
                rejected=rejected,
                failed=failed,
                question_id=question_id,
                action=action,
                status='failed',
            )
            continue
        item["status"] = "processing"
        item["error"] = ""
        save_question_queue(queue)
        try:
            _send_question_action(question_id, action, reply_text)
            terminal_status = "rejected" if action == "reject" else "sent"
            item["status"] = terminal_status
            item["sent_at"] = common.utc_now_iso()
            item["error"] = ""
            question_meta = item.get("question", {}) or {}
            upsert_question_archive_record(
                {
                    "id": question_id,
                    "status": terminal_status,
                    "sent_at": item["sent_at"],
                    "action": action,
                    "product_name": _clean(question_meta.get("product_name")),
                    "supplier_article": _clean(question_meta.get("supplier_article")),
                    "brand_name": _clean(question_meta.get("brand_name")),
                    "nm_id": _safe_int(question_meta.get("nm_id")),
                    "created_date": _clean(question_meta.get("created_date")),
                    "question_text": _clean_lines(question_meta.get("question_text")),
                    "reply": reply_text,
                    "reply_source": _clean(item.get("reply_source") or "queued"),
                    "rule_ids": item.get("rule_ids", []),
                    "cluster_key": _clean(item.get("cluster_key")),
                    "cluster_title": _clean(item.get("cluster_title")),
                    "confidence": float(item.get("confidence") or 0.0),
                }
            )
            remove_question_draft(question_id)
            if action == "reject":
                rejected += 1
            else:
                sent += 1
            processed += 1
            save_question_queue(queue)
            background_jobs.progress(
                stage='process_questions_item',
                message=(
                    f'Вопрос {question_id} обработан: отклонён.'
                    if action == 'reject'
                    else f'Вопрос {question_id} отправлен в WB.'
                ),
                percent=round(index * 100 / total, 2),
                current=index,
                total=total,
                processed=processed,
                sent=sent,
                rejected=rejected,
                failed=failed,
                question_id=question_id,
                action=action,
                status=terminal_status,
            )
            time.sleep(QUESTION_API_SEND_DELAY_SECONDS)
        except Exception as exc:
            item["status"] = "failed"
            item["error"] = str(exc)
            failed += 1
            processed += 1
            save_question_queue(queue)
            background_jobs.progress(
                stage='process_questions_item_error',
                message=f'Ошибка отправки вопроса {question_id}: {exc}',
                percent=round(index * 100 / total, 2),
                current=index,
                total=total,
                processed=processed,
                sent=sent,
                rejected=rejected,
                failed=failed,
                question_id=question_id,
                action=action,
                status='failed',
            )
            if "429" in str(exc):
                time.sleep(max(2.0, QUESTION_API_SEND_DELAY_SECONDS * 3))

    message = f"Очередь вопросов обработана. Ответов отправлено: {sent}. Отклонено: {rejected}. Ошибок: {failed}. Всего: {processed}."
    background_jobs.progress(
        stage='process_questions_done',
        message=message,
        percent=100,
        current=total,
        total=total,
        processed=processed,
        sent=sent,
        rejected=rejected,
        failed=failed,
    )
    _log("process_queue_finish", processed=processed, sent=sent, rejected=rejected, failed=failed)
    return {
        "message": message,
        "sent": sent,
        "rejected": rejected,
        "failed": failed,
        "processed": processed,
    }



def process_auto_question_rules(limit: int = 0, send_now: bool = False) -> Dict[str, Any]:
    _log("auto_run_start", limit=limit, send_now=bool(send_now))
    snapshot = get_question_snapshot(force_refresh=True)
    snapshot_map = _question_snapshot_map(snapshot)
    ignored_ids = load_question_ignored_ids()
    snapshot_map = {qid: item for qid, item in snapshot_map.items() if qid not in ignored_ids}
    drafts = load_question_drafts()
    prepared = 0
    queued = 0
    skipped = 0
    selected_ids: List[str] = []
    for question_id, question in snapshot_map.items():
        entry = generate_question_draft(question, force=False)
        if not entry.get("auto_ready"):
            skipped += 1
            continue
        prepared += 1
        selected_ids.append(question_id)
        if limit > 0 and len(selected_ids) >= limit:
            break
    if selected_ids:
        class _PseudoForm:
            def __init__(self, ids: List[str], drafts_map: Dict[str, Dict[str, Any]]):
                self._ids = ids
                self._drafts_map = drafts_map
            def getlist(self, name: str) -> List[str]:
                return list(self._ids) if name == "selected_ids" else []
            def get(self, name: str, default: Any = None) -> Any:
                if name.startswith("reply__"):
                    qid = name.split("__", 1)[1]
                    return (self._drafts_map.get(qid) or {}).get("reply") or default
                if name.startswith("action__"):
                    qid = name.split("__", 1)[1]
                    return (self._drafts_map.get(qid) or {}).get("action") or default
                return default
        added, _ = queue_questions_from_form(_PseudoForm(selected_ids, load_question_drafts()))
        queued = added
    result = {
        "prepared": prepared,
        "queued": queued,
        "skipped": skipped,
        "processed": 0,
        "sent": 0,
        "rejected": 0,
        "failed": 0,
        "message": f"Автообработка: подготовлено {prepared}, в очередь добавлено {queued}, пропущено {skipped}.",
    }
    if send_now and queued:
        process_result = process_question_queue(max_items=limit if limit > 0 else 0, auto_only=True)
        result.update(process_result)
        result["message"] = (
            f"Автообработка завершена. Подготовлено {prepared}, в очередь добавлено {queued}. "
            f"Отправлено {process_result.get('sent', 0)}, отклонено {process_result.get('rejected', 0)}, ошибок {process_result.get('failed', 0)}."
        )
    _log("auto_run_finish", prepared=prepared, queued=queued, skipped=skipped, sent=result.get("sent"), rejected=result.get("rejected"), failed=result.get("failed"))
    return result



def make_manual_cluster_key(title: str) -> str:
    clean_title = _clean_lines(title) or "Новый подкластер"
    return "manual::" + hashlib.sha1(clean_title.encode("utf-8")).hexdigest()[:12]



def reassign_cluster_members(question_ids: List[str], target_title: str) -> str:
    question_ids = [_clean(item) for item in question_ids if _clean(item)]
    if not question_ids:
        raise ValueError("Не выбраны вопросы для переноса.")
    cluster_key = make_manual_cluster_key(target_title)
    state = load_question_clusters()
    assignments = state.setdefault("assignments", {})
    cluster_meta = state.setdefault("cluster_meta", {})
    cluster_meta.setdefault(cluster_key, {})
    cluster_meta[cluster_key]["title_override"] = _clean_lines(target_title) or "Подкластер"
    cluster_meta[cluster_key]["updated_at"] = common.utc_now_iso()
    for question_id in question_ids:
        assignments[question_id] = {
            "cluster_key": cluster_key,
            "updated_at": common.utc_now_iso(),
            "source": "manual_move",
        }
    save_question_clusters(state)
    return cluster_key



def reset_cluster_assignments(question_ids: List[str]) -> int:
    question_ids = [_clean(item) for item in question_ids if _clean(item)]
    if not question_ids:
        return 0
    state = load_question_clusters()
    assignments = state.setdefault("assignments", {})
    removed = 0
    for question_id in question_ids:
        if question_id in assignments:
            removed += 1
            del assignments[question_id]
    save_question_clusters(state)
    return removed



def auto_split_cluster_by_article(cluster_key: str) -> int:
    snapshot = get_question_snapshot(force_refresh=False)
    rows = build_question_rows(cluster_filter=cluster_key, hide_submitted=False, page_size=300, page=1)
    members = rows.get("all_rows") or []
    if not members:
        return 0
    by_article: Dict[str, List[str]] = defaultdict(list)
    for row in members:
        article = _clean(row.get("supplier_article")) or f"nm-{_safe_int(row.get('nm_id'))}"
        by_article[article].append(_clean(row.get("id")))
    changed = 0
    for article, question_ids in by_article.items():
        if len(question_ids) <= 1:
            continue
        reassign_cluster_members(question_ids, f"{(rows.get('clusters') or [{}])[0].get('title', 'Подкластер')} — {article}")
        changed += len(question_ids)
    return changed



def _next_rule_id(rules: Dict[str, Any]) -> str:
    base = len(rules.get("rules") or []) + 1
    while True:
        candidate = f"qr-{base:04d}"
        if not any(_clean(item.get("id")) == candidate for item in rules.get("rules") or [] if isinstance(item, dict)):
            return candidate
        base += 1



def _extract_reference_text(filename: str, data: bytes) -> str:
    from io import BytesIO
    import zipfile
    from xml.etree import ElementTree as ET

    suffix = Path(filename or "").suffix.lower()
    raw = data or b""
    if not raw:
        return ""

    text = ""
    if suffix in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
        try:
            import openpyxl  # type: ignore
            wb = openpyxl.load_workbook(BytesIO(raw), read_only=True, data_only=True)
            parts: List[str] = []
            for sheet in wb.worksheets[:6]:
                parts.append(f"[Лист] {sheet.title}")
                for row_idx, row in enumerate(sheet.iter_rows(values_only=True), start=1):
                    values = [str(value).strip() for value in row if value not in (None, "")]
                    if values:
                        parts.append(" | ".join(values))
                    if row_idx >= 250:
                        break
            text = "\n".join(parts)
        except Exception:
            text = ""
    elif suffix == ".docx":
        try:
            with zipfile.ZipFile(BytesIO(raw)) as zf:
                xml = zf.read("word/document.xml")
            tree = ET.fromstring(xml)
            chunks: List[str] = []
            for node in tree.iter():
                if node.text and node.text.strip():
                    chunks.append(node.text.strip())
            text = "\n".join(chunks)
        except Exception:
            text = ""
    elif suffix == ".pdf":
        try:
            from pypdf import PdfReader  # type: ignore
            reader = PdfReader(BytesIO(raw))
            pages: List[str] = []
            for page in reader.pages[:20]:
                pages.append((page.extract_text() or "").strip())
            text = "\n".join(chunk for chunk in pages if chunk)
        except Exception:
            text = ""

    if not text:
        for encoding in ["utf-8", "utf-8-sig", "cp1251", "latin-1"]:
            try:
                text = raw.decode(encoding)
                break
            except Exception:
                text = ""
    if not text:
        return ""

    text = text.strip()
    if suffix == ".json":
        try:
            parsed = json.loads(text)
            text = json.dumps(parsed, ensure_ascii=False, indent=2)
        except Exception:
            pass
    return trim_question_reply(text, 12000)



def build_rule_instruction_from_reference(cluster_title: str, filename: str, reference_text: str, manager_notes: str = "") -> str:
    if not reference_text:
        return _clean_lines(manager_notes)
    excerpt = trim_question_reply(reference_text, 8000)
    system_prompt = (
        "Ты помогаешь менеджеру Wildberries превратить сырой справочный файл в рабочее правило для ответов на вопросы покупателей. "
        "Верни только короткую практическую инструкцию для менеджера на русском без преамбулы."
    )
    user_prompt = f"""
Кластер: {cluster_title}
Имя файла: {filename}
Пожелания менеджера:
{_clean_lines(manager_notes) or '—'}

Содержимое файла:
{excerpt}

Сделай рабочую инструкцию так, чтобы её можно было прямо сохранить в правило и использовать для генерации ответов.
""".strip()
    try:
        return trim_question_reply(
            common.call_ai(
                [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                model=common.OPENAI_MODEL,
                temperature=0.2,
            ),
            2500,
        )
    except Exception:
        return _clean_lines(manager_notes) or trim_question_reply(excerpt, 1500)



def upsert_question_rule(
    *,
    rule_id: str = "",
    title: str,
    cluster_key: str,
    question_keywords_any: List[str],
    question_keywords_all: List[str],
    article_keywords_any: List[str],
    article_keywords_all: List[str],
    manager_instruction: str,
    answer_mode: str,
    answer_template: str,
    allow_auto_send: bool,
    enabled: bool = True,
    priority: int = 100,
) -> Dict[str, Any]:
    rules = load_question_rules()
    rules.setdefault("rules", [])
    normalized_mode = _clean(answer_mode).lower() or "ai"
    payload = {
        "id": _clean(rule_id) or _next_rule_id(rules),
        "title": _clean(title) or "Новое правило",
        "cluster_key": _clean(cluster_key),
        "question_keywords_any": [_normalize_search_text(item) for item in question_keywords_any if _normalize_search_text(item)],
        "question_keywords_all": [_normalize_search_text(item) for item in question_keywords_all if _normalize_search_text(item)],
        "article_keywords_any": [_normalize_search_text(item) for item in article_keywords_any if _normalize_search_text(item)],
        "article_keywords_all": [_normalize_search_text(item) for item in article_keywords_all if _normalize_search_text(item)],
        "manager_instruction": _clean_lines(manager_instruction),
        "answer_mode": normalized_mode if normalized_mode in {"ai", "template", "reject", "skip"} else "ai",
        "answer_template": trim_question_reply(_clean_lines(answer_template), QUESTION_MAX_REPLY_LENGTH),
        "allow_auto_send": bool(allow_auto_send),
        "enabled": bool(enabled),
        "priority": int(priority or 100),
        "updated_at": common.utc_now_iso(),
    }
    existing_index = None
    for idx, rule in enumerate(rules.get("rules") or []):
        if _clean(rule.get("id")) == payload["id"]:
            existing_index = idx
            payload.setdefault("created_at", _clean(rule.get("created_at")) or common.utc_now_iso())
            break
    payload.setdefault("created_at", common.utc_now_iso())
    if existing_index is None:
        rules["rules"].append(payload)
    else:
        rules["rules"][existing_index] = {**rules["rules"][existing_index], **payload}
    save_question_rules(rules)
    _log("rule_saved", rule_id=payload.get("id"), enabled=bool(payload.get("enabled")), cluster_key=payload.get("cluster_key"), answer_mode=payload.get("answer_mode"))
    return payload



def toggle_question_rule(rule_id: str, enabled: bool) -> bool:
    rules = load_question_rules()
    changed = False
    for rule in rules.get("rules") or []:
        if _clean(rule.get("id")) == _clean(rule_id):
            rule["enabled"] = bool(enabled)
            rule["updated_at"] = common.utc_now_iso()
            changed = True
            break
    if changed:
        save_question_rules(rules)
    return changed



def parse_rule_form(form: Any, files: Any = None) -> Dict[str, Any]:
    title = _clean(form.get("title"))
    cluster_key = _clean(form.get("cluster_key"))
    manager_instruction = _clean_lines(form.get("manager_instruction"))
    if files is not None:
        reference_file = files.get("reference_file")
    else:
        reference_file = None
    if reference_file is not None and getattr(reference_file, "filename", ""):
        reference_bytes = reference_file.read()
        reference_file.stream.seek(0)
        reference_text = _extract_reference_text(reference_file.filename, reference_bytes)
        if reference_text:
            manager_instruction = build_rule_instruction_from_reference(
                title or cluster_key or "Кластер",
                reference_file.filename,
                reference_text,
                manager_instruction,
            )
    return {
        "rule_id": _clean(form.get("rule_id")),
        "title": title,
        "cluster_key": cluster_key,
        "question_keywords_any": _split_keywords(form.get("question_keywords_any")),
        "question_keywords_all": _split_keywords(form.get("question_keywords_all")),
        "article_keywords_any": _split_keywords(form.get("article_keywords_any")),
        "article_keywords_all": _split_keywords(form.get("article_keywords_all")),
        "manager_instruction": manager_instruction,
        "answer_mode": _clean(form.get("answer_mode") or "ai") or "ai",
        "answer_template": _clean_lines(form.get("answer_template")),
        "allow_auto_send": bool(form.get("allow_auto_send")),
        "enabled": bool(form.get("enabled", 1)),
        "priority": _safe_int(form.get("priority"), 100),
    }
