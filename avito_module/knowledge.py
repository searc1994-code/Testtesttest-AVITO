from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Sequence

from .compat import clean_text

_TOKEN_RE = re.compile(r"[0-9A-Za-zА-Яа-яЁё]{2,}")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


@dataclass(slots=True)
class KnowledgeHit:
    doc_id: int
    chunk_id: int
    title: str
    kind: str
    item_id: str
    item_title: str
    score: float
    excerpt: str
    source_name: str = ""
    source_url: str = ""
    tags: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    def as_meta(self) -> Dict[str, Any]:
        return {
            "doc_id": int(self.doc_id),
            "chunk_id": int(self.chunk_id),
            "title": self.title,
            "kind": self.kind,
            "item_id": self.item_id,
            "item_title": self.item_title,
            "score": round(float(self.score or 0.0), 4),
            "excerpt": self.excerpt,
            "source_name": self.source_name,
            "source_url": self.source_url,
            "tags": list(self.tags),
            "meta": dict(self.meta or {}),
        }


@dataclass(slots=True)
class MediaSuggestion:
    asset_id: int
    media_kind: str
    title: str
    caption: str
    item_id: str
    item_title: str
    mime_type: str
    external_url: str
    local_path: str
    preview_url: str = ""
    score: float = 0.0
    tags: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    def as_meta(self) -> Dict[str, Any]:
        return {
            "asset_id": int(self.asset_id),
            "media_kind": self.media_kind,
            "title": self.title,
            "caption": self.caption,
            "item_id": self.item_id,
            "item_title": self.item_title,
            "mime_type": self.mime_type,
            "external_url": self.external_url,
            "local_path": self.local_path,
            "preview_url": self.preview_url,
            "score": round(float(self.score or 0.0), 4),
            "tags": list(self.tags),
            "meta": dict(self.meta or {}),
        }


def tokenize_text(text: Any) -> List[str]:
    text = clean_text(text).lower()
    if not text:
        return []
    return _TOKEN_RE.findall(text)



def normalize_for_search(text: Any) -> str:
    tokens = tokenize_text(text)
    return " ".join(tokens)



def split_text_into_chunks(text: Any, *, max_chars: int = 900, overlap_chars: int = 120) -> List[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    max_chars = max(200, int(max_chars or 900))
    overlap_chars = max(0, min(int(overlap_chars or 120), max_chars // 2))
    if len(raw) <= max_chars:
        return [raw]
    parts: List[str] = []
    cursor = 0
    while cursor < len(raw):
        target = min(len(raw), cursor + max_chars)
        slice_text = raw[cursor:target]
        if target < len(raw):
            sentence_breaks = [m.end() for m in _SENTENCE_SPLIT_RE.finditer(slice_text)]
            newline_break = slice_text.rfind("\n")
            if sentence_breaks:
                target = cursor + sentence_breaks[-1]
            elif newline_break > max_chars * 0.55:
                target = cursor + newline_break + 1
        part = raw[cursor:target].strip()
        if part:
            parts.append(part)
        if target >= len(raw):
            break
        cursor = max(0, target - overlap_chars)
    return parts



def score_match(
    query: Any,
    *,
    text: Any,
    title: Any = "",
    item_id: Any = "",
    item_title: Any = "",
    tags: Sequence[str] | None = None,
    media_kind: str = "",
) -> float:
    query_text = clean_text(query)
    if not query_text:
        return 0.0
    query_terms = tokenize_text(query_text)
    if not query_terms:
        return 0.0

    text_norm = normalize_for_search(text)
    title_norm = normalize_for_search(title)
    item_id_norm = normalize_for_search(item_id)
    item_title_norm = normalize_for_search(item_title)
    tags_norm = " ".join(normalize_for_search(tag) for tag in (tags or []))

    corpus_terms = set(tokenize_text(" ".join([text_norm, title_norm, item_id_norm, item_title_norm, tags_norm, clean_text(media_kind)])))
    overlap = sum(1 for term in query_terms if term in corpus_terms)
    score = overlap / max(1, len(query_terms))

    query_norm = normalize_for_search(query_text)
    if query_norm and query_norm in text_norm:
        score += 1.0
    if query_norm and query_norm in title_norm:
        score += 0.8

    if item_id_norm and item_id_norm in query_norm:
        score += 0.9

    item_overlap = sum(1 for term in query_terms if term in set(tokenize_text(item_title_norm)))
    if item_overlap:
        score += min(0.8, item_overlap * 0.18)

    tag_overlap = sum(1 for term in query_terms if term in set(tokenize_text(tags_norm)))
    if tag_overlap:
        score += min(0.4, tag_overlap * 0.08)

    if media_kind and media_kind.lower() in query_norm:
        score += 0.25

    return round(float(score), 6)



def compact_excerpt(text: Any, query: Any = "", *, max_chars: int = 220) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    max_chars = max(60, int(max_chars or 220))
    query_terms = tokenize_text(query)
    lowered = raw.lower()
    pivot = 0
    for term in query_terms:
        idx = lowered.find(term.lower())
        if idx >= 0:
            pivot = idx
            break
    start = max(0, pivot - max_chars // 3)
    end = min(len(raw), start + max_chars)
    excerpt = raw[start:end].strip()
    if start > 0:
        excerpt = "…" + excerpt
    if end < len(raw):
        excerpt = excerpt.rstrip() + "…"
    return excerpt
