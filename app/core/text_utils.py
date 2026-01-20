from __future__ import annotations

import html
import re
import unicodedata

_MOJIBAKE_MARKERS = (
    "\u00c3",
    "\u00c2",
    "\u00e2",
    "\u20ac",
    "\u2122",
    "\u0153",
    "\u017e",
    "\ufffd",
)


def fix_mojibake(value: str) -> str:
    if not value:
        return value
    if not any(marker in value for marker in _MOJIBAKE_MARKERS):
        return value
    fixed = value
    for enc in ("latin-1", "cp1252"):
        try:
            candidate = fixed.encode(enc).decode("utf-8")
        except UnicodeError:
            continue
        if candidate and candidate != fixed:
            fixed = candidate
    if fixed != value and any(marker in fixed for marker in _MOJIBAKE_MARKERS):
        try:
            fixed = fixed.encode("latin-1").decode("utf-8")
        except UnicodeError:
            pass
    return fixed if fixed else value


def clean_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def clean_person_name(value: str | None) -> str | None:
    if not value:
        return None
    text = html.unescape(str(value))
    text = fix_mojibake(text)
    return clean_whitespace(text) if text else None


def strip_accents(value: str) -> str:
    if not value:
        return value
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_person_name(value: str) -> str:
    text = clean_person_name(value) or ""
    if not text:
        return ""
    text = strip_accents(text)
    text = text.replace("'", " ").replace("-", " ")
    text = re.sub(r"[^a-z ]+", " ", text.lower()).strip()
    tokens = [t for t in text.split() if t]
    if len(tokens) > 1:
        tokens = [t for t in tokens if len(t) > 1]
    return " ".join(tokens)
