from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

import requests

from app.core.config import settings


_CACHE = {"last_error": None, "last_ok": None}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _should_skip() -> bool:
    last_error = _CACHE.get("last_error")
    if not last_error:
        return False
    return (_now() - last_error) < timedelta(seconds=60)


def llm_enabled() -> bool:
    if not settings.llm_enabled:
        return False
    if _should_skip():
        return False
    if not settings.llm_model:
        return False
    return True


def _ollama_generate(prompt: str) -> Optional[str]:
    base = settings.llm_base_url.rstrip("/")
    url = f"{base}/api/generate"
    payload = {
        "model": settings.llm_model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": float(settings.llm_temperature),
            "num_predict": int(settings.llm_max_tokens),
        },
    }
    resp = requests.post(url, json=payload, timeout=float(settings.llm_timeout))
    resp.raise_for_status()
    data = resp.json()
    text = (data.get("response") or "").strip()
    return text or None


def rewrite_answer(
    query: str,
    base_answer: str,
    intent: str,
    recent_messages: Optional[list[dict]] = None,
) -> tuple[str, bool]:
    if not llm_enabled():
        return base_answer, False

    system = (
        "Sei un assistente calcistico professionista. "
        "Rispondi in modo conversazionale e naturale, senza inventare dati. "
        "Usa SOLO le informazioni nel BASE_ANSWER. "
        "Non cambiare numeri, percentuali, quote o nomi. "
        "Non aggiungere nuove statistiche. "
        "Se BASE_ANSWER e' breve, aggiungi una domanda di follow-up."
    )

    history = ""
    if recent_messages:
        lines = []
        for item in recent_messages[-6:]:
            role = item.get("role", "user")
            content = item.get("content", "")
            lines.append(f"{role.upper()}: {content}")
        history = "\n".join(lines)

    prompt = "\n".join([
        system,
        "",
        f"INTENT: {intent}",
        f"QUERY: {query}",
        "BASE_ANSWER:",
        base_answer,
        "",
        "CONTEXT_HISTORY:",
        history or "n/a",
        "",
        "Riscrivi la risposta in 1-3 paragrafi brevi."
    ])

    try:
        if settings.llm_provider.lower() == "ollama":
            text = _ollama_generate(prompt)
        else:
            text = None
        if text:
            _CACHE["last_ok"] = _now()
            return text, True
    except Exception:
        _CACHE["last_error"] = _now()
        return base_answer, False

    return base_answer, False
