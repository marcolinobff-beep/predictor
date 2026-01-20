from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4
from typing import Optional, Dict, Any

from app.db.sqlite import get_conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_session(session_id: str) -> Dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM chat_sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE chat_sessions SET updated_at_utc = ? WHERE session_id = ?",
                (_now_iso(), session_id),
            )
            return dict(row)

        payload = {
            "session_id": session_id,
            "created_at_utc": _now_iso(),
            "updated_at_utc": _now_iso(),
            "last_competition": None,
            "last_day_utc": None,
            "last_match_id": None,
            "last_intent": None,
            "meta_json": "{}",
        }
        conn.execute(
            """
            INSERT INTO chat_sessions
              (session_id, created_at_utc, updated_at_utc, last_competition,
               last_day_utc, last_match_id, last_intent, meta_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["session_id"],
                payload["created_at_utc"],
                payload["updated_at_utc"],
                payload["last_competition"],
                payload["last_day_utc"],
                payload["last_match_id"],
                payload["last_intent"],
                payload["meta_json"],
            ),
        )
        return payload


def update_session(
    session_id: str,
    last_competition: Optional[str] = None,
    last_day_utc: Optional[str] = None,
    last_match_id: Optional[str] = None,
    last_intent: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM chat_sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if not row:
            return
        meta_json = row["meta_json"] or "{}"
        try:
            meta_data = json.loads(meta_json)
        except Exception:
            meta_data = {}
        if meta:
            meta_data.update(meta)
        conn.execute(
            """
            UPDATE chat_sessions
            SET updated_at_utc = ?, last_competition = ?, last_day_utc = ?,
                last_match_id = ?, last_intent = ?, meta_json = ?
            WHERE session_id = ?
            """,
            (
                _now_iso(),
                last_competition if last_competition is not None else row["last_competition"],
                last_day_utc if last_day_utc is not None else row["last_day_utc"],
                last_match_id if last_match_id is not None else row["last_match_id"],
                last_intent if last_intent is not None else row["last_intent"],
                json.dumps(meta_data, ensure_ascii=True),
                session_id,
            ),
        )


def add_message(
    session_id: str,
    role: str,
    content: str,
    meta: Optional[Dict[str, Any]] = None,
) -> str:
    message_id = str(uuid4())
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO chat_messages
              (message_id, session_id, role, content, created_at_utc, meta_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                session_id,
                role,
                content,
                _now_iso(),
                json.dumps(meta or {}, ensure_ascii=True),
            ),
        )
    return message_id


def get_recent_messages(session_id: str, limit: int = 6) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT role, content, created_at_utc
            FROM chat_messages
            WHERE session_id = ?
            ORDER BY created_at_utc DESC
            LIMIT ?
            """,
            (session_id, limit),
        ).fetchall()
    items = [
        {"role": r["role"], "content": r["content"], "created_at_utc": r["created_at_utc"]}
        for r in rows
    ]
    return list(reversed(items))
