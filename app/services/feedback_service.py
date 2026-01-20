from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4
from typing import Optional, Dict, Any

from app.db.sqlite import get_conn


def add_chat_feedback(
    query: str,
    response: str,
    label: str,
    notes: Optional[str] = None,
    match_id: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> str:
    feedback_id = str(uuid4())
    created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    meta_json = json.dumps(meta or {}, ensure_ascii=True)

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO chat_feedback
              (feedback_id, created_at_utc, query, response, label, notes, match_id, meta_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                feedback_id,
                created_at,
                query,
                response,
                label,
                notes,
                match_id,
                meta_json,
            ),
        )

    return feedback_id
