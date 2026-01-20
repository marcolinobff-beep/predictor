from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/feedback/chat_feedback.jsonl")
    ap.add_argument("--label", default=None)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    rows = []
    with get_conn() as conn:
        sql = """
            SELECT feedback_id, created_at_utc, query, response, label, notes, match_id, meta_json
            FROM chat_feedback
        """
        params: list[Any] = []
        if args.label:
            sql += " WHERE label = ?"
            params.append(args.label)
        sql += " ORDER BY created_at_utc DESC"
        if args.limit and args.limit > 0:
            sql += " LIMIT ?"
            params.append(args.limit)
        rows = conn.execute(sql, params).fetchall()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for r in rows:
            meta = {}
            try:
                meta = json.loads(r["meta_json"]) if r["meta_json"] else {}
            except Exception:
                meta = {}
            payload = {
                "feedback_id": r["feedback_id"],
                "created_at_utc": r["created_at_utc"],
                "query": r["query"],
                "response": r["response"],
                "label": r["label"],
                "notes": r["notes"],
                "match_id": r["match_id"],
                "meta": meta,
            }
            f.write(json.dumps(payload, ensure_ascii=True) + "\n")

    print(f"OK: exported {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
