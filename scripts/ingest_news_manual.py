from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from uuid import uuid4

from app.db.sqlite import get_conn


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Path JSON con lista news")
    p.add_argument("--source", default="MANUAL", help="es: Gazzetta, SkySport")
    p.add_argument("--reliability", type=float, default=0.6)
    p.add_argument("--match_id", default=None)
    p.add_argument("--team", default=None)
    p.add_argument("--player", default=None)
    p.add_argument("--event_type", default=None)
    p.add_argument("--published_at_utc", default=None, help="ISO string; default now")
    args = p.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)

    items = data["news"] if isinstance(data, dict) and "news" in data else data
    if not isinstance(items, list):
        raise SystemExit("Input JSON non valido: deve essere una lista oppure un dict con chiave 'news'.")

    rows = []
    for item in items:
        news_id = item.get("news_id") or str(uuid4())
        source = item.get("source") or args.source
        title = item.get("title")
        if not title:
            raise SystemExit("Ogni news deve avere 'title'.")
        url = item.get("url")
        published_at = item.get("published_at_utc") or args.published_at_utc or now_utc_iso()
        reliability = float(item.get("reliability_score", args.reliability))
        related_match_id = item.get("match_id") or args.match_id
        related_team = item.get("team") or args.team
        related_player = item.get("player") or args.player
        event_type = item.get("event_type") or args.event_type
        summary = item.get("summary")
        raw_json = json.dumps(item, ensure_ascii=True)

        rows.append((
            news_id,
            source,
            title,
            url,
            published_at,
            reliability,
            related_match_id,
            related_team,
            related_player,
            event_type,
            summary,
            raw_json,
        ))

    with get_conn() as c:
        c.executemany(
            """
            INSERT INTO news_articles (
              news_id, source, title, url, published_at_utc, reliability_score,
              related_match_id, related_team, related_player, event_type, summary, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        c.commit()

    print(f"OK: inserted {len(rows)} news")


if __name__ == "__main__":
    main()
