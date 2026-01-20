from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from uuid import uuid4
from typing import Dict, Any

from app.db.sqlite import get_conn


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def detect_odds_table_cols(conn) -> Dict[str, bool]:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(odds_quotes)").fetchall()]
    return {
        "has_quote_id": "quote_id" in cols,
        "has_batch_id": "batch_id" in cols,
        "has_retrieved": "retrieved_at_utc" in cols,
        "has_source_id": "source_id" in cols,
        "has_reliability_score": "reliability_score" in cols,
        "has_ttl_seconds": "ttl_seconds" in cols,
        "has_cache_hit": "cache_hit" in cols,
        "has_raw_ref": "raw_ref" in cols,
    }


def _insert_odds_row(conn, meta: Dict[str, bool], payload: Dict[str, Any]) -> None:
    allowed_cols = [
        "quote_id", "match_id", "bookmaker", "market", "selection", "odds_decimal",
        "retrieved_at_utc", "batch_id", "source_id",
        "reliability_score", "ttl_seconds", "cache_hit", "raw_ref",
    ]

    cols = []
    vals = []
    for c in allowed_cols:
        if c in payload:
            if c == "quote_id" and not meta["has_quote_id"]:
                continue
            if c == "batch_id" and not meta["has_batch_id"]:
                continue
            if c == "source_id" and not meta["has_source_id"]:
                continue
            if c == "reliability_score" and not meta["has_reliability_score"]:
                continue
            if c == "ttl_seconds" and not meta["has_ttl_seconds"]:
                continue
            if c == "cache_hit" and not meta["has_cache_hit"]:
                continue
            if c == "raw_ref" and not meta["has_raw_ref"]:
                continue
            cols.append(c)
            vals.append(payload[c])

    if not cols:
        raise RuntimeError("Nessuna colonna valida da inserire in odds_quotes (schema inatteso).")

    placeholders = ", ".join(["?"] * len(cols))
    col_sql = ", ".join(cols)
    conn.execute(
        f"INSERT INTO odds_quotes ({col_sql}) VALUES ({placeholders})",
        tuple(vals),
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--match_id", required=True, help="es: understat:30019")
    p.add_argument("--bookmaker", default="MANUAL", help="es: Bet365")
    p.add_argument("--input", required=True, help="Path JSON con lista quote")
    p.add_argument("--replace", action="store_true", help="Cancella quote esistenti per match+bookmaker prima di inserire")
    p.add_argument("--retrieved_at_utc", default=None, help="ISO timestamp; default now")
    args = p.parse_args()

    match_id = args.match_id
    bookmaker = args.bookmaker
    batch_id = str(uuid4())
    retrieved_at = args.retrieved_at_utc or now_utc_iso()
    source_id = "manual:cli"
    reliability_score = 0.7

    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)

    # supporta: {"odds":[...]} oppure direttamente [...]
    odds_list = data["odds"] if isinstance(data, dict) and "odds" in data else data
    if not isinstance(odds_list, list):
        raise SystemExit("Input JSON non valido: deve essere una lista oppure un dict con chiave 'odds'.")

    with get_conn() as c:
        meta = detect_odds_table_cols(c)
        if args.replace:
            c.execute(
                "DELETE FROM odds_quotes WHERE match_id=? AND bookmaker=?",
                (match_id, bookmaker),
            )

        for o in odds_list:
            market = o["market"]
            selection = o["selection"]
            odds_decimal = float(o["odds_decimal"])
            payload = {
                "quote_id": str(uuid4()),
                "match_id": match_id,
                "batch_id": batch_id,
                "source_id": source_id,
                "reliability_score": reliability_score,
                "bookmaker": bookmaker,
                "market": market,
                "selection": selection,
                "odds_decimal": odds_decimal,
                "retrieved_at_utc": retrieved_at,
            }
            _insert_odds_row(c, meta, payload)
        c.commit()

    print(f"OK: inserted {len(odds_list)} odds | match_id={match_id} bookmaker={bookmaker} batch_id={batch_id} retrieved_at_utc={retrieved_at}")


if __name__ == "__main__":
    main()
