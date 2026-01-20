from __future__ import annotations
import argparse
from datetime import datetime, timezone
from app.db.sqlite import get_conn

def parse_utc(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20)
    args = ap.parse_args()

    now = datetime.now(timezone.utc)

    with get_conn() as c:
        rows = c.execute(
            """
            SELECT match_id, bookmaker, market, selection, odds_decimal, retrieved_at_utc
            FROM odds_quotes
            ORDER BY retrieved_at_utc DESC
            LIMIT ?
            """,
            (args.n,),
        ).fetchall()

    if not rows:
        print("No odds_quotes rows found.")
        return

    for r in rows:
        match_id, book, market, sel, odds, ts = tuple(r)
        dt = parse_utc(ts) if ts else None
        age_h = (now - dt).total_seconds()/3600.0 if dt else None
        age_s = f"{age_h:.1f}h" if age_h is not None else "NA"
        print(f"{ts} | age={age_s:>8} | {match_id} | {book} | {market} {sel} | {odds}")

if __name__ == "__main__":
    main()
