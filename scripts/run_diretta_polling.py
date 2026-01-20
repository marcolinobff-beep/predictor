from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone, date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn
from app.services.lineup_refresh_service import refresh_lineups_for_day


def _parse_date(value: str | None) -> date:
    if not value:
        return datetime.now(timezone.utc).date()
    return date.fromisoformat(value)


def _parse_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _leagues_from_db() -> list[str]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT competition FROM matches ORDER BY competition"
        ).fetchall()
    return [r["competition"] for r in rows if r and r["competition"]]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (UTC). Default: today")
    ap.add_argument("--leagues", default=None, help="Comma separated. Default: all in DB")
    ap.add_argument("--duration-minutes", type=int, default=180)
    ap.add_argument("--interval-seconds", type=int, default=600)
    ap.add_argument("--min-interval-minutes", type=int, default=10)
    args = ap.parse_args()

    day = _parse_date(args.date)
    leagues = _parse_list(args.leagues) or _leagues_from_db()
    if not leagues:
        print("No leagues found in DB.")
        return

    deadline = datetime.now(timezone.utc) + timedelta(minutes=args.duration_minutes)

    while datetime.now(timezone.utc) < deadline:
        for league in leagues:
            notes = refresh_lineups_for_day(
                day_utc=day,
                competition=league,
                min_interval_minutes=args.min_interval_minutes,
                diretta_only=True,
            )
            print(f"Diretta {league}: {'; '.join(notes)}")
        time.sleep(max(10, args.interval_seconds))

    print("OK: diretta polling completed.")


if __name__ == "__main__":
    main()
