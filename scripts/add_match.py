import os
import sys
import uuid
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn

def main():
    competition = input("competition (es. Serie_A): ").strip()
    season = input("season (es. 2025/26) [opzionale]: ").strip() or None
    kickoff_utc = input("kickoff_utc ISO (es. 2026-01-10T19:30:00Z): ").strip()
    home = input("home team: ").strip()
    away = input("away team: ").strip()
    venue = input("venue [opzionale]: ").strip() or None

    match_id = str(uuid.uuid4())

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO matches (match_id, competition, season, kickoff_utc, home, away, venue)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (match_id, competition, season, kickoff_utc, home, away, venue)
        )

    print(f"OK: inserted match_id={match_id}")

if __name__ == "__main__":
    main()
