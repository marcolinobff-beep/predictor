from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


def main() -> None:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tactical_stats (
                match_id TEXT PRIMARY KEY,
                source TEXT,
                possession_home REAL,
                possession_away REAL,
                ppda_home REAL,
                ppda_away REAL
            )
            """
        )
        conn.commit()
    print("OK: tactical_stats table ready.")


if __name__ == "__main__":
    main()
