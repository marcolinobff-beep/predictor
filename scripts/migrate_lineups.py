import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


DDL = """
CREATE TABLE IF NOT EXISTS probable_lineups (
  lineup_id TEXT PRIMARY KEY,
  match_id TEXT NOT NULL,
  source TEXT NOT NULL,
  fetched_at_utc TEXT NOT NULL,
  confidence REAL DEFAULT 0.6,
  home_players_json TEXT,
  away_players_json TEXT,
  home_absences_json TEXT,
  away_absences_json TEXT,
  notes TEXT,
  raw_ref TEXT
);

CREATE INDEX IF NOT EXISTS idx_prob_lineups_match ON probable_lineups(match_id);
CREATE INDEX IF NOT EXISTS idx_prob_lineups_source ON probable_lineups(source);
"""


def main() -> None:
    with get_conn() as conn:
        conn.executescript(DDL)
        cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(probable_lineups)").fetchall()
        }
        if "home_absences_json" not in cols:
            conn.execute("ALTER TABLE probable_lineups ADD COLUMN home_absences_json TEXT")
        if "away_absences_json" not in cols:
            conn.execute("ALTER TABLE probable_lineups ADD COLUMN away_absences_json TEXT")
    print("OK: probable_lineups table ready")


if __name__ == "__main__":
    main()
