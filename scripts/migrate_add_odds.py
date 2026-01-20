from app.db.sqlite import get_conn

DDL = """
CREATE TABLE IF NOT EXISTS odds_quotes (
  quote_id TEXT PRIMARY KEY,
  match_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  source_id TEXT NOT NULL,            -- es. "manual" o "web:provider"
  reliability_score REAL NOT NULL,    -- 0..1
  bookmaker TEXT NOT NULL,
  market TEXT NOT NULL,               -- "1X2", "OU_2.5", "BTTS"
  selection TEXT NOT NULL,            -- HOME/DRAW/AWAY | OVER/UNDER | YES/NO
  odds_decimal REAL NOT NULL,
  retrieved_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_odds_match_batch ON odds_quotes(match_id, batch_id);
CREATE INDEX IF NOT EXISTS idx_odds_match_time ON odds_quotes(match_id, retrieved_at_utc);
"""

def main():
    with get_conn() as conn:
        conn.executescript(DDL)
    print("OK: odds_quotes ready")

if __name__ == "__main__":
    main()
