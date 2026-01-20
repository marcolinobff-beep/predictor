from app.db.sqlite import get_conn

DDL = """
CREATE TABLE IF NOT EXISTS ingest_runs (
  run_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,              -- "understat"
  league TEXT NOT NULL,                 -- "Serie_A"
  season INTEGER NOT NULL,              -- 2025 (stagione 2025/26)
  started_at_utc TEXT NOT NULL,
  ended_at_utc TEXT,
  status TEXT NOT NULL,                 -- "OK"/"ERROR"
  items_matches INTEGER DEFAULT 0,
  items_teams INTEGER DEFAULT 0,
  items_players INTEGER DEFAULT 0,
  raw_ref TEXT,                         -- path base cache
  error TEXT
);

CREATE TABLE IF NOT EXISTS understat_matches (
  understat_match_id TEXT PRIMARY KEY,
  league TEXT NOT NULL,
  season INTEGER NOT NULL,
  datetime_utc TEXT NOT NULL,
  home_team TEXT NOT NULL,
  away_team TEXT NOT NULL,
  home_goals INTEGER,
  away_goals INTEGER,
  home_xg REAL,
  away_xg REAL,
  raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_us_matches_league_season ON understat_matches(league, season);
CREATE INDEX IF NOT EXISTS idx_us_matches_datetime ON understat_matches(datetime_utc);

CREATE TABLE IF NOT EXISTS understat_teams (
  league TEXT NOT NULL,
  season INTEGER NOT NULL,
  team_id TEXT NOT NULL,
  team_title TEXT NOT NULL,
  raw_json TEXT NOT NULL,
  PRIMARY KEY (league, season, team_id)
);

CREATE TABLE IF NOT EXISTS understat_players (
  league TEXT NOT NULL,
  season INTEGER NOT NULL,
  player_id TEXT NOT NULL,
  player_name TEXT NOT NULL,
  team_title TEXT,
  position TEXT,
  time_minutes INTEGER,
  games INTEGER,
  xg REAL,
  xa REAL,
  shots INTEGER,
  key_passes INTEGER,
  raw_json TEXT NOT NULL,
  PRIMARY KEY (league, season, player_id)
);

-- Mappa tra i match "interni" (matches.match_id) e Understat
CREATE TABLE IF NOT EXISTS match_external_ids (
  match_id TEXT NOT NULL,
  source_id TEXT NOT NULL,              -- "understat"
  external_id TEXT NOT NULL,            -- understat_match_id
  PRIMARY KEY (match_id, source_id)
);
"""

def main():
    with get_conn() as conn:
        conn.executescript(DDL)
    print("OK: understat tables ready")

if __name__ == "__main__":
    main()
