from app.db.sqlite import get_conn


DDL = """
CREATE TABLE IF NOT EXISTS player_projections (
  league TEXT NOT NULL,
  season INTEGER NOT NULL,
  player_id TEXT NOT NULL,
  player_name TEXT NOT NULL,
  team_title TEXT,
  position TEXT,
  games INTEGER,
  time_minutes INTEGER,
  xg REAL,
  xa REAL,
  shots INTEGER,
  key_passes INTEGER,
  xg_per90 REAL,
  xa_per90 REAL,
  shots_per90 REAL,
  key_passes_per90 REAL,
  gi_per90 REAL,
  xg_share REAL,
  xa_share REAL,
  gi_share REAL,
  created_at_utc TEXT NOT NULL,
  PRIMARY KEY (league, season, player_id)
);

CREATE INDEX IF NOT EXISTS idx_player_proj_league_season ON player_projections(league, season);
CREATE INDEX IF NOT EXISTS idx_player_proj_team ON player_projections(team_title);
"""


def main() -> None:
    with get_conn() as conn:
        conn.executescript(DDL)
    print("OK: player_projections table ready")


if __name__ == "__main__":
    main()
