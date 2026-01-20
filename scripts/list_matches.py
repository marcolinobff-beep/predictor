from app.db.sqlite import get_conn

def main():
    with get_conn() as conn:
        rows = conn.execute("SELECT match_id, competition, season, kickoff_utc, home, away FROM matches ORDER BY kickoff_utc DESC").fetchall()
    for r in rows:
        print(f"{r['match_id']} | {r['competition']} {r['season']} | {r['kickoff_utc']} | {r['home']} vs {r['away']}")

if __name__ == "__main__":
    main()
