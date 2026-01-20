from app.db.sqlite import get_conn

def main():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT match_id, kickoff_utc, home, away, season, competition
            FROM matches
            WHERE match_id LIKE 'understat:%'
            ORDER BY kickoff_utc DESC
            LIMIT 30
            """
        ).fetchall()

    for r in rows:
        print(f"{r['match_id']} | {r['competition']} {r['season']} | {r['kickoff_utc']} | {r['home']} vs {r['away']}")

if __name__ == "__main__":
    main()
