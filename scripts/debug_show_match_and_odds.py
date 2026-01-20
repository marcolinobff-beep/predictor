from app.db.sqlite import get_conn

MATCH_ID = "understat:30019"

def main():
    with get_conn() as conn:
        m = conn.execute("SELECT * FROM matches WHERE match_id=?", (MATCH_ID,)).fetchone()
        print("MATCH:", dict(m) if m else None)

        rows = conn.execute(
            """
            SELECT bookmaker, market, selection, odds_decimal, retrieved_at_utc, batch_id
            FROM odds_quotes
            WHERE match_id=?
            ORDER BY market, selection
            """,
            (MATCH_ID,)
        ).fetchall()

        print("\nODDS:")
        for r in rows:
            print(dict(r))

if __name__ == "__main__":
    main()
