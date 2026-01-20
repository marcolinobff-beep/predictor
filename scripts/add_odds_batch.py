import uuid
from datetime import datetime, timezone
from app.db.sqlite import get_conn

ALLOWED = {
    "1X2": {"HOME","DRAW","AWAY"},
    "OU_2.5": {"OVER","UNDER"},
    "BTTS": {"YES","NO"},
}

def main():
    match_id = input("match_id: ").strip()
    batch_id = str(uuid.uuid4())
    source_id = input("source_id [manual]: ").strip() or "manual"
    reliability = float(input("reliability_score [0.9]: ").strip() or "0.9")
    retrieved_at = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

    print("\nFormato: market selection bookmaker odds_decimal")
    print("Esempio: 1X2 HOME Bet365 2.20")
    print("Mercati ammessi:", ", ".join(ALLOWED.keys()))
    print("Invio vuoto per finire.\n")

    rows = []
    while True:
        line = input("> ").strip()
        if not line:
            break
        parts = line.split()
        if len(parts) != 4:
            print("Formato errato. Usa: market selection bookmaker odds_decimal")
            continue

        market, selection, bookmaker, odds_s = parts
        market = market.upper()
        selection = selection.upper()

        if market not in ALLOWED or selection not in ALLOWED[market]:
            print(f"Market/selection non validi. {market} -> {ALLOWED.get(market)}")
            continue

        odds = float(odds_s)
        if odds <= 1.0:
            print("odds_decimal deve essere > 1.0")
            continue

        rows.append((
            str(uuid.uuid4()),
            match_id,
            batch_id,
            source_id,
            reliability,
            bookmaker,
            market,
            selection,
            odds,
            retrieved_at
        ))

    if not rows:
        print("Nessuna quota inserita.")
        return

    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO odds_quotes
              (quote_id, match_id, batch_id, source_id, reliability_score, bookmaker, market, selection, odds_decimal, retrieved_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows
        )

    print(f"OK: inserted {len(rows)} quotes batch_id={batch_id}")

if __name__ == "__main__":
    main()
