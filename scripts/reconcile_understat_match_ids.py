from __future__ import annotations

import glob
import json
import os
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


def pick_latest_league_results() -> str:
    paths = glob.glob("data/cache/**/league_results.json", recursive=True)
    if not paths:
        raise SystemExit("Nessun league_results.json trovato sotto data/cache/**")
    paths = sorted(paths, key=os.path.getmtime)
    return paths[-1]


def iso_z_from_understat_datetime(dt_str: str) -> str:
    # Understat cache: "YYYY-MM-DD HH:MM:SS"
    d = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return d.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main():
    p = pick_latest_league_results()
    print("using:", p)

    data = json.load(open(p, "r", encoding="utf-8"))

    updated = 0
    inserted = 0
    skipped = 0

    with get_conn() as c:
        for m in data:
            under_id = "understat:" + str(m["id"])
            ko = iso_z_from_understat_datetime(m["datetime"])
            home = (m.get("h") or {}).get("title")
            away = (m.get("a") or {}).get("title")
            if not home or not away:
                skipped += 1
                continue

            # 1) già presente come understat:* ?
            r = c.execute("SELECT 1 FROM matches WHERE match_id=?", (under_id,)).fetchone()
            if r:
                continue

            # 2) esiste lo stesso match (kickoff+home+away) con un altro match_id?
            r2 = c.execute(
                "SELECT match_id FROM matches WHERE kickoff_utc=? AND home=? AND away=?",
                (ko, home, away),
            ).fetchone()

            if r2:
                old_id = r2[0]
                # se per qualche motivo old_id già understat:* diverso, non tocchiamo
                if isinstance(old_id, str) and old_id.startswith("understat:"):
                    skipped += 1
                    continue

                # aggiorna match_id al valore understat:* (fix “definitivo”)
                c.execute(
                    "UPDATE matches SET match_id=? WHERE match_id=?",
                    (under_id, old_id),
                )
                updated += 1
            else:
                # 3) se non esiste proprio, inserisci (minimo indispensabile)
                # NOTA: 'competition' e 'season' nel tuo schema esistono: qui mettiamo placeholder
                # Se vuoi, possiamo dedurli dal path (Serie_A/2025) ma per ora basta.
                c.execute(
                    """
                    INSERT INTO matches (match_id, competition, season, kickoff_utc, home, away, venue)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (under_id, "Serie_A", "2025/26", ko, home, away, None),
                )
                inserted += 1

        c.commit()

    print(f"OK reconcile: updated={updated} inserted={inserted} skipped={skipped}")


if __name__ == "__main__":
    main()
