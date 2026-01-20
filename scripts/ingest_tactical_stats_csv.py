from __future__ import annotations

import argparse
import csv
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


def _to_float(value: object | None) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("%"):
        s = s[:-1].strip()
    try:
        return float(s)
    except ValueError:
        return None


def _get_any(row: dict, keys: list[str]) -> str | None:
    for k in keys:
        if k in row and row[k] is not None:
            val = str(row[k]).strip()
            if val:
                return val
    return None


def _ensure_table(conn) -> None:
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to CSV with tactical stats")
    ap.add_argument("--source", default=None, help="Default source label (override if CSV has source)")
    args = ap.parse_args()

    path = os.path.abspath(args.csv)
    if not os.path.exists(path):
        raise SystemExit(f"CSV not found: {path}")

    inserted = 0
    updated = 0
    skipped = 0

    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit("CSV has no header.")

        with get_conn() as conn:
            _ensure_table(conn)
            for row in reader:
                match_id = _get_any(row, ["match_id", "matchId"])
                if not match_id:
                    understat_id = _get_any(row, ["understat_id"])
                    if understat_id:
                        if understat_id.isdigit() and not understat_id.startswith("understat:"):
                            match_id = f"understat:{understat_id}"
                        else:
                            match_id = understat_id
                if not match_id:
                    skipped += 1
                    continue

                source = _get_any(row, ["source"]) or args.source or "csv"
                pos_home = _to_float(_get_any(row, ["possession_home", "home_possession"]))
                pos_away = _to_float(_get_any(row, ["possession_away", "away_possession"]))
                ppda_home = _to_float(_get_any(row, ["ppda_home", "home_ppda"]))
                ppda_away = _to_float(_get_any(row, ["ppda_away", "away_ppda"]))

                exists = conn.execute(
                    "SELECT 1 FROM tactical_stats WHERE match_id = ?",
                    (match_id,),
                ).fetchone()

                conn.execute(
                    """
                    INSERT INTO tactical_stats (
                        match_id, source, possession_home, possession_away, ppda_home, ppda_away
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(match_id) DO UPDATE SET
                        source = excluded.source,
                        possession_home = excluded.possession_home,
                        possession_away = excluded.possession_away,
                        ppda_home = excluded.ppda_home,
                        ppda_away = excluded.ppda_away
                    """,
                    (match_id, source, pos_home, pos_away, ppda_home, ppda_away),
                )

                if exists:
                    updated += 1
                else:
                    inserted += 1

            conn.commit()

    print(f"OK: tactical_stats upserted inserted={inserted} updated={updated} skipped={skipped}")


if __name__ == "__main__":
    main()
