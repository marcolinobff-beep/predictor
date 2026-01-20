from __future__ import annotations

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


def _print_counts(conn, label: str) -> None:
    rows = conn.execute(
        "SELECT competition, COUNT(*) AS cnt FROM matches GROUP BY competition ORDER BY competition"
    ).fetchall()
    print(label)
    for r in rows:
        print(f"  {r['competition']}: {r['cnt']}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-name", default="Serie A", dest="src")
    ap.add_argument("--to-name", default="Serie_A", dest="dst")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with get_conn() as conn:
        _print_counts(conn, "Before")
        if not args.dry_run:
            conn.execute(
                "UPDATE matches SET competition = ? WHERE competition = ?",
                (args.dst, args.src),
            )
            conn.commit()
        _print_counts(conn, "After")


if __name__ == "__main__":
    main()
