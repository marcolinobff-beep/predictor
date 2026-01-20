from __future__ import annotations

import argparse
import os
import subprocess
import sys
from typing import List

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


def _season_start(season_label: str) -> int | None:
    if not season_label:
        return None
    s = str(season_label).strip()
    if "/" in s:
        left = s.split("/", 1)[0]
        if left.isdigit():
            return int(left)
    if len(s) >= 4 and s[:4].isdigit():
        return int(s[:4])
    return None


def _list_seasons(league: str) -> List[int]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT season
            FROM matches
            WHERE competition = ?
            ORDER BY season DESC
            """,
            (league,),
        ).fetchall()
    seasons = []
    for r in rows:
        year = _season_start(r["season"])
        if year and year not in seasons:
            seasons.append(year)
    return seasons


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", default="Serie_A")
    ap.add_argument("--max-seasons", type=int, default=5)
    ap.add_argument("--out", default="data/reports/kpi_report.json")
    args = ap.parse_args()

    seasons = _list_seasons(args.league)[: args.max_seasons]
    if not seasons:
        raise SystemExit("No seasons found for league in matches table.")

    seasons_str = ",".join(str(s) for s in seasons)
    cmd = [
        sys.executable,
        "scripts/kpi_report.py",
        "--league",
        args.league,
        "--seasons",
        seasons_str,
        "--out",
        args.out,
    ]
    subprocess.run(cmd, check=True)
    print(f"OK: KPI report updated for {args.league} seasons={seasons_str}")


if __name__ == "__main__":
    main()
