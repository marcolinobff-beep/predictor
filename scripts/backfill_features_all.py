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


DEFAULT_LEAGUES = ["Serie_A", "EPL", "Bundesliga", "La_Liga", "Ligue_1"]


def _parse_list(value: str | None) -> List[str]:
    if not value:
        return DEFAULT_LEAGUES
    return [v.strip() for v in value.split(",") if v.strip()]


def _season_start(label: str | None) -> int | None:
    if not label:
        return None
    s = str(label).strip()
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
            ORDER BY season
            """,
            (league,),
        ).fetchall()
    seasons: List[int] = []
    for r in rows:
        year = _season_start(r["season"])
        if year and year not in seasons:
            seasons.append(year)
    return seasons


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--leagues", default=None, help="Comma separated (default top 5)")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--sleep", type=float, default=0.0)
    args = ap.parse_args()

    for league in _parse_list(args.leagues):
        seasons = _list_seasons(league)
        if not seasons:
            print(f"WARN: no seasons found for {league}")
            continue
        for season in seasons:
            cmd = [
                sys.executable,
                "scripts/build_features_understat_v5.py",
                "--league",
                league,
                "--season",
                str(season),
                "--features_version",
                args.features_version,
            ]
            subprocess.run(cmd, check=True)
            if args.sleep and args.sleep > 0:
                import time
                time.sleep(args.sleep)

    print("OK: backfill features completed.")


if __name__ == "__main__":
    main()
