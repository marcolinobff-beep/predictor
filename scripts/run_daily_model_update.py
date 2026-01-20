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


def _latest_seasons(league: str, max_seasons: int) -> List[int]:
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
        if len(seasons) >= max_seasons:
            break
    return seasons


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", default="Serie_A")
    ap.add_argument("--max-seasons", type=int, default=2)
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--skip-gbm", action="store_true")
    ap.add_argument("--skip-calibration", action="store_true")
    ap.add_argument("--skip-rho", action="store_true")
    args = ap.parse_args()

    seasons = _latest_seasons(args.league, args.max_seasons)
    if not seasons:
        raise SystemExit("No seasons found for league in matches table.")

    for season in seasons:
        cmd = [
            sys.executable,
            "scripts/build_features_understat_v5.py",
            "--league",
            args.league,
            "--season",
            str(season),
            "--features_version",
            args.features_version,
        ]
        subprocess.run(cmd, check=True)

    seasons_str = ",".join(str(s) for s in seasons)

    if not args.skip_rho:
        subprocess.run([
            sys.executable,
            "scripts/fit_dc_rho.py",
            "--league",
            args.league,
            "--seasons",
            seasons_str,
            "--features-version",
            args.features_version,
        ], check=True)

    if not args.skip_calibration:
        subprocess.run([
            sys.executable,
            "scripts/calibrate_model_by_season.py",
            "--league",
            args.league,
            "--seasons",
            seasons_str,
            "--features-version",
            args.features_version,
        ], check=True)

    if not args.skip_gbm:
        subprocess.run([
            sys.executable,
            "scripts/train_gbm_light.py",
            "--league",
            args.league,
            "--seasons",
            seasons_str,
            "--features-version",
            args.features_version,
        ], check=True)

    print(f"OK: model update completed for seasons {seasons_str}")


if __name__ == "__main__":
    main()
