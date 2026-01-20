from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone, date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn
from app.services.lineup_refresh_service import refresh_lineups_for_day

ODDS_API_SPORT_KEYS = {
    "Serie_A": "soccer_italy_serie_a",
    "EPL": "soccer_epl",
    "Bundesliga": "soccer_germany_bundesliga",
    "La_Liga": "soccer_spain_la_liga",
    "Ligue_1": "soccer_france_ligue_one",
}


def _parse_date(value: str | None) -> date:
    if not value:
        return datetime.now(timezone.utc).date()
    return date.fromisoformat(value)


def _season_to_code(season: str | None) -> str | None:
    if not season:
        return None
    s = str(season).strip()
    if "/" in s:
        left, right = s.split("/", 1)
        if left.isdigit() and right.isdigit():
            return f"{left[-2:]}{right[-2:]}"
    if len(s) == 4 and s.isdigit():
        return s
    return None


def _season_for_day(day_utc: date, league: str) -> str | None:
    start = datetime(day_utc.year, day_utc.month, day_utc.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    start_iso = start.isoformat().replace("+00:00", "Z")
    end_iso = end.isoformat().replace("+00:00", "Z")
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT season, COUNT(*) AS cnt
            FROM matches
            WHERE competition = ?
              AND kickoff_utc >= ? AND kickoff_utc < ?
            GROUP BY season
            ORDER BY cnt DESC
            """,
            (league, start_iso, end_iso),
        ).fetchall()
    if not rows:
        return None
    return rows[0]["season"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", default="Serie_A")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--division", default="I1")
    ap.add_argument("--season", default=None, help="es: 2425 (override auto)")
    ap.add_argument("--skip-odds", action="store_true")
    ap.add_argument("--closing", action="store_true")
    ap.add_argument("--oddsapi", action="store_true", help="Ingest multi-bookmaker odds via The Odds API")
    ap.add_argument("--oddsapi-markets", default="h2h,totals")
    ap.add_argument("--oddsapi-regions", default="eu")
    ap.add_argument("--oddsapi-bookmaker", default=None)
    ap.add_argument("--oddsapi-sport-key", default=None)
    ap.add_argument("--run-kpi", action="store_true")
    ap.add_argument("--max-seasons", type=int, default=5)
    ap.add_argument("--tactical-csv", default=None, help="CSV con tactical stats (possession/ppda)")
    ap.add_argument("--tactical-source", default=None, help="Override source label for tactical CSV")
    args = ap.parse_args()

    day = _parse_date(args.date)
    notes = refresh_lineups_for_day(day, args.league)
    print(f"Lineups refresh: {'; '.join(notes)}")

    if args.tactical_csv:
        cmd = [
            sys.executable,
            "scripts/ingest_tactical_stats_csv.py",
            "--csv",
            args.tactical_csv,
        ]
        if args.tactical_source:
            cmd.extend(["--source", args.tactical_source])
        subprocess.run(cmd, check=True)

    if not args.skip_odds:
        season_label = args.season or _season_for_day(day, args.league)
        season_code = _season_to_code(season_label) if season_label else None
        if not season_code:
            print("WARN: season code not resolved, skipping football-data odds ingest.")
        else:
            cmd = [
                sys.executable,
                "scripts/ingest_odds_football_data_for_day.py",
                "--date",
                day.isoformat(),
                "--division",
                args.division,
                "--season",
                season_code,
                "--competition",
                args.league,
            ]
            if args.closing:
                cmd.append("--closing")
            subprocess.run(cmd, check=True)

    if args.oddsapi:
        sport_key = args.oddsapi_sport_key or ODDS_API_SPORT_KEYS.get(args.league)
        if not sport_key:
            print(f"WARN: no Odds API sport key mapping for {args.league}, skipping oddsapi ingest.")
        else:
            cmd = [
                sys.executable,
                "scripts/ingest_odds_oddsapi_day.py",
                "--date",
                day.isoformat(),
                "--competition",
                args.league,
                "--sport-key",
                sport_key,
                "--regions",
                args.oddsapi_regions,
                "--markets",
                args.oddsapi_markets,
            ]
            if args.oddsapi_bookmaker:
                cmd.extend(["--bookmaker", args.oddsapi_bookmaker])
            subprocess.run(cmd, check=True)

    if args.run_kpi:
        cmd = [
            sys.executable,
            "scripts/run_kpi_auto.py",
            "--league",
            args.league,
            "--max-seasons",
            str(args.max_seasons),
        ]
        subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
