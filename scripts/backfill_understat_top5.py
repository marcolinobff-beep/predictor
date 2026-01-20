from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Iterable, List
from uuid import uuid4

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn
from scripts.fetch_understat_league_http import fetch_league_data
from scripts.ingest_understat_from_cache import upsert_understat_data


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _season_start(label: str) -> int | None:
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


def _list_seasons_from_competition(comp: str) -> List[int]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT season
            FROM matches
            WHERE competition = ?
            ORDER BY season
            """,
            (comp,),
        ).fetchall()
    out: List[int] = []
    for r in rows:
        year = _season_start(r["season"])
        if year and year not in out:
            out.append(year)
    return out


def _has_understat_data(league: str, season: int) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT 1 FROM understat_matches
            WHERE league = ? AND season = ?
            LIMIT 1
            """,
            (league, season),
        ).fetchone()
    return bool(row)


def _write_cache(cache_base: str, results, teams, players) -> None:
    os.makedirs(cache_base, exist_ok=True)
    with open(os.path.join(cache_base, "league_results.json"), "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False)
    with open(os.path.join(cache_base, "teams.json"), "w", encoding="utf-8") as f:
        json.dump(teams, f, ensure_ascii=False)
    with open(os.path.join(cache_base, "players.json"), "w", encoding="utf-8") as f:
        json.dump(players, f, ensure_ascii=False)


def _parse_leagues(value: str | None) -> List[str]:
    if not value:
        return ["EPL", "Bundesliga", "La_Liga", "Ligue_1"]
    return [v.strip() for v in value.split(",") if v.strip()]


def _parse_seasons(values: str | None) -> List[int]:
    if not values:
        return []
    out: List[int] = []
    for part in values.split(","):
        part = part.strip()
        if not part:
            continue
        if part.isdigit():
            out.append(int(part))
    return out


def _ensure_ingest_run(run_id: str, league: str, season: int) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO ingest_runs(run_id, source_id, league, season, started_at_utc, status)
            VALUES (?, 'understat_http', ?, ?, ?, 'RUNNING')
            """,
            (run_id, league, season, _utc_now_iso()),
        )
        conn.commit()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-competition", default="Serie_A")
    ap.add_argument("--leagues", default=None)
    ap.add_argument("--seasons", default=None, help="Comma separated season start years (es: 2021,2022)")
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument("--timeout", type=int, default=25)
    ap.add_argument("--skip-existing", action="store_true")
    args = ap.parse_args()

    leagues = _parse_leagues(args.leagues)
    seasons = _parse_seasons(args.seasons)
    if not seasons:
        seasons = _list_seasons_from_competition(args.from_competition)
    if not seasons:
        raise SystemExit("No seasons found. Provide --seasons or ensure matches in DB.")

    total = 0
    for league in leagues:
        for season in seasons:
            if args.skip_existing and _has_understat_data(league, season):
                print(f"Skip {league} {season}: data already present.")
                continue

            run_id = str(uuid4())
            cache_base = os.path.join(
                "data", "cache", "understat", league, str(season), run_id
            )

            print(f"Fetch understat league={league} season={season}")
            data = fetch_league_data(league, season, timeout=args.timeout)
            results = data.get("dates") or []
            teams_raw = data.get("teams") or {}
            teams = list(teams_raw.values()) if isinstance(teams_raw, dict) else teams_raw
            players = data.get("players") or []

            _write_cache(cache_base, results, teams, players)
            _ensure_ingest_run(run_id, league, season)
            upsert_understat_data(run_id, league, season, results, teams, players, cache_base)
            print(
                f"OK: ingested {league} {season} matches={len(results)} teams={len(teams)} players={len(players)}"
            )
            total += 1
            if args.sleep > 0:
                time.sleep(args.sleep)

    print(f"OK: completed understat backfill jobs={total}")


if __name__ == "__main__":
    main()
