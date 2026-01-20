from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from app.db.sqlite import get_conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _per90(value: float, minutes: int) -> float:
    if minutes <= 0:
        return 0.0
    return value * 90.0 / minutes


def _collect_team_totals(rows, min_minutes: int, min_games: int) -> Tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]:
    totals_all: Dict[str, Dict[str, float]] = {}
    totals_filtered: Dict[str, Dict[str, float]] = {}

    def _add(target: Dict[str, Dict[str, float]], team: str, xg: float, xa: float):
        if team not in target:
            target[team] = {"xg": 0.0, "xa": 0.0}
        target[team]["xg"] += xg
        target[team]["xa"] += xa

    for r in rows:
        team = r["team_title"] or "UNKNOWN"
        xg = float(r["xg"] or 0.0)
        xa = float(r["xa"] or 0.0)
        _add(totals_all, team, xg, xa)

        minutes = int(r["time_minutes"] or 0)
        games = int(r["games"] or 0)
        if minutes >= min_minutes and games >= min_games:
            _add(totals_filtered, team, xg, xa)

    return totals_all, totals_filtered


def _team_totals_for(team: str, totals_all, totals_filtered) -> Dict[str, float]:
    data = totals_filtered.get(team)
    if data and (data.get("xg", 0.0) > 0 or data.get("xa", 0.0) > 0):
        return data
    return totals_all.get(team, {"xg": 0.0, "xa": 0.0})


def build_for_season(league: str, season: int, min_minutes: int, min_games: int) -> int:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT league, season, player_id, player_name, team_title, position,
                   time_minutes, games, xg, xa, shots, key_passes
            FROM understat_players
            WHERE league = ? AND season = ?
            """,
            (league, season),
        ).fetchall()

        if not rows:
            print(f"No players found for {league} {season}")
            return 0

        totals_all, totals_filtered = _collect_team_totals(rows, min_minutes, min_games)
        created_at = _now_iso()

        wrote = 0
        for r in rows:
            team = r["team_title"] or "UNKNOWN"
            totals = _team_totals_for(team, totals_all, totals_filtered)
            team_xg = float(totals.get("xg") or 0.0)
            team_xa = float(totals.get("xa") or 0.0)

            minutes = int(r["time_minutes"] or 0)
            xg = float(r["xg"] or 0.0)
            xa = float(r["xa"] or 0.0)
            shots = int(r["shots"] or 0)
            key_passes = int(r["key_passes"] or 0)

            xg_per90 = _per90(xg, minutes) if minutes > 0 else None
            xa_per90 = _per90(xa, minutes) if minutes > 0 else None
            shots_per90 = _per90(shots, minutes) if minutes > 0 else None
            key_passes_per90 = _per90(key_passes, minutes) if minutes > 0 else None
            gi_per90 = None
            if xg_per90 is not None or xa_per90 is not None:
                gi_per90 = (xg_per90 or 0.0) + (xa_per90 or 0.0)

            xg_share = (xg / team_xg) if team_xg > 0 else None
            xa_share = (xa / team_xa) if team_xa > 0 else None
            gi_share = None
            if xg_share is not None or xa_share is not None:
                gi_share = (xg_share or 0.0) + (xa_share or 0.0)

            conn.execute(
                """
                INSERT OR REPLACE INTO player_projections (
                    league, season, player_id, player_name, team_title, position,
                    games, time_minutes, xg, xa, shots, key_passes,
                    xg_per90, xa_per90, shots_per90, key_passes_per90, gi_per90,
                    xg_share, xa_share, gi_share, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    league,
                    season,
                    r["player_id"],
                    r["player_name"],
                    team,
                    r["position"],
                    r["games"],
                    minutes,
                    xg,
                    xa,
                    shots,
                    key_passes,
                    xg_per90,
                    xa_per90,
                    shots_per90,
                    key_passes_per90,
                    gi_per90,
                    xg_share,
                    xa_share,
                    gi_share,
                    created_at,
                ),
            )
            wrote += 1

        conn.commit()

    print(f"OK: wrote player projections for {league} {season} -> {wrote}")
    return wrote


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", required=True, help="Comma separated season start years, es: 2021,2022,2023")
    ap.add_argument("--min-minutes", type=int, default=450)
    ap.add_argument("--min-games", type=int, default=5)
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    for season in seasons:
        build_for_season(args.league, season, args.min_minutes, args.min_games)


if __name__ == "__main__":
    main()
