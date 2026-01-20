from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from app.db.sqlite import get_conn


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def to_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def to_int(x):
    try:
        return int(float(x)) if x is not None else None
    except Exception:
        return None


def season_label(season_start: int) -> str:
    return f"{season_start}/{str(season_start + 1)[-2:]}"


def to_kickoff_iso_z(dt_str: str) -> Optional[str]:
    if not dt_str:
        return None
    s = str(dt_str).strip()
    if not s:
        return None
    if "T" in s and s.endswith("Z"):
        return s
    try:
        d = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return d.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        if " " in s and "T" not in s:
            s2 = s.replace(" ", "T")
            if not s2.endswith("Z"):
                s2 += "Z"
            return s2
        return None


def _pick_latest_cache(base_dir: str) -> Optional[str]:
    if not os.path.isdir(base_dir):
        return None
    candidates = []
    for name in os.listdir(base_dir):
        p = os.path.join(base_dir, name)
        if not os.path.isdir(p):
            continue
        if os.path.isfile(os.path.join(p, "league_results.json")):
            candidates.append(p)
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


def upsert_match_row(conn, league: str, season_start: int, m: dict) -> None:
    understat_match_id = str(m.get("id"))
    if not understat_match_id:
        return

    match_id = f"understat:{understat_match_id}"
    dt_utc = to_kickoff_iso_z(m.get("datetime"))
    if not dt_utc:
        return

    h = m.get("h") or {}
    a = m.get("a") or {}

    home_team = h.get("title") or h.get("short_title") or "UNKNOWN_HOME"
    away_team = a.get("title") or a.get("short_title") or "UNKNOWN_AWAY"

    comp = league
    season_str = season_label(season_start)

    r = conn.execute("SELECT 1 FROM matches WHERE match_id=?", (match_id,)).fetchone()
    if r:
        return

    r2 = conn.execute(
        "SELECT match_id FROM matches WHERE kickoff_utc=? AND home=? AND away=?",
        (dt_utc, home_team, away_team),
    ).fetchone()

    if r2:
        old_id = r2[0]
        if isinstance(old_id, str) and old_id.startswith("understat:"):
            return
        conn.execute(
            "UPDATE matches SET match_id=?, competition=?, season=? WHERE match_id=?",
            (match_id, comp, season_str, old_id),
        )
        return

    conn.execute(
        """
        INSERT INTO matches (match_id, competition, season, kickoff_utc, home, away, venue)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (match_id, comp, season_str, dt_utc, home_team, away_team, None),
    )


def upsert_understat_data(run_id: str, league: str, season: int, results, teams, players, cache_base: str):
    with get_conn() as conn:
        for m in results:
            upsert_match_row(conn, league=league, season_start=season, m=m)

        for m in results:
            dt_utc = to_kickoff_iso_z(m.get("datetime"))
            understat_match_id = str(m.get("id"))
            h = m.get("h") or {}
            a = m.get("a") or {}
            goals = m.get("goals") or {}
            xg = m.get("xG") or {}

            home_team = h.get("title") or h.get("short_title") or "UNKNOWN_HOME"
            away_team = a.get("title") or a.get("short_title") or "UNKNOWN_AWAY"

            raw = json.dumps(m, ensure_ascii=False)

            conn.execute(
                """
                INSERT OR REPLACE INTO understat_matches
                (understat_match_id, league, season, datetime_utc, home_team, away_team,
                 home_goals, away_goals, home_xg, away_xg, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    understat_match_id, league, season, dt_utc, home_team, away_team,
                    to_int(goals.get("h")), to_int(goals.get("a")),
                    to_float(xg.get("h")), to_float(xg.get("a")),
                    raw
                )
            )

        for t in teams:
            team_id = str(t.get("id"))
            title = t.get("title") or t.get("name") or "UNKNOWN_TEAM"
            conn.execute(
                """
                INSERT OR REPLACE INTO understat_teams
                (league, season, team_id, team_title, raw_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (league, season, team_id, title, json.dumps(t, ensure_ascii=False))
            )

        for p in players:
            pid = str(p.get("id"))
            conn.execute(
                """
                INSERT OR REPLACE INTO understat_players
                (league, season, player_id, player_name, team_title, position,
                 time_minutes, games, xg, xa, shots, key_passes, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    league, season, pid,
                    p.get("player_name") or "UNKNOWN_PLAYER",
                    p.get("team_title"),
                    p.get("position"),
                    to_int(p.get("time")),
                    to_int(p.get("games")),
                    to_float(p.get("xG")),
                    to_float(p.get("xA")),
                    to_int(p.get("shots")),
                    to_int(p.get("key_passes")),
                    json.dumps(p, ensure_ascii=False)
                )
            )

        conn.execute(
            """
            UPDATE ingest_runs
            SET ended_at_utc = ?, status = 'OK',
                items_matches = ?, items_teams = ?, items_players = ?,
                raw_ref = ?
            WHERE run_id = ?
            """,
            (utc_now_iso(), len(results), len(teams), len(players), cache_base, run_id)
        )

        conn.commit()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--season", required=True, type=int)
    ap.add_argument("--cache-dir", default=None, help="Override cache dir containing league_results.json")
    args = ap.parse_args()

    cache_base = args.cache_dir
    if not cache_base:
        base_dir = os.path.join("data", "cache", "understat", args.league, str(args.season))
        cache_base = _pick_latest_cache(base_dir)

    if not cache_base:
        raise SystemExit("No cache folder found. Run understat ingest or provide --cache-dir.")

    with open(os.path.join(cache_base, "league_results.json"), "r", encoding="utf-8") as f:
        results = json.load(f)
    with open(os.path.join(cache_base, "teams.json"), "r", encoding="utf-8") as f:
        teams = json.load(f)
    with open(os.path.join(cache_base, "players.json"), "r", encoding="utf-8") as f:
        players = json.load(f)

    run_id = str(uuid4())
    started = utc_now_iso()

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO ingest_runs(run_id, source_id, league, season, started_at_utc, status)
            VALUES (?, 'understat_cache', ?, ?, ?, 'RUNNING')
            """,
            (run_id, args.league, args.season, started)
        )
        conn.commit()

    upsert_understat_data(run_id, args.league, args.season, results, teams, players, cache_base)
    print(f"OK: ingest understat cache done run_id={run_id} matches={len(results)} teams={len(teams)} players={len(players)}")


if __name__ == "__main__":
    main()
