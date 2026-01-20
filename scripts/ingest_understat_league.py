import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import aiohttp
from understat import Understat

from app.db.sqlite import get_conn


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


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
    # 2025 -> "2025/26"
    return f"{season_start}/{str(season_start + 1)[-2:]}"


def to_kickoff_iso_z(dt_str: str) -> str | None:
    """
    Understat results: spesso 'YYYY-MM-DD HH:MM:SS' (UTC).
    Normalizziamo a 'YYYY-MM-DDTHH:MM:SSZ'
    """
    if not dt_str:
        return None
    s = str(dt_str).strip()
    if not s:
        return None

    # già ISO con T?
    if "T" in s and s.endswith("Z"):
        return s

    # classico understat: "YYYY-MM-DD HH:MM:SS"
    try:
        d = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return d.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        # fallback: prova a sostituire spazio con T e aggiungere Z
        if " " in s and "T" not in s:
            s2 = s.replace(" ", "T")
            if not s2.endswith("Z"):
                s2 += "Z"
            return s2
        return None


async def fetch_all(league: str, season: int):
    async with aiohttp.ClientSession() as session:
        us = Understat(session)
        results = await us.get_league_results(league, season)
        teams = await us.get_teams(league, season)
        players = await us.get_league_players(league, season)
        return results, teams, players


def upsert_match_row(conn, league: str, season_start: int, m: dict) -> None:
    """
    Fix definitivo:
    - match_id canonico = understat:<id>
    - se esiste già lo stesso match (kickoff+home+away) con altro match_id, aggiorna match_id.
    - altrimenti inserisci.
    """
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

    comp = league  # nel tuo DB usi "Serie_A"
    season_str = season_label(season_start)

    # 1) Se è già presente come understat:<id>, fine.
    r = conn.execute("SELECT 1 FROM matches WHERE match_id=?", (match_id,)).fetchone()
    if r:
        return

    # 2) Se esiste lo stesso match con altro id (tipicamente UUID), convertilo
    r2 = conn.execute(
        "SELECT match_id FROM matches WHERE kickoff_utc=? AND home=? AND away=?",
        (dt_utc, home_team, away_team),
    ).fetchone()

    if r2:
        old_id = r2[0]
        # se per caso è già understat:qualcosa (diverso), non tocchiamo per evitare collisioni strane
        if isinstance(old_id, str) and old_id.startswith("understat:"):
            return

        conn.execute(
            "UPDATE matches SET match_id=?, competition=?, season=? WHERE match_id=?",
            (match_id, comp, season_str, old_id),
        )
        return

    # 3) Non esiste proprio: inserisci
    conn.execute(
        """
        INSERT INTO matches (match_id, competition, season, kickoff_utc, home, away, venue)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (match_id, comp, season_str, dt_utc, home_team, away_team, None),
    )


def upsert_understat_data(run_id: str, league: str, season: int, results, teams, players, cache_base: str):
    with get_conn() as conn:
        # --- FIX DEFINITIVO: aggiorna/crea match in tabella matches usando understat ids ---
        for m in results:
            upsert_match_row(conn, league=league, season_start=season, m=m)

        # --- understat_matches (come avevi) ---
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

        # teams
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

        # players
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

        # ingest run row
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True, help='Understat league name, es: "Serie_A" oppure "EPL"')
    ap.add_argument("--season", required=True, type=int, help="Season start year, es 2025 per 2025/26")
    args = ap.parse_args()

    run_id = str(uuid4())
    started = utc_now_iso()
    cache_base = os.path.join("data", "cache", "understat", args.league, str(args.season), run_id)
    ensure_dir(cache_base)

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO ingest_runs(run_id, source_id, league, season, started_at_utc, status)
            VALUES (?, 'understat', ?, ?, ?, 'RUNNING')
            """,
            (run_id, args.league, args.season, started)
        )
        conn.commit()

    try:
        results, teams, players = asyncio.run(fetch_all(args.league, args.season))

        # salva raw su disco (audit/debug)
        with open(os.path.join(cache_base, "league_results.json"), "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False)
        with open(os.path.join(cache_base, "teams.json"), "w", encoding="utf-8") as f:
            json.dump(teams, f, ensure_ascii=False)
        with open(os.path.join(cache_base, "players.json"), "w", encoding="utf-8") as f:
            json.dump(players, f, ensure_ascii=False)

        upsert_understat_data(run_id, args.league, args.season, results, teams, players, cache_base)
        print(f"OK: ingest understat done run_id={run_id} matches={len(results)} teams={len(teams)} players={len(players)}")

    except Exception as e:
        with get_conn() as conn:
            conn.execute(
                """
                UPDATE ingest_runs
                SET ended_at_utc = ?, status = 'ERROR', error = ?
                WHERE run_id = ?
                """,
                (utc_now_iso(), str(e), run_id)
            )
            conn.commit()
        raise


if __name__ == "__main__":
    main()
