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


async def fetch_fixtures(league: str, season: int):
    async with aiohttp.ClientSession() as session:
        us = Understat(session)
        # FIXTURES feed (match non ancora “nei risultati”, o futuri)
        fixtures = await us.get_league_fixtures(league, season)
        return fixtures


def to_kickoff_utc(dt_str: str) -> str:
    # Understat spesso: "YYYY-MM-DD HH:MM:SS"
    s = (dt_str or "").strip()
    if not s:
        return None
    if "T" not in s:
        s = s.replace(" ", "T")
    if not s.endswith("Z") and "+" not in s:
        s = s + "Z"
    return s


def upsert_fixtures_to_matches(league: str, season: int, fixtures):
    # Qui scriviamo DIRETTAMENTE in matches con match_id=understat:<id>
    # così non serve più riconciliare in emergenza.
    with get_conn() as conn:
        for m in fixtures:
            mid = "understat:" + str(m.get("id"))
            dt_utc = to_kickoff_utc(m.get("datetime"))
            h = m.get("h") or {}
            a = m.get("a") or {}
            home = h.get("title") or h.get("short_title") or "UNKNOWN_HOME"
            away = a.get("title") or a.get("short_title") or "UNKNOWN_AWAY"
            venue = m.get("venue")

            conn.execute(
                """
                INSERT OR REPLACE INTO matches(match_id, competition, season, kickoff_utc, home, away, venue)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (mid, league, f"{season}/{str(season+1)[-2:]}", dt_utc, home, away, venue),
            )
        conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--season", required=True, type=int)
    args = ap.parse_args()

    run_id = str(uuid4())
    cache_base = os.path.join("data", "cache", "understat", args.league, str(args.season), run_id)
    ensure_dir(cache_base)

    fixtures = asyncio.run(fetch_fixtures(args.league, args.season))

    with open(os.path.join(cache_base, "league_fixtures.json"), "w", encoding="utf-8") as f:
        json.dump(fixtures, f, ensure_ascii=False)

    upsert_fixtures_to_matches(args.league, args.season, fixtures)
    print(f"OK: fixtures ingested run_id={run_id} fixtures={len(fixtures)} cache={cache_base}")


if __name__ == "__main__":
    main()
