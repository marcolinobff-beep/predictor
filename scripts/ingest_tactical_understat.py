from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Tuple
from uuid import uuid4

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


def _default_headers(match_id: str) -> dict:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": f"https://understat.com/match/{match_id}",
        "X-Requested-With": "XMLHttpRequest",
    }


def _to_float(value: object | None) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _share(home_val: Optional[float], away_val: Optional[float]) -> Optional[float]:
    if home_val is None or away_val is None:
        return None
    total = home_val + away_val
    if total <= 0:
        return None
    return home_val / total


def _parse_match_info(html: str) -> Optional[dict]:
    m = re.search(r"\bmatch_info\s*=\s*JSON\.parse\((['\"])(.*?)\1\)", html, re.DOTALL)
    if not m:
        return None
    payload = m.group(2)
    try:
        data = payload.encode("utf-8").decode("unicode_escape")
        return json.loads(data)
    except Exception:
        return None


def _fetch_match_info(match_id: str, timeout: int) -> Optional[dict]:
    url = f"https://understat.com/match/{match_id}"
    try:
        resp = requests.get(url, headers=_default_headers(match_id), timeout=timeout)
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    return _parse_match_info(resp.text)


def _ensure_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tactical_stats (
            match_id TEXT PRIMARY KEY,
            source TEXT,
            possession_home REAL,
            possession_away REAL,
            ppda_home REAL,
            ppda_away REAL
        )
        """
    )


def _table_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row)


def _list_ingested_ids(league: str, season: int) -> set[str]:
    with get_conn() as conn:
        if not _table_exists(conn, "tactical_stats"):
            return set()
        rows = conn.execute(
            """
            SELECT u.understat_match_id
            FROM understat_matches u
            JOIN tactical_stats t
              ON t.match_id = ('understat:' || u.understat_match_id)
            WHERE u.league = ? AND u.season = ?
            """,
            (league, season),
        ).fetchall()
    return {str(r["understat_match_id"]) for r in rows if r and r["understat_match_id"]}


def _list_seasons(league: str) -> List[int]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT season
            FROM understat_matches
            WHERE league = ?
            ORDER BY season
            """,
            (league,),
        ).fetchall()
    return [int(r["season"]) for r in rows if r and r["season"] is not None]


def _list_matches(
    league: str,
    season: int,
    since_dt: Optional[datetime] = None,
    until_dt: Optional[datetime] = None,
) -> List[str]:
    sql = """
        SELECT understat_match_id, datetime_utc
        FROM understat_matches
        WHERE league = ? AND season = ?
    """
    params: List[object] = [league, season]
    if since_dt:
        sql += " AND datetime_utc >= ?"
        params.append(since_dt.isoformat().replace("+00:00", "Z"))
    if until_dt:
        sql += " AND datetime_utc <= ?"
        params.append(until_dt.isoformat().replace("+00:00", "Z"))
    sql += " ORDER BY datetime_utc ASC"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [str(r["understat_match_id"]) for r in rows if r and r["understat_match_id"]]


def _already_ingested(match_id: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM tactical_stats WHERE match_id = ?",
            (match_id,),
        ).fetchone()
    return bool(row)


def _upsert_tactical(
    match_id: str,
    source: str,
    possession_home: Optional[float],
    possession_away: Optional[float],
    ppda_home: Optional[float],
    ppda_away: Optional[float],
) -> None:
    with get_conn() as conn:
        _ensure_table(conn)
        conn.execute(
            """
            INSERT INTO tactical_stats (
                match_id, source, possession_home, possession_away, ppda_home, ppda_away
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id) DO UPDATE SET
                source = excluded.source,
                possession_home = excluded.possession_home,
                possession_away = excluded.possession_away,
                ppda_home = excluded.ppda_home,
                ppda_away = excluded.ppda_away
            """,
            (match_id, source, possession_home, possession_away, ppda_home, ppda_away),
        )
        conn.commit()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", default=None, help="Comma separated season start years")
    ap.add_argument("--max-matches", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.7)
    ap.add_argument("--timeout", type=int, default=25)
    ap.add_argument("--update-existing", action="store_true")
    ap.add_argument("--all-seasons", action="store_true")
    ap.add_argument("--days-back", type=int, default=None)
    ap.add_argument("--days-ahead", type=int, default=None)
    args = ap.parse_args()

    seasons = []
    if args.seasons:
        for part in args.seasons.split(","):
            part = part.strip()
            if part.isdigit():
                seasons.append(int(part))
    if not seasons:
        seasons = _list_seasons(args.league)
        if seasons and not args.all_seasons:
            seasons = [max(seasons)]
    if not seasons:
        raise SystemExit("No seasons found for league in understat_matches.")

    inserted = 0
    skipped = 0
    failed = 0

    now = datetime.now(timezone.utc)
    since_dt = None
    until_dt = None
    if args.days_back is not None:
        since_dt = now - timedelta(days=int(args.days_back))
    if args.days_ahead is not None:
        until_dt = now + timedelta(days=int(args.days_ahead))

    for season in seasons:
        match_ids = _list_matches(args.league, season, since_dt=since_dt, until_dt=until_dt)
        if not args.update_existing:
            ingested = _list_ingested_ids(args.league, season)
            if ingested:
                match_ids = [mid for mid in match_ids if mid not in ingested]
        if args.max_matches:
            match_ids = match_ids[: args.max_matches]

        for understat_id in match_ids:
            match_id = f"understat:{understat_id}"
            if not args.update_existing and _already_ingested(match_id):
                skipped += 1
                continue

            info = _fetch_match_info(understat_id, args.timeout)
            if not info:
                failed += 1
                continue

            h_ppda = _to_float(info.get("h_ppda"))
            a_ppda = _to_float(info.get("a_ppda"))
            h_deep = _to_float(info.get("h_deep"))
            a_deep = _to_float(info.get("a_deep"))
            h_xg = _to_float(info.get("h_xg"))
            a_xg = _to_float(info.get("a_xg"))
            h_shots = _to_float(info.get("h_shot"))
            a_shots = _to_float(info.get("a_shot"))

            share = _share(h_deep, a_deep)
            if share is None:
                share = _share(h_xg, a_xg)
            if share is None:
                share = _share(h_shots, a_shots)

            pos_home = None
            pos_away = None
            if share is not None:
                pos_home = round(share * 100.0, 2)
                pos_away = round(100.0 - pos_home, 2)

            _upsert_tactical(
                match_id=match_id,
                source="understat_match_info",
                possession_home=pos_home,
                possession_away=pos_away,
                ppda_home=h_ppda,
                ppda_away=a_ppda,
            )
            inserted += 1

            if args.sleep > 0:
                time.sleep(args.sleep)

    print(f"OK: tactical ingest done inserted={inserted} skipped={skipped} failed={failed}")


if __name__ == "__main__":
    main()
