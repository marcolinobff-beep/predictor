from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone, date, timedelta
from typing import Dict, List, Optional, Tuple

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.core.ids import stable_hash
from app.db.sqlite import get_conn


FIELD_SEP = chr(172)
KV_SEP = chr(247)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", value.lower().replace("_", " ")).strip()


def _load_aliases(path: Optional[str]) -> Dict[str, List[str]]:
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise SystemExit("Aliases file must be a dict: team -> [aliases].")
    return {k: v for k, v in data.items() if isinstance(v, list)}


def _team_alias_map(conn, aliases: Dict[str, List[str]]) -> Dict[str, str]:
    rows = conn.execute(
        """
        SELECT DISTINCT home AS team FROM matches
        UNION
        SELECT DISTINCT away AS team FROM matches
        """
    ).fetchall()
    teams = [r["team"] for r in rows if r and r["team"]]

    alias_map: Dict[str, str] = {}
    for team in teams:
        alias_map[_normalize_text(team)] = team
        for a in aliases.get(team, []):
            alias_map[_normalize_text(a)] = team
    return alias_map


def _fetch_environment(league_url: str) -> Dict[str, object]:
    resp = requests.get(league_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    m = re.search(r"window\\.environment\\s*=\\s*(\\{.*?\\});", resp.text, re.DOTALL)
    if m:
        return json.loads(m.group(1))

    idx = resp.text.find("window.environment")
    if idx == -1:
        raise RuntimeError("window.environment not found on diretta page.")

    start = resp.text.find("{", idx)
    if start == -1:
        raise RuntimeError("window.environment JSON start not found.")

    depth = 0
    end = None
    in_str = False
    escape = False
    for i, ch in enumerate(resp.text[start:], start=start):
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == "\"":
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end is None:
        raise RuntimeError("window.environment JSON end not found.")

    return json.loads(resp.text[start:end])


def _parse_events(html: str) -> List[Dict[str, str]]:
    pattern = f"{FIELD_SEP}~AA{KV_SEP}"
    if pattern not in html:
        return []
    parts = html.split(pattern)
    events = []
    for part in parts[1:]:
        event_id = part[:8]
        chunk = part[:2000]
        fields = {}
        for seg in chunk.split(FIELD_SEP):
            if KV_SEP in seg:
                k, v = seg.split(KV_SEP, 1)
                fields[k] = v
        home = fields.get("CX")
        away = fields.get("AF")
        ts = fields.get("AD")
        if home and away:
            events.append({
                "event_id": event_id,
                "home": home,
                "away": away,
                "kickoff_ts": ts,
            })
    return events


def _parse_lineups(feed_text: str) -> Tuple[List[str], List[str]]:
    home_players: List[str] = []
    away_players: List[str] = []
    current_team = None

    for seg in feed_text.split(FIELD_SEP):
        if KV_SEP not in seg:
            continue
        key, value = seg.split(KV_SEP, 1)
        key = key.lstrip("~")
        if key == "LC":
            if value in ("1", "2"):
                current_team = value
        elif key == "LI" and current_team:
            if current_team == "1":
                if value not in home_players:
                    home_players.append(value)
            elif current_team == "2":
                if value not in away_players:
                    away_players.append(value)

    return home_players, away_players


def _list_matches_for_day(conn, day_utc: date, competition: str):
    start = datetime(day_utc.year, day_utc.month, day_utc.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return conn.execute(
        """
        SELECT match_id, kickoff_utc, home, away
        FROM matches
        WHERE competition = ?
          AND kickoff_utc >= ? AND kickoff_utc < ?
        ORDER BY kickoff_utc ASC
        """,
        (
            competition,
            start.isoformat().replace("+00:00", "Z"),
            end.isoformat().replace("+00:00", "Z"),
        ),
    ).fetchall()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--competition", default="Serie_A")
    ap.add_argument("--league-url", default="https://www.diretta.it/calcio/italia/serie-a/")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (UTC). Default: today")
    ap.add_argument("--aliases", default="news_team_aliases.json")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    day = date.fromisoformat(args.date) if args.date else _now_utc().date()
    env = _fetch_environment(args.league_url)
    feed_sign = env.get("config", {}).get("app", {}).get("feed_sign")
    if not feed_sign:
        raise SystemExit("feed_sign missing in diretta environment.")

    resp = requests.get(args.league_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    events = _parse_events(resp.text)
    if not events:
        raise SystemExit("No events parsed from diretta page.")

    with get_conn() as conn:
        alias_map = _team_alias_map(conn, _load_aliases(args.aliases))
        matches = _list_matches_for_day(conn, day, args.competition)

        def _norm(team: str) -> str:
            return alias_map.get(_normalize_text(team), team)

        event_map = {
            (_normalize_text(_norm(e["home"])), _normalize_text(_norm(e["away"]))): e
            for e in events
        }

        inserted = 0
        skipped = 0

        for m in matches:
            key = (_normalize_text(_norm(m["home"])), _normalize_text(_norm(m["away"])))
            ev = event_map.get(key)
            if not ev:
                skipped += 1
                continue

            event_id = ev["event_id"]
            feed_url = f"https://www.diretta.it/x/feed/df_li_1_{event_id}"
            headers = {"User-Agent": "Mozilla/5.0", "x-fsign": feed_sign}
            feed_resp = requests.get(feed_url, headers=headers, timeout=20)
            if feed_resp.status_code != 200:
                skipped += 1
                continue

            home_players, away_players = _parse_lineups(feed_resp.text)
            if len(home_players) < 9 or len(away_players) < 9:
                skipped += 1
                continue

            lineup_id = stable_hash({
                "source": "Diretta.it",
                "match_id": m["match_id"],
                "event_id": event_id,
                "home_players": home_players,
                "away_players": away_players,
            })

            if args.dry_run:
                inserted += 1
                continue

            conn.execute(
                """
                INSERT OR REPLACE INTO probable_lineups
                  (lineup_id, match_id, source, fetched_at_utc, confidence,
                   home_players_json, away_players_json, notes, raw_ref)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lineup_id,
                    m["match_id"],
                    "Diretta.it",
                    _now_utc().isoformat().replace("+00:00", "Z"),
                    0.82,
                    json.dumps(home_players, ensure_ascii=True),
                    json.dumps(away_players, ensure_ascii=True),
                    "parsed_from_feed",
                    feed_url,
                ),
            )
            inserted += 1

        if not args.dry_run:
            conn.commit()

    print(f"OK: inserted={inserted} skipped={skipped}")


if __name__ == "__main__":
    main()
