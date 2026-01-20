from __future__ import annotations

import argparse
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from app.core.ids import stable_hash
from app.db.sqlite import get_conn


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", value.lower().replace("_", " ")).strip()


def _load_sources(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise SystemExit("Sources file must be a list of objects.")
    return data


def _load_aliases(path: Optional[str]) -> Dict[str, List[str]]:
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise SystemExit("Aliases file must be a dict: team -> [aliases].")
    return {k: v for k, v in data.items() if isinstance(v, list)}


def _team_maps(conn, aliases: Dict[str, List[str]]):
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

    matches = conn.execute(
        "SELECT match_id, home, away FROM matches"
    ).fetchall()
    match_pairs = [(m["match_id"], m["home"], m["away"]) for m in matches]
    return alias_map, match_pairs


def _detect_match_id(text: str, match_pairs: List[Tuple[str, str, str]]) -> Optional[str]:
    t = _normalize_text(text)
    for match_id, home, away in match_pairs:
        h = _normalize_text(home)
        a = _normalize_text(away)
        if h and a and h in t and a in t:
            return match_id
    return None


def _parse_rss_items(xml_text: str) -> List[Dict[str, str]]:
    root = ET.fromstring(xml_text)
    items: List[Dict[str, str]] = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        desc = (item.findtext("description") or "").strip()
        guid = (item.findtext("guid") or "").strip()
        items.append({"title": title, "link": link, "summary": desc, "guid": guid})
    return items


def _extract_lineups(text: str, alias_map: Dict[str, str]) -> Optional[Tuple[str, List[str], str, List[str]]]:
    # Cerca pattern tipo "TEAM: player1, player2 ... - TEAM: player1, player2 ..."
    cleaned = text.replace("\n", " ").replace("\r", " ")
    # Split greedy on " - " if present
    parts = [p.strip() for p in re.split(r"\s+-\s+", cleaned) if p.strip()]
    if len(parts) < 2:
        return None

    parsed = []
    for part in parts[:2]:
        if ":" not in part:
            continue
        team_raw, players_raw = part.split(":", 1)
        team = alias_map.get(_normalize_text(team_raw), team_raw.strip())
        players = [p.strip() for p in players_raw.split(",") if p.strip()]
        if team and players:
            parsed.append((team, players))

    if len(parsed) < 2:
        return None

    (home_team, home_players), (away_team, away_players) = parsed[0], parsed[1]
    return home_team, home_players, away_team, away_players


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--sources", default="lineup_sources.json")
    p.add_argument("--aliases", default="news_team_aliases.json")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    sources = _load_sources(args.sources)
    aliases = _load_aliases(args.aliases)

    inserted = 0
    skipped = 0

    with get_conn() as conn:
        alias_map, match_pairs = _team_maps(conn, aliases)

        for src in sources:
            url = src.get("url") or ""
            if not url:
                continue
            if src.get("type") != "rss":
                continue

            try:
                resp = httpx.get(url, timeout=15.0)
                resp.raise_for_status()
            except Exception:
                continue

            try:
                items = _parse_rss_items(resp.text)
            except Exception:
                continue

            for item in items:
                title = item.get("title") or ""
                summary = item.get("summary") or ""
                text = f"{title} {summary}"

                if not re.search(r"probabil|formazion|lineup", text, re.IGNORECASE):
                    skipped += 1
                    continue

                match_id = _detect_match_id(text, match_pairs)
                if not match_id:
                    skipped += 1
                    continue

                parsed = _extract_lineups(summary, alias_map)
                if not parsed:
                    skipped += 1
                    continue

                home_team, home_players, away_team, away_players = parsed
                if len(home_players) < 7 or len(away_players) < 7:
                    skipped += 1
                    continue

                lineup_id = stable_hash({
                    "source": src.get("name"),
                    "match_id": match_id,
                    "home": home_team,
                    "away": away_team,
                    "players_home": home_players,
                    "players_away": away_players,
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
                        match_id,
                        src.get("name") or "UNKNOWN",
                        _now_utc().isoformat().replace("+00:00", "Z"),
                        float(src.get("reliability_score", 0.6)),
                        json.dumps(home_players, ensure_ascii=True),
                        json.dumps(away_players, ensure_ascii=True),
                        "parsed_from_rss",
                        item.get("link") or item.get("guid") or None,
                    ),
                )
                inserted += 1

        if not args.dry_run:
            conn.commit()

    print(f"OK: inserted={inserted} skipped={skipped}")


if __name__ == "__main__":
    main()
