from __future__ import annotations

import argparse
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Dict, Any, List, Optional, Tuple

import httpx

from app.core.ids import stable_hash
from app.db.sqlite import get_conn


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", value.lower().replace("_", " ")).strip()


def _load_sources(path: str) -> List[Dict[str, Any]]:
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


def _detect_related_team(text: str, alias_map: Dict[str, str]) -> Optional[str]:
    t = _normalize_text(text)
    for alias_norm, team in alias_map.items():
        if alias_norm and alias_norm in t:
            return team
    return None


def _detect_match_id(text: str, alias_map: Dict[str, str], match_pairs: List[Tuple[str, str, str]]) -> Optional[str]:
    t = _normalize_text(text)
    for match_id, home, away in match_pairs:
        h = _normalize_text(home)
        a = _normalize_text(away)
        if h and a and h in t and a in t:
            return match_id
    return None


def _detect_event_type(text: str) -> Optional[str]:
    t = _normalize_text(text)
    if re.search(r"(infortun|injur|out|doubt)", t):
        return "injury"
    if re.search(r"(squalif|suspens|ban)", t):
        return "suspension"
    if re.search(r"(ammon|yellow card|red card|cartellin)", t):
        return "cards"
    if re.search(r"(lineup|starting xi|probable)", t):
        return "lineup"
    return None


def _parse_rss_items(xml_text: str) -> List[Dict[str, str]]:
    root = ET.fromstring(xml_text)

    items: List[Dict[str, str]] = []

    # RSS 2.0
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        desc = (item.findtext("description") or "").strip()
        guid = (item.findtext("guid") or "").strip()
        items.append({"title": title, "link": link, "published": pub, "summary": desc, "guid": guid})

    # Atom
    for entry in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
        title = (entry.findtext("{http://www.w3.org/2005/Atom}title") or "").strip()
        link_el = entry.find("{http://www.w3.org/2005/Atom}link")
        link = (link_el.get("href") if link_el is not None else "") or ""
        pub = (entry.findtext("{http://www.w3.org/2005/Atom}updated") or "").strip()
        if not pub:
            pub = (entry.findtext("{http://www.w3.org/2005/Atom}published") or "").strip()
        summary = (entry.findtext("{http://www.w3.org/2005/Atom}summary") or "").strip()
        guid = (entry.findtext("{http://www.w3.org/2005/Atom}id") or "").strip()
        items.append({"title": title, "link": link, "published": pub, "summary": summary, "guid": guid})

    return items


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--sources", default="news_sources.json", help="Path to sources JSON")
    p.add_argument("--aliases", default=None, help="Optional team aliases JSON")
    p.add_argument("--limit-per-source", type=int, default=30)
    p.add_argument("--since-hours", type=int, default=72)
    p.add_argument("--require-team-match", action="store_true", help="Skip items without team or match link")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    sources = _load_sources(args.sources)
    aliases = _load_aliases(args.aliases)

    now_utc = _now_utc()
    min_dt = now_utc - timedelta(hours=args.since_hours)

    inserted = 0
    skipped_old = 0
    skipped_existing = 0
    skipped_unmatched = 0

    with get_conn() as conn:
        alias_map, match_pairs = _team_maps(conn, aliases)

        for src in sources:
            name = src.get("name") or "UNKNOWN"
            url = src.get("url")
            if not url:
                continue
            reliability = float(src.get("reliability_score", 0.6))

            try:
                resp = httpx.get(url, timeout=15.0)
                resp.raise_for_status()
            except Exception:
                continue

            try:
                items = _parse_rss_items(resp.text)
            except Exception:
                continue

            count = 0
            for item in items:
                if count >= args.limit_per_source:
                    break

                title = item.get("title", "").strip()
                if not title:
                    continue
                link = item.get("link", "").strip()
                summary = item.get("summary", "").strip()
                published_raw = item.get("published", "")
                published_dt = _parse_dt(published_raw) or now_utc

                if published_dt < min_dt:
                    skipped_old += 1
                    continue

                related_team = _detect_related_team(f"{title} {summary}", alias_map)
                related_match_id = _detect_match_id(f"{title} {summary}", alias_map, match_pairs)
                event_type = _detect_event_type(f"{title} {summary}")

                if args.require_team_match and not (related_team or related_match_id):
                    skipped_unmatched += 1
                    continue

                news_id = stable_hash({
                    "source": name,
                    "title": title,
                    "link": link,
                    "published_at_utc": published_dt.isoformat(),
                })

                payload = {
                    "news_id": news_id,
                    "source": name,
                    "title": title,
                    "url": link or None,
                    "published_at_utc": published_dt.isoformat(),
                    "reliability_score": reliability,
                    "related_match_id": related_match_id,
                    "related_team": related_team,
                    "related_player": None,
                    "event_type": event_type,
                    "summary": summary or None,
                    "raw_json": json.dumps(item, ensure_ascii=True),
                }

                if args.dry_run:
                    inserted += 1
                    count += 1
                    continue

                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO news_articles (
                      news_id, source, title, url, published_at_utc, reliability_score,
                      related_match_id, related_team, related_player, event_type, summary, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["news_id"],
                        payload["source"],
                        payload["title"],
                        payload["url"],
                        payload["published_at_utc"],
                        payload["reliability_score"],
                        payload["related_match_id"],
                        payload["related_team"],
                        payload["related_player"],
                        payload["event_type"],
                        payload["summary"],
                        payload["raw_json"],
                    ),
                )
                if cur.rowcount == 0:
                    skipped_existing += 1
                else:
                    inserted += 1
                count += 1

        if not args.dry_run:
            conn.commit()

    print(f"OK: inserted={inserted} skipped_old={skipped_old} skipped_existing={skipped_existing} skipped_unmatched={skipped_unmatched}")


if __name__ == "__main__":
    main()
