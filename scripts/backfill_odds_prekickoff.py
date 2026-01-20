from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone, date, timedelta
from typing import Dict, Any, List, Optional, Tuple
from uuid import uuid4

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _norm_team(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    aliases = {
        "internazionale": "inter",
        "intermilan": "inter",
        "interfc": "inter",
        "acmilan": "milan",
        "milanac": "milan",
        "asroma": "roma",
        "sscnapoli": "napoli",
        "ssclazio": "lazio",
        "sslazio": "lazio",
        "hellasverona": "verona",
        "parmacalcio1913": "parma",
        "como1907": "como",
        "uscremonese": "cremonese",
        "ussassuolo": "sassuolo",
        "uslecce": "lecce",
        "cagliaricalcio": "cagliari",
        "genoacfc": "genoa",
        "torinofc": "torino",
        "atalantabc": "atalanta",
        "acffiorentina": "fiorentina",
        "bolognafc": "bologna",
    }
    return aliases.get(s, s)


def _parse_dt(v: str) -> Optional[datetime]:
    if not v:
        return None
    s = v.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def detect_odds_table_cols(conn) -> Dict[str, bool]:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(odds_quotes)").fetchall()]
    return {
        "has_quote_id": "quote_id" in cols,
        "has_batch_id": "batch_id" in cols,
        "has_retrieved": "retrieved_at_utc" in cols,
        "has_source_id": "source_id" in cols,
        "has_reliability_score": "reliability_score" in cols,
        "has_ttl_seconds": "ttl_seconds" in cols,
        "has_cache_hit": "cache_hit" in cols,
        "has_raw_ref": "raw_ref" in cols,
    }


def _insert_odds_row(conn, meta: Dict[str, bool], payload: Dict[str, Any]) -> None:
    allowed_cols = [
        "quote_id", "match_id", "bookmaker", "market", "selection", "odds_decimal",
        "retrieved_at_utc", "batch_id", "source_id",
        "reliability_score", "ttl_seconds", "cache_hit", "raw_ref",
    ]

    cols = []
    vals = []
    for c in allowed_cols:
        if c in payload:
            if c == "quote_id" and not meta["has_quote_id"]:
                continue
            if c == "batch_id" and not meta["has_batch_id"]:
                continue
            if c == "source_id" and not meta["has_source_id"]:
                continue
            if c == "reliability_score" and not meta["has_reliability_score"]:
                continue
            if c == "ttl_seconds" and not meta["has_ttl_seconds"]:
                continue
            if c == "cache_hit" and not meta["has_cache_hit"]:
                continue
            if c == "raw_ref" and not meta["has_raw_ref"]:
                continue
            cols.append(c)
            vals.append(payload[c])

    placeholders = ", ".join(["?"] * len(cols))
    col_sql = ", ".join(cols)
    conn.execute(
        f"INSERT INTO odds_quotes ({col_sql}) VALUES ({placeholders})",
        tuple(vals),
    )


def _collect_market(markets: List[Dict[str, Any]], key: str) -> Optional[Dict[str, Any]]:
    for m in markets:
        if m.get("key") == key:
            return m
    return None


def _shorten(text: str, limit: int = 300) -> str:
    if not text:
        return ""
    text = " ".join(text.strip().split())
    if len(text) > limit:
        return text[:limit] + "..."
    return text


def _extract_error(resp: requests.Response) -> str:
    try:
        data = resp.json()
        if isinstance(data, dict):
            msg = data.get("message") or data.get("error") or data.get("errors")
            if msg:
                return _shorten(str(msg))
    except Exception:
        pass
    return _shorten(resp.text)


def _has_pre_kickoff_odds(conn, match_id: str, kickoff_utc: datetime, max_hours: int) -> bool:
    start = kickoff_utc - timedelta(hours=max_hours)
    row = conn.execute(
        """
        SELECT 1
        FROM odds_quotes
        WHERE match_id = ?
          AND retrieved_at_utc <= ?
          AND retrieved_at_utc >= ?
        LIMIT 1
        """,
        (
            match_id,
            kickoff_utc.isoformat().replace("+00:00", "Z"),
            start.isoformat().replace("+00:00", "Z"),
        ),
    ).fetchone()
    return bool(row)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", default="Serie_A")
    ap.add_argument("--start-date", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--end-date", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--hours-before", type=int, default=6)
    ap.add_argument("--max-check-hours", type=int, default=24)
    ap.add_argument("--sport-key", default="soccer_italy_serie_a")
    ap.add_argument("--regions", default="eu")
    ap.add_argument("--markets", default="h2h,totals")
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--max-matches", type=int, default=200)
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--progress-every", type=int, default=1)
    ap.add_argument("--connect-timeout", type=float, default=10.0)
    ap.add_argument("--read-timeout", type=float, default=30.0)
    ap.add_argument("--max-retries", type=int, default=2)
    ap.add_argument("--no-proxy", action="store_true")
    args = ap.parse_args()

    api_key = args.api_key or os.getenv("ODDS_API_KEY") or os.getenv("THE_ODDS_API_KEY")
    if not api_key:
        raise SystemExit("Missing ODDS_API_KEY/THE_ODDS_API_KEY. Set env var or pass --api-key.")

    start_day = date.fromisoformat(args.start_date)
    end_day = date.fromisoformat(args.end_date)
    if end_day < start_day:
        raise SystemExit("end-date must be >= start-date.")

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT match_id, home, away, kickoff_utc
            FROM matches
            WHERE competition = ?
              AND kickoff_utc >= ? AND kickoff_utc < ?
            ORDER BY kickoff_utc ASC
            """,
            (
                args.league,
                datetime(start_day.year, start_day.month, start_day.day, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
                datetime(end_day.year, end_day.month, end_day.day, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
            ),
        ).fetchall()

    matches = list(rows)[: args.max_matches]
    match_map = {
        (_norm_team(m["home"]), _norm_team(m["away"])): m["match_id"]
        for m in matches
    }

    snapshot_groups: Dict[str, List[Dict[str, Any]]] = {}
    with get_conn() as conn:
        for m in matches:
            kickoff = _parse_dt(m["kickoff_utc"])
            if not kickoff:
                continue
            if not args.force and _has_pre_kickoff_odds(conn, m["match_id"], kickoff, args.max_check_hours):
                continue
            snapshot = (kickoff - timedelta(hours=args.hours_before)).replace(microsecond=0)
            snapshot_key = snapshot.isoformat().replace("+00:00", "Z")
            snapshot_groups.setdefault(snapshot_key, []).append(m)

    inserted = 0
    skipped = 0
    groups = list(snapshot_groups.items())
    print(f"Backfill snapshots={len(groups)} matches={sum(len(v) for v in snapshot_groups.values())}")

    session = requests.Session()
    if args.no_proxy:
        session.trust_env = False

    with get_conn() as conn:
        meta = detect_odds_table_cols(conn)
        for idx, (snapshot, group_matches) in enumerate(groups, start=1):
            if args.progress_every > 0 and (idx % args.progress_every == 0 or idx == len(groups)):
                print(f"Fetching {idx}/{len(groups)} snapshot={snapshot} matches={len(group_matches)}", flush=True)

            url = f"https://api.the-odds-api.com/v4/sports/{args.sport_key}/odds-history/"
            params = {
                "apiKey": api_key,
                "regions": args.regions,
                "markets": args.markets,
                "oddsFormat": "decimal",
                "dateFormat": "iso",
                "date": snapshot,
            }
            resp = None
            last_err: Optional[Exception] = None
            for attempt in range(args.max_retries + 1):
                try:
                    resp = session.get(
                        url,
                        params=params,
                        timeout=(args.connect_timeout, args.read_timeout),
                    )
                    last_err = None
                    break
                except requests.RequestException as exc:
                    last_err = exc
                    if attempt < args.max_retries:
                        time.sleep(args.sleep)

            if last_err is not None or resp is None:
                skipped += len(group_matches)
                if args.progress_every > 0 and (idx % args.progress_every == 0 or idx == len(groups)):
                    print(f"Skip snapshot {snapshot} error={last_err}", flush=True)
                time.sleep(args.sleep)
                continue

            if resp.status_code == 401:
                err = _extract_error(resp)
                msg = "Historical odds require a paid plan or API key is invalid."
                if err:
                    msg += f" {err}"
                raise SystemExit(msg)

            if resp.status_code != 200:
                skipped += len(group_matches)
                if args.progress_every > 0 and (idx % args.progress_every == 0 or idx == len(groups)):
                    err = _extract_error(resp)
                    msg = f" status={resp.status_code}"
                    if err:
                        msg += f" error={err}"
                    print(f"Skip snapshot {snapshot}{msg}", flush=True)
                time.sleep(args.sleep)
                continue

            payload = resp.json()
            events = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(events, list):
                skipped += len(group_matches)
                time.sleep(args.sleep)
                continue

            retrieved_at = payload.get("timestamp") if isinstance(payload, dict) else None
            retrieved_at = retrieved_at or _now_iso()

            event_map = {}
            for ev in events:
                home = ev.get("home_team")
                away = ev.get("away_team")
                if home and away:
                    event_map[(_norm_team(home), _norm_team(away))] = ev

            batch_id = f"oddsapi_hist_{uuid4()}"

            for m in group_matches:
                key = (_norm_team(m["home"]), _norm_team(m["away"]))
                ev = event_map.get(key)
                if not ev:
                    skipped += 1
                    continue

                books = ev.get("bookmakers") or []
                if not books:
                    skipped += 1
                    continue

                for b in books:
                    markets = b.get("markets") or []
                    h2h = _collect_market(markets, "h2h")
                    totals = _collect_market(markets, "totals")
                    btts = _collect_market(markets, "btts")

                    odds_rows: List[Tuple[str, str, float]] = []

                    if h2h:
                        for o in h2h.get("outcomes", []):
                            name = (o.get("name") or "").upper()
                            price = o.get("price")
                            if not price:
                                continue
                            if name in ("HOME", m["home"].upper()):
                                odds_rows.append(("1X2", "HOME", float(price)))
                            elif name in ("AWAY", m["away"].upper()):
                                odds_rows.append(("1X2", "AWAY", float(price)))
                            elif name == "DRAW":
                                odds_rows.append(("1X2", "DRAW", float(price)))

                    if totals:
                        point = totals.get("point")
                        if point == 2.5:
                            for o in totals.get("outcomes", []):
                                name = (o.get("name") or "").upper()
                                price = o.get("price")
                                if not price:
                                    continue
                                if name == "OVER":
                                    odds_rows.append(("OU_2.5", "OVER", float(price)))
                                elif name == "UNDER":
                                    odds_rows.append(("OU_2.5", "UNDER", float(price)))

                    if btts:
                        for o in btts.get("outcomes", []):
                            name = (o.get("name") or "").upper()
                            price = o.get("price")
                            if not price:
                                continue
                            if name == "YES":
                                odds_rows.append(("BTTS", "YES", float(price)))
                            elif name == "NO":
                                odds_rows.append(("BTTS", "NO", float(price)))

                    if not odds_rows:
                        continue

                    source_id = f"odds_api:{args.sport_key}"
                    reliability_score = 0.85
                    ttl_seconds = 6 * 3600
                    raw_ref = f"oddsapi:{ev.get('id')}"
                    bookmaker = b.get("title") or b.get("key") or "ODDS_API"

                    for market, selection, odds_dec in odds_rows:
                        payload = {
                            "quote_id": f"{m['match_id']}:{bookmaker}:{market}:{selection}:{batch_id}",
                            "match_id": m["match_id"],
                            "bookmaker": bookmaker,
                            "market": market,
                            "selection": selection,
                            "odds_decimal": odds_dec,
                            "retrieved_at_utc": retrieved_at,
                            "batch_id": batch_id,
                            "source_id": source_id,
                            "reliability_score": reliability_score,
                            "ttl_seconds": ttl_seconds,
                            "cache_hit": 0,
                            "raw_ref": raw_ref,
                        }
                        _insert_odds_row(conn, meta, payload)
                        inserted += 1

            if args.progress_every > 0 and (idx % args.progress_every == 0 or idx == len(groups)):
                print(f"Progress {idx}/{len(groups)} snapshots | inserted={inserted} skipped={skipped}")

            time.sleep(args.sleep)

        conn.commit()

    print(f"OK: inserted={inserted} skipped={skipped}")


if __name__ == "__main__":
    main()
