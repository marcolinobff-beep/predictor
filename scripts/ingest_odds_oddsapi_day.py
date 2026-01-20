from __future__ import annotations

import argparse
import os
import re
import sys
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
        "brightonandhovealbion": "brighton",
        "brightonhovealbion": "brighton",
        "afcbournemouth": "bournemouth",
        "manchesterutd": "manchesterunited",
        "manutd": "manchesterunited",
        "manunited": "manchesterunited",
        "manchestercity": "manchestercity",
        "mancity": "manchestercity",
        "newcastleunited": "newcastle",
        "newcastleutd": "newcastle",
        "westhamunited": "westham",
        "wolverhamptonwanderers": "wolves",
        "tottenhamhotspur": "tottenham",
        "spurs": "tottenham",
        "sheffieldutd": "sheffieldunited",
        "nottmforest": "nottinghamforest",
        "psg": "parissaintgermain",
        "parissg": "parissaintgermain",
        "parissaintgermain": "parissaintgermain",
        "bayernmunich": "bayernmunchen",
        "bayerleverkusen": "leverkusen",
        "borussiadortmund": "dortmund",
        "borussiamgladbach": "borussiamonchengladbach",
        "rbleipzig": "leipzig",
        "athleticbilbao": "athleticclub",
        "realbetisbalompie": "realbetis",
        "atleticomadrid": "atletico",
        "atleticodemadrid": "atletico",
        "realmadrid": "realmadrid",
        "fcbarcelona": "barcelona",
        "realsociedad": "realsociedad",
        "sevillafc": "sevilla",
        "elchecf": "elche",
        "villarrealcf": "villarreal",
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

    if not cols:
        raise RuntimeError("Nessuna colonna valida da inserire in odds_quotes (schema inatteso).")

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


def _parse_unsupported_markets(message: str) -> List[str]:
    if not message:
        return []
    m = re.search(r"Markets not supported by this endpoint:\s*(.+)", message, re.IGNORECASE)
    if not m:
        return []
    raw = m.group(1)
    return [part.strip() for part in raw.split(",") if part.strip()]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (UTC). Default: today")
    ap.add_argument("--competition", default="Serie_A")
    ap.add_argument("--sport-key", default="soccer_italy_serie_a")
    ap.add_argument("--regions", default="eu")
    ap.add_argument("--markets", default="h2h,totals")
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--bookmaker", default=None, help="Seleziona un singolo bookmaker (opzionale)")
    args = ap.parse_args()

    api_key = args.api_key or os.getenv("ODDS_API_KEY") or os.getenv("THE_ODDS_API_KEY")
    if not api_key:
        raise SystemExit("Missing ODDS_API_KEY/THE_ODDS_API_KEY. Set env var or pass --api-key.")

    day = date.fromisoformat(args.date) if args.date else datetime.now(timezone.utc).date()
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    with get_conn() as conn:
        matches = conn.execute(
            """
            SELECT match_id, home, away, kickoff_utc
            FROM matches
            WHERE kickoff_utc >= ? AND kickoff_utc < ?
              AND competition = ?
            ORDER BY kickoff_utc ASC
            """,
            (
                start.isoformat().replace("+00:00", "Z"),
                end.isoformat().replace("+00:00", "Z"),
                args.competition,
            ),
        ).fetchall()

    if not matches:
        print("No matches found for that date.")
        return

    match_map = {
        (_norm_team(m["home"]), _norm_team(m["away"])): m["match_id"]
        for m in matches
    }

    url = f"https://api.the-odds-api.com/v4/sports/{args.sport_key}/odds"
    markets = args.markets
    resp = None
    for attempt in range(2):
        params = {
            "apiKey": api_key,
            "regions": args.regions,
            "markets": markets,
            "oddsFormat": "decimal",
            "dateFormat": "iso",
        }
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 422:
            try:
                payload = resp.json()
                message = payload.get("message") or payload.get("error")
            except Exception:
                message = resp.text
            unsupported = _parse_unsupported_markets(message or "")
            if unsupported and attempt == 0:
                markets_list = [m.strip() for m in markets.split(",") if m.strip()]
                markets_list = [m for m in markets_list if m not in unsupported]
                if not markets_list:
                    break
                markets = ",".join(markets_list)
                continue
        resp.raise_for_status()
        break

    if resp is None:
        raise SystemExit("Odds API response missing.")
    events = resp.json()

    retrieved_at = _now_iso()
    batch_id = f"oddsapi_{uuid4()}"
    inserted = 0
    missing = []

    with get_conn() as conn:
        meta = detect_odds_table_cols(conn)
        for ev in events:
            home = ev.get("home_team")
            away = ev.get("away_team")
            commence = _parse_dt(ev.get("commence_time"))
            if not (home and away and commence):
                continue
            if not (start <= commence < end):
                continue

            key = (_norm_team(home), _norm_team(away))
            match_id = match_map.get(key)
            if not match_id:
                missing.append(f"{home} vs {away}")
                continue

            books = ev.get("bookmakers") or []
            if args.bookmaker:
                books = [b for b in books if (b.get("key") == args.bookmaker or b.get("title") == args.bookmaker)]
            if not books:
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
                        if name in ("HOME", home.upper()):
                            odds_rows.append(("1X2", "HOME", float(price)))
                        elif name in ("AWAY", away.upper()):
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
                        "quote_id": f"{match_id}:{bookmaker}:{market}:{selection}:{batch_id}",
                        "match_id": match_id,
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

        conn.commit()

    print(f"OK: inserted={inserted} missing_matches={len(missing)}")
    if missing:
        print("Missing:", ", ".join(missing[:10]))


if __name__ == "__main__":
    main()
