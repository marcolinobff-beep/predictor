from __future__ import annotations

import argparse
import json
import math
import random
from datetime import datetime, timezone, date, timedelta
from typing import Dict, Any, List, Tuple
from uuid import uuid4

from app.core.ids import stable_hash
from app.db.sqlite import get_conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _poisson_pmf(lam: float, k: int) -> float:
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def _match_probs(lam_h: float, lam_a: float, cap: int = 8) -> Dict[str, float]:
    p_h = [ _poisson_pmf(lam_h, k) for k in range(cap + 1) ]
    p_a = [ _poisson_pmf(lam_a, k) for k in range(cap + 1) ]

    p_home = 0.0
    p_draw = 0.0
    p_away = 0.0
    p_over = 0.0
    p_btts_yes = 0.0

    for i, ph in enumerate(p_h):
        for j, pa in enumerate(p_a):
            p = ph * pa
            if i > j:
                p_home += p
            elif i == j:
                p_draw += p
            else:
                p_away += p
            if i + j >= 3:
                p_over += p
            if i > 0 and j > 0:
                p_btts_yes += p

    return {
        "home_win": p_home,
        "draw": p_draw,
        "away_win": p_away,
        "over_2_5": p_over,
        "under_2_5": 1.0 - p_over,
        "btts_yes": p_btts_yes,
        "btts_no": 1.0 - p_btts_yes,
    }


def _select_value(prob_map: Dict[str, float], options: List[str]) -> str:
    return max(options, key=lambda k: prob_map[k])


def _odds_for_selection(prob: float, mult: float) -> float:
    if prob <= 0:
        return 100.0
    return max(1.01, round((1.0 / prob) * mult, 2))


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


def _seed_for_match(match_id: str, seed: int) -> random.Random:
    h = stable_hash({"match_id": match_id, "seed": seed})
    return random.Random(int(h[:8], 16))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
    p.add_argument("--competition", default=None)
    p.add_argument("--features-version", default="demo_v1")
    p.add_argument("--bookmaker", default="DEMO")
    p.add_argument("--value-boost", type=float, default=1.08)
    p.add_argument("--other-mult", type=float, default=0.97)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    day = date.fromisoformat(args.date)
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    start_iso = start.isoformat().replace("+00:00", "Z")
    end_iso = end.isoformat().replace("+00:00", "Z")

    with get_conn() as conn:
        sql = """
            SELECT match_id, competition, home, away, kickoff_utc
            FROM matches
            WHERE kickoff_utc >= ? AND kickoff_utc < ?
        """
        params: List[Any] = [start_iso, end_iso]
        if args.competition:
            sql += " AND competition = ?"
            params.append(args.competition)
        sql += " ORDER BY kickoff_utc ASC"
        matches = conn.execute(sql, params).fetchall()

        if not matches:
            print("No matches found for that date.")
            return

        meta = detect_odds_table_cols(conn)
        inserted_odds = 0
        inserted_features = 0

        for m in matches:
            rng = _seed_for_match(m["match_id"], args.seed)
            lam_h = round(0.9 + rng.random() * 1.1, 2)
            lam_a = round(0.7 + rng.random() * 1.0, 2)

            features = {"lambda_home": lam_h, "lambda_away": lam_a}
            created_at_utc = _now_iso()
            conn.execute(
                """
                INSERT OR REPLACE INTO match_features (match_id, features_version, features_json, created_at_utc)
                VALUES (?, ?, ?, ?)
                """,
                (m["match_id"], args.features_version, json.dumps(features), created_at_utc),
            )
            inserted_features += 1

            probs = _match_probs(lam_h, lam_a, cap=8)

            value_1x2 = _select_value(probs, ["home_win", "draw", "away_win"])
            value_ou = "over_2_5" if probs["over_2_5"] >= probs["under_2_5"] else "under_2_5"
            value_btts = "btts_yes" if probs["btts_yes"] >= probs["btts_no"] else "btts_no"

            odds_map = {
                ("1X2", "HOME"): _odds_for_selection(probs["home_win"], args.value_boost if value_1x2 == "home_win" else args.other_mult),
                ("1X2", "DRAW"): _odds_for_selection(probs["draw"], args.value_boost if value_1x2 == "draw" else args.other_mult),
                ("1X2", "AWAY"): _odds_for_selection(probs["away_win"], args.value_boost if value_1x2 == "away_win" else args.other_mult),
                ("OU_2.5", "OVER"): _odds_for_selection(probs["over_2_5"], args.value_boost if value_ou == "over_2_5" else args.other_mult),
                ("OU_2.5", "UNDER"): _odds_for_selection(probs["under_2_5"], args.value_boost if value_ou == "under_2_5" else args.other_mult),
                ("BTTS", "YES"): _odds_for_selection(probs["btts_yes"], args.value_boost if value_btts == "btts_yes" else args.other_mult),
                ("BTTS", "NO"): _odds_for_selection(probs["btts_no"], args.value_boost if value_btts == "btts_no" else args.other_mult),
            }

            batch_id = str(uuid4())
            retrieved_at = _now_iso()
            for (market, selection), odds_dec in odds_map.items():
                payload = {
                    "quote_id": str(uuid4()),
                    "match_id": m["match_id"],
                    "batch_id": batch_id,
                    "source_id": "demo:seed",
                    "reliability_score": 0.6,
                    "bookmaker": args.bookmaker,
                    "market": market,
                    "selection": selection,
                    "odds_decimal": float(odds_dec),
                    "retrieved_at_utc": retrieved_at,
                    "raw_ref": f"seed:{args.date}",
                }
                _insert_odds_row(conn, meta, payload)
                inserted_odds += 1

        conn.commit()

    print(f"OK: features={inserted_features} odds={inserted_odds} matches={len(matches)}")


if __name__ == "__main__":
    main()
