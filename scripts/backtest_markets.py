from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn
from app.core.calibration import load_calibration, apply_calibration, select_calibration, select_league_calibration
from app.core.dc_params import get_rho
from app.core.probabilities import match_probs
from app.services.market_rules_service import get_market_rules


def _season_label(season_start: int) -> str:
    return f"{season_start}/{str(season_start + 1)[-2:]}"


def _brier(records: List[Tuple[float, int]]) -> float:
    if not records:
        return 0.0
    return sum((p - o) ** 2 for p, o in records) / len(records)


def _logloss_1x2(p_home: float, p_draw: float, p_away: float, outcome: str) -> float:
    eps = 1e-12
    if outcome == "H":
        return -math.log(max(p_home, eps))
    if outcome == "D":
        return -math.log(max(p_draw, eps))
    return -math.log(max(p_away, eps))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", required=True, help="Comma separated season start years, es: 2023,2024,2025")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--calibration", default="data/calibration/calibration_by_season.json")
    ap.add_argument("--dc-params", default="data/calibration/dc_params.json")
    ap.add_argument("--min-edge", type=float, default=None)
    ap.add_argument("--max-odds", type=float, default=6.0)
    ap.add_argument("--odds-age-hours", type=int, default=12)
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    season_labels = {_season_label(s) for s in seasons}

    cal = load_calibration(args.calibration)
    cal = select_league_calibration(cal, args.league)
    rho = get_rho(args.dc_params, args.league)
    min_edge = float(args.min_edge) if args.min_edge is not None else float(get_market_rules().get("min_edge", 0.03))

    brier_records: Dict[str, List[Tuple[float, int]]] = {k: [] for k in [
        "home_win", "draw", "away_win", "over_2_5", "under_2_5", "btts_yes", "btts_no"
    ]}
    logloss = []
    picks = 0
    profit = 0.0
    clv_list: List[float] = []

    with get_conn() as conn:
        matches = conn.execute(
            """
            SELECT match_id, season, kickoff_utc
            FROM matches
            WHERE competition = ?
            """,
            (args.league,),
        ).fetchall()

        for m in matches:
            if m["season"] not in season_labels:
                continue
            match_id = m["match_id"]
            if not match_id.startswith("understat:"):
                continue

            feat = conn.execute(
                """
                SELECT features_json
                FROM match_features
                WHERE match_id = ? AND features_version = ?
                ORDER BY created_at_utc DESC
                LIMIT 1
                """,
                (match_id, args.features_version),
            ).fetchone()
            if not feat:
                continue

            understat_id = match_id.split(":", 1)[1]
            us = conn.execute(
                """
                SELECT home_goals, away_goals
                FROM understat_matches
                WHERE understat_match_id = ?
                """,
                (understat_id,),
            ).fetchone()
            if not us or us["home_goals"] is None or us["away_goals"] is None:
                continue

            features = json.loads(feat["features_json"])
            lam_h = float(features.get("lambda_home", 0.0))
            lam_a = float(features.get("lambda_away", 0.0))
            if lam_h <= 0 or lam_a <= 0:
                continue

            probs = match_probs(lam_h, lam_a, cap=8, rho=rho)
            if cal:
                kickoff = datetime.fromisoformat(str(m["kickoff_utc"]).replace("Z", "+00:00"))
                cal_sel = select_calibration(cal, m["season"], kickoff)
                if cal_sel:
                    probs = apply_calibration(probs, cal_sel)

            hg = int(us["home_goals"])
            ag = int(us["away_goals"])

            brier_records["home_win"].append((probs["home_win"], 1 if hg > ag else 0))
            brier_records["draw"].append((probs["draw"], 1 if hg == ag else 0))
            brier_records["away_win"].append((probs["away_win"], 1 if hg < ag else 0))
            brier_records["over_2_5"].append((probs["over_2_5"], 1 if (hg + ag) >= 3 else 0))
            brier_records["under_2_5"].append((probs["under_2_5"], 1 if (hg + ag) <= 2 else 0))
            brier_records["btts_yes"].append((probs["btts_yes"], 1 if (hg > 0 and ag > 0) else 0))
            brier_records["btts_no"].append((probs["btts_no"], 1 if (hg == 0 or ag == 0) else 0))

            outcome = "H" if hg > ag else ("D" if hg == ag else "A")
            logloss.append(_logloss_1x2(probs["home_win"], probs["draw"], probs["away_win"], outcome))

            # ROI/CLV: usare ultimo snapshot pre-kickoff
            kickoff = m["kickoff_utc"]
            odds_rows = conn.execute(
                """
                SELECT market, selection, odds_decimal, retrieved_at_utc, source_id
                FROM odds_quotes
                WHERE match_id = ?
                  AND retrieved_at_utc <= ?
                """,
                (match_id, kickoff),
            ).fetchall()
            if not odds_rows:
                continue

            pre_rows = [
                r for r in odds_rows
                if not (r["source_id"] and "closing" in str(r["source_id"]))
            ]
            if not pre_rows:
                pre_rows = odds_rows

            # pick migliore per edge
            best_pick = None
            for r in pre_rows:
                market = r["market"].upper()
                selection = r["selection"].upper()
                odds_dec = float(r["odds_decimal"])
                if odds_dec <= 1.01 or odds_dec > args.max_odds:
                    continue
                key = None
                if market == "1X2":
                    key = {"HOME": "home_win", "DRAW": "draw", "AWAY": "away_win"}.get(selection)
                elif market == "OU_2.5":
                    key = {"OVER": "over_2_5", "UNDER": "under_2_5"}.get(selection)
                elif market == "BTTS":
                    key = {"YES": "btts_yes", "NO": "btts_no"}.get(selection)
                if not key:
                    continue
                p = probs.get(key)
                if p is None:
                    continue
                edge = p * odds_dec - 1.0
                if edge < min_edge:
                    continue
                if not best_pick or edge > best_pick["edge"]:
                    best_pick = {"market": market, "selection": selection, "odds": odds_dec, "edge": edge}

            if not best_pick:
                continue

            picks += 1
            win = False
            if best_pick["market"] == "1X2":
                if best_pick["selection"] == "HOME" and outcome == "H":
                    win = True
                if best_pick["selection"] == "DRAW" and outcome == "D":
                    win = True
                if best_pick["selection"] == "AWAY" and outcome == "A":
                    win = True
            elif best_pick["market"] == "OU_2.5":
                total = hg + ag
                if best_pick["selection"] == "OVER" and total >= 3:
                    win = True
                if best_pick["selection"] == "UNDER" and total <= 2:
                    win = True
            elif best_pick["market"] == "BTTS":
                btts = (hg > 0 and ag > 0)
                if best_pick["selection"] == "YES" and btts:
                    win = True
                if best_pick["selection"] == "NO" and not btts:
                    win = True

            profit += (best_pick["odds"] - 1.0) if win else -1.0

            # CLV: confronta con ultima quota pre-kickoff per stessa selezione
            closing_rows = [
                r for r in odds_rows
                if r["source_id"] and "closing" in str(r["source_id"])
                and r["market"] == best_pick["market"]
                and r["selection"] == best_pick["selection"]
            ]
            closing_odds = None
            if closing_rows:
                closing_rows.sort(key=lambda x: x["retrieved_at_utc"])
                closing_odds = float(closing_rows[-1]["odds_decimal"])
            else:
                # fallback: use last available pre snapshot only if we have >1 distinct timestamps
                ts_rows = [
                    r for r in pre_rows
                    if r["market"] == best_pick["market"] and r["selection"] == best_pick["selection"]
                ]
                ts_vals = sorted({str(r["retrieved_at_utc"]) for r in ts_rows})
                if len(ts_vals) >= 2:
                    last_ts = ts_vals[-1]
                    for r in ts_rows:
                        if str(r["retrieved_at_utc"]) == last_ts:
                            closing_odds = float(r["odds_decimal"])
                            break

            if closing_odds:
                clv = (closing_odds / best_pick["odds"]) - 1.0
                clv_list.append(clv)

    brier = {k: _brier(v) for k, v in brier_records.items()}
    avg_logloss = sum(logloss) / len(logloss) if logloss else 0.0
    roi = profit / picks if picks > 0 else 0.0
    avg_clv = sum(clv_list) / len(clv_list) if clv_list else None

    print("Backtest summary")
    print("Brier:", brier)
    print("LogLoss_1X2:", round(avg_logloss, 4))
    print("Picks:", picks, "ROI/unit:", round(roi, 3))
    if avg_clv is None:
        print("Avg_CLV: NA", "samples:", len(clv_list))
    else:
        print("Avg_CLV:", round(avg_clv, 4), "samples:", len(clv_list))


if __name__ == "__main__":
    main()
