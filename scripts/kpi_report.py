from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Tuple

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


def _logloss_binary(p: float, outcome: bool) -> float:
    eps = 1e-12
    if outcome:
        return -math.log(max(p, eps))
    return -math.log(max(1.0 - p, eps))


def _phase_for_date(dt: datetime) -> str:
    m = dt.month
    if m in (8, 9, 10):
        return "early"
    if m in (11, 12, 1, 2):
        return "mid"
    return "late"


def _init_brier_records() -> Dict[str, List[Tuple[float, int]]]:
    return {k: [] for k in [
        "home_win", "draw", "away_win", "over_2_5", "under_2_5", "btts_yes", "btts_no"
    ]}


def _brier_by_market(brier: Dict[str, float]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not brier:
        return out
    parts_1x2 = [brier.get("home_win"), brier.get("draw"), brier.get("away_win")]
    if all(v is not None for v in parts_1x2):
        out["1X2"] = sum(parts_1x2) / 3.0
    parts_ou = [brier.get("over_2_5"), brier.get("under_2_5")]
    if all(v is not None for v in parts_ou):
        out["OU_2.5"] = sum(parts_ou) / 2.0
    parts_btts = [brier.get("btts_yes"), brier.get("btts_no")]
    if all(v is not None for v in parts_btts):
        out["BTTS"] = sum(parts_btts) / 2.0
    return out


def _init_picks_by_market() -> Dict[str, Dict[str, object]]:
    return {
        "1X2": {"picks": 0, "profit": 0.0, "clv": []},
        "OU_2.5": {"picks": 0, "profit": 0.0, "clv": []},
        "BTTS": {"picks": 0, "profit": 0.0, "clv": []},
    }


def _market_outcome(market: str, selection: str, hg: int, ag: int) -> bool:
    if market == "1X2":
        if selection == "HOME":
            return hg > ag
        if selection == "DRAW":
            return hg == ag
        if selection == "AWAY":
            return hg < ag
    if market == "OU_2.5":
        total = hg + ag
        if selection == "OVER":
            return total >= 3
        if selection == "UNDER":
            return total <= 2
    if market == "BTTS":
        btts = (hg > 0 and ag > 0)
        if selection == "YES":
            return btts
        if selection == "NO":
            return not btts
    return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", required=True, help="Comma separated season start years, es: 2021,2022")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--calibration", default="data/calibration/calibration_by_season.json")
    ap.add_argument("--dc-params", default="data/calibration/dc_params.json")
    ap.add_argument("--min-edge", type=float, default=None)
    ap.add_argument("--max-odds", type=float, default=6.0)
    ap.add_argument("--out", default="data/reports/kpi_report.json")
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    season_labels = {_season_label(s) for s in seasons}

    cal = load_calibration(args.calibration)
    cal = select_league_calibration(cal, args.league)
    rho = get_rho(args.dc_params, args.league)
    min_edge = float(args.min_edge) if args.min_edge is not None else float(get_market_rules().get("min_edge", 0.03))

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "league": args.league,
        "seasons": seasons,
        "features_version": args.features_version,
        "dc_rho": rho,
        "calibration": bool(cal),
        "by_season": {},
    }

    with get_conn() as conn:
        for season_label in sorted(season_labels):
            rows = conn.execute(
                """
                SELECT match_id, kickoff_utc, season
                FROM matches
                WHERE competition = ? AND season = ?
                """,
                (args.league, season_label),
            ).fetchall()

            brier_records = _init_brier_records()
            logloss = []
            logloss_by_market = {"1X2": [], "OU_2.5": [], "BTTS": []}
            picks_by_market = _init_picks_by_market()
            phase_records = {
                "early": {
                    "brier": _init_brier_records(),
                    "logloss": [],
                    "logloss_by_market": {"1X2": [], "OU_2.5": [], "BTTS": []},
                    "picks": _init_picks_by_market(),
                },
                "mid": {
                    "brier": _init_brier_records(),
                    "logloss": [],
                    "logloss_by_market": {"1X2": [], "OU_2.5": [], "BTTS": []},
                    "picks": _init_picks_by_market(),
                },
                "late": {
                    "brier": _init_brier_records(),
                    "logloss": [],
                    "logloss_by_market": {"1X2": [], "OU_2.5": [], "BTTS": []},
                    "picks": _init_picks_by_market(),
                },
            }

            for r in rows:
                match_id = r["match_id"]
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
                    kickoff = datetime.fromisoformat(str(r["kickoff_utc"]).replace("Z", "+00:00"))
                    cal_sel = select_calibration(cal, r["season"], kickoff)
                    if cal_sel:
                        probs = apply_calibration(probs, cal_sel)

                hg = int(us["home_goals"])
                ag = int(us["away_goals"])
                outcome = "H" if hg > ag else ("D" if hg == ag else "A")
                kickoff = datetime.fromisoformat(str(r["kickoff_utc"]).replace("Z", "+00:00"))
                phase = _phase_for_date(kickoff)
                phase_data = phase_records.get(phase)

                brier_records["home_win"].append((probs["home_win"], 1 if outcome == "H" else 0))
                brier_records["draw"].append((probs["draw"], 1 if outcome == "D" else 0))
                brier_records["away_win"].append((probs["away_win"], 1 if outcome == "A" else 0))
                brier_records["over_2_5"].append((probs["over_2_5"], 1 if (hg + ag) >= 3 else 0))
                brier_records["under_2_5"].append((probs["under_2_5"], 1 if (hg + ag) <= 2 else 0))
                brier_records["btts_yes"].append((probs["btts_yes"], 1 if (hg > 0 and ag > 0) else 0))
                brier_records["btts_no"].append((probs["btts_no"], 1 if (hg == 0 or ag == 0) else 0))
                if phase_data:
                    phase_data["brier"]["home_win"].append((probs["home_win"], 1 if outcome == "H" else 0))
                    phase_data["brier"]["draw"].append((probs["draw"], 1 if outcome == "D" else 0))
                    phase_data["brier"]["away_win"].append((probs["away_win"], 1 if outcome == "A" else 0))
                    phase_data["brier"]["over_2_5"].append((probs["over_2_5"], 1 if (hg + ag) >= 3 else 0))
                    phase_data["brier"]["under_2_5"].append((probs["under_2_5"], 1 if (hg + ag) <= 2 else 0))
                    phase_data["brier"]["btts_yes"].append((probs["btts_yes"], 1 if (hg > 0 and ag > 0) else 0))
                    phase_data["brier"]["btts_no"].append((probs["btts_no"], 1 if (hg == 0 or ag == 0) else 0))

                logloss.append(_logloss_1x2(probs["home_win"], probs["draw"], probs["away_win"], outcome))
                if phase_data:
                    phase_data["logloss"].append(_logloss_1x2(
                        probs["home_win"], probs["draw"], probs["away_win"], outcome
                    ))
                logloss_by_market["1X2"].append(_logloss_1x2(
                    probs["home_win"], probs["draw"], probs["away_win"], outcome
                ))
                if phase_data:
                    phase_data["logloss_by_market"]["1X2"].append(_logloss_1x2(
                        probs["home_win"], probs["draw"], probs["away_win"], outcome
                    ))

                total = hg + ag
                ou_outcome = total >= 3
                btts_outcome = (hg > 0 and ag > 0)
                logloss_by_market["OU_2.5"].append(_logloss_binary(probs["over_2_5"], ou_outcome))
                logloss_by_market["BTTS"].append(_logloss_binary(probs["btts_yes"], btts_outcome))
                if phase_data:
                    phase_data["logloss_by_market"]["OU_2.5"].append(
                        _logloss_binary(probs["over_2_5"], ou_outcome)
                    )
                    phase_data["logloss_by_market"]["BTTS"].append(
                        _logloss_binary(probs["btts_yes"], btts_outcome)
                    )

                # ROI per market (solo se ci sono quote)
                odds_rows = conn.execute(
                    """
                    SELECT market, selection, odds_decimal, retrieved_at_utc
                    FROM odds_quotes
                    WHERE match_id = ?
                      AND retrieved_at_utc <= ?
                    """,
                    (match_id, r["kickoff_utc"]),
                ).fetchall()
                if not odds_rows:
                    continue

                for market in ("1X2", "OU_2.5", "BTTS"):
                    candidates = [r for r in odds_rows if r["market"] == market]
                    if not candidates:
                        continue
                    best = None
                    for c in candidates:
                        odds_dec = float(c["odds_decimal"])
                        if odds_dec <= 1.01 or odds_dec > args.max_odds:
                            continue
                        selection = c["selection"]
                        key = None
                        if market == "1X2":
                            key = {"HOME": "home_win", "DRAW": "draw", "AWAY": "away_win"}.get(selection)
                        elif market == "OU_2.5":
                            key = {"OVER": "over_2_5", "UNDER": "under_2_5"}.get(selection)
                        elif market == "BTTS":
                            key = {"YES": "btts_yes", "NO": "btts_no"}.get(selection)
                        if not key:
                            continue
                        p = probs.get(key, 0.0)
                        edge = p * odds_dec - 1.0
                        if edge < min_edge:
                            continue
                        if not best or edge > best["edge"]:
                            best = {"selection": selection, "odds": odds_dec, "edge": edge}
                    if not best:
                        continue

                    picks_by_market[market]["picks"] += 1
                    win = _market_outcome(market, best["selection"], hg, ag)
                    picks_by_market[market]["profit"] += (best["odds"] - 1.0) if win else -1.0
                    if phase_data:
                        phase_picks = phase_data["picks"][market]
                        phase_picks["picks"] += 1
                        phase_picks["profit"] += (best["odds"] - 1.0) if win else -1.0

            season_report = {
                "brier": {k: _brier(v) for k, v in brier_records.items()},
                "brier_by_market": _brier_by_market({k: _brier(v) for k, v in brier_records.items()}),
                "logloss_1x2": sum(logloss) / len(logloss) if logloss else 0.0,
                "logloss_by_market": {
                    m: (sum(v) / len(v) if v else 0.0)
                    for m, v in logloss_by_market.items()
                },
                "roi_by_market": {
                    m: {
                        "picks": v["picks"],
                        "roi": (v["profit"] / v["picks"]) if v["picks"] else 0.0,
                    }
                    for m, v in picks_by_market.items()
                },
                "by_phase": {},
            }

            for phase, pdata in phase_records.items():
                season_report["by_phase"][phase] = {
                    "brier": {k: _brier(v) for k, v in pdata["brier"].items()},
                    "brier_by_market": _brier_by_market({k: _brier(v) for k, v in pdata["brier"].items()}),
                    "logloss_1x2": sum(pdata["logloss"]) / len(pdata["logloss"]) if pdata["logloss"] else 0.0,
                    "logloss_by_market": {
                        m: (sum(v) / len(v) if v else 0.0)
                        for m, v in pdata["logloss_by_market"].items()
                    },
                    "roi_by_market": {
                        m: {
                            "picks": v["picks"],
                            "roi": (v["profit"] / v["picks"]) if v["picks"] else 0.0,
                        }
                        for m, v in pdata["picks"].items()
                    },
                }

            report["by_season"][season_label] = season_report

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    existing: Dict[str, object] = {}
    if os.path.exists(args.out):
        try:
            with open(args.out, "r", encoding="utf-8") as f:
                existing = json.load(f) or {}
        except Exception:
            existing = {}

    by_league: Dict[str, object] = {}
    if isinstance(existing, dict) and "by_league" in existing:
        by_league = dict(existing.get("by_league") or {})
    elif isinstance(existing, dict) and existing.get("league") and existing.get("by_season"):
        by_league = {str(existing.get("league")): existing}

    by_league[str(args.league)] = report
    out_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "by_league": by_league,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out_payload, f, ensure_ascii=True, indent=2)

    print(f"OK: wrote KPI report to {args.out} league={args.league}")


if __name__ == "__main__":
    main()
