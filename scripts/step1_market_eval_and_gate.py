from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn
from app.core.market_models import implied_probs_from_odds_1x2, load_multinomial
from app.core.dc_params import get_rho
from app.core.probabilities import match_probs
from app.core.config import settings

CLASSES = ["HOME", "DRAW", "AWAY"]
KEYS_1X2 = ["HOME", "DRAW", "AWAY"]

def _logloss(P: np.ndarray, y: np.ndarray, eps: float=1e-12) -> float:
    P = np.clip(P, eps, 1.0)
    return float(-np.mean(np.log(P[np.arange(len(y)), y])))

def _brier(P: np.ndarray, y: np.ndarray) -> float:
    Y = np.zeros_like(P)
    Y[np.arange(len(y)), y] = 1.0
    return float(np.mean(np.sum((P - Y) ** 2, axis=1)))

def _parse_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z","+00:00"))

def _get_odds_snapshots(conn, match_id: str, kickoff_utc: str):
    rows = conn.execute(
        """
        SELECT selection, odds_decimal, retrieved_at_utc
        FROM odds_quotes
        WHERE match_id = ?
          AND market='1X2'
          AND retrieved_at_utc <= ?
        ORDER BY retrieved_at_utc ASC
        """,
        (match_id, kickoff_utc),
    ).fetchall()
    return rows

def _odds_from_snapshot(rows, which: str) -> Optional[Dict[str, float]]:
    # rows already sorted ASC
    if not rows:
        return None
    if which == "earliest":
        chosen = {}
        for sel, odd, _ in rows:
            if sel not in chosen:
                chosen[sel] = float(odd)
        if all(k in chosen for k in KEYS_1X2):
            return chosen
        return None
    if which == "closing":
        # take last available per selection
        chosen = {}
        for sel, odd, _ in rows[::-1]:
            if sel not in chosen:
                chosen[sel] = float(odd)
        if all(k in chosen for k in KEYS_1X2):
            return chosen
        return None
    raise ValueError(which)

def _market_probs(odds: Dict[str, float]) -> np.ndarray:
    mp = implied_probs_from_odds_1x2(odds["HOME"], odds["DRAW"], odds["AWAY"])
    return np.array([mp["home_win"], mp["draw"], mp["away_win"]], dtype=float)

def _dc_probs(comp: str, feats: Dict[str, Any]) -> np.ndarray:
    lam_h = float(feats.get("lambda_home", 0.0) or 0.0)
    lam_a = float(feats.get("lambda_away", 0.0) or 0.0)
    if lam_h <= 0 or lam_a <= 0:
        return np.array([1/3,1/3,1/3], dtype=float)
    rho = get_rho(comp, None)
    p = match_probs(lam_h, lam_a, rho=rho, cap=8)
    return np.array([p["home_win"], p["draw"], p["away_win"]], dtype=float)

def _residual_probs(residual, comp: str, feats: Dict[str, Any], mvec: np.ndarray) -> np.ndarray:
    Xin=[]
    for fn in residual.feature_names:
        if fn == "logit_mkt_home":
            Xin.append(math.log(mvec[0]/max(1e-6,1-mvec[0])))
        elif fn == "logit_mkt_draw":
            Xin.append(math.log(mvec[1]/max(1e-6,1-mvec[1])))
        elif fn == "logit_mkt_away":
            Xin.append(math.log(mvec[2]/max(1e-6,1-mvec[2])))
        elif fn.startswith("comp_"):
            Xin.append(1.0 if fn == f"comp_{comp}" else 0.0)
        else:
            Xin.append(float(feats.get(fn, 0.0) or 0.0))
    pr = residual.predict_proba(np.array([Xin], dtype=float))[0]
    return np.array([pr[0],pr[1],pr[2]], dtype=float)

def _stack_probs(stack, feats: Dict[str, Any], mvec: np.ndarray, dvec: np.ndarray, rvec: np.ndarray) -> np.ndarray:
    # feature order must match stack.feature_names
    def ent(v):
        v = np.clip(v,1e-12,1.0)
        return float(-np.sum(v*np.log(v)))
    maxdiff_mkt_dc = float(np.max(np.abs(mvec-dvec)))
    maxdiff_mkt_res = float(np.max(np.abs(mvec-rvec)))
    fmap = {
        "market_home_win": mvec[0], "market_draw": mvec[1], "market_away_win": mvec[2],
        "dc_home_win": dvec[0], "dc_draw": dvec[1], "dc_away_win": dvec[2],
        "res_home_win": rvec[0], "res_draw": rvec[1], "res_away_win": rvec[2],
        "ent_market": ent(mvec), "ent_dc": ent(dvec), "ent_res": ent(rvec),
        "maxdiff_mkt_dc": maxdiff_mkt_dc, "maxdiff_mkt_res": maxdiff_mkt_res,
    }
    Xs = np.array([[float(fmap.get(fn, feats.get(fn, 0.0) or 0.0)) for fn in stack.feature_names]], dtype=float)
    ps = stack.predict_proba(Xs)[0]
    return np.array([ps[0],ps[1],ps[2]], dtype=float)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--residual", default=settings.residual_model_path)
    ap.add_argument("--stack", default=settings.stack_model_path)
    ap.add_argument("--gate-out", default=settings.betting_gate_path)
    ap.add_argument("--report-out", default=os.path.join(ROOT,"data/reports/step1_market_eval_report.json"))
    ap.add_argument("--walkforward-cutoff", default=None, help="ISO datetime UTC: tutto dopo è test. default: ultimi 20% per data.")
    ap.add_argument("--min-edge", type=float, default=0.02, help="Soglia (p_model - p_market) per considerare una pick EV.")
    args=ap.parse_args()

    residual = load_multinomial(args.residual)
    stack = load_multinomial(args.stack)

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT m.match_id, m.competition, m.kickoff_utc,
                   um.home_goals, um.away_goals,
                   mf.features_json
            FROM matches m
            JOIN understat_matches um
              ON um.understat_match_id = substr(m.match_id, instr(m.match_id, ':')+1)
            JOIN match_features mf
              ON mf.match_id = m.match_id AND mf.features_version = ?
            WHERE um.home_goals IS NOT NULL AND um.away_goals IS NOT NULL
            """,
            (args.features_version,),
        ).fetchall()

        data=[]
        for match_id, comp, kickoff_utc, hg, ag, feats_json in rows:
            snaps=_get_odds_snapshots(conn, match_id, kickoff_utc)
            if not snaps:
                continue
            odds_early=_odds_from_snapshot(snaps, "earliest")
            odds_close=_odds_from_snapshot(snaps, "closing")
            if not (odds_early and odds_close):
                continue

            mvec=_market_probs(odds_close)  # use closing for baseline scoring
            feats=json.loads(feats_json)
            dvec=_dc_probs(comp, feats)

            rvec=None
            if residual:
                rvec=_residual_probs(residual, comp, feats, mvec)
            else:
                rvec=mvec.copy()

            if stack:
                pvec=_stack_probs(stack, feats, mvec, dvec, rvec)
                model_name="stack"
            elif residual:
                pvec=rvec
                model_name="residual"
            else:
                pvec=dvec
                model_name="dc"

            if hg > ag: y=0
            elif hg==ag: y=1
            else: y=2

            data.append({
                "match_id": match_id,
                "competition": comp,
                "kickoff_utc": kickoff_utc,
                "y": y,
                "market": mvec,
                "model": pvec,
                "odds_early": odds_early,
                "odds_close": odds_close,
            })

    if not data:
        raise SystemExit("Nessun dato utile (risultati+quote pre-kickoff)")

    # sort by kickoff
    data.sort(key=lambda r: r["kickoff_utc"])
    N=len(data)
    if args.walkforward_cutoff:
        cutoff=args.walkforward_cutoff
        split = next((i for i,r in enumerate(data) if r["kickoff_utc"] >= cutoff), int(N*0.8))
    else:
        split=int(N*0.8)

    train=data[:split]
    test=data[split:]

    def to_arrays(rows):
        y=np.array([r["y"] for r in rows], int)
        Pm=np.stack([r["market"] for r in rows], axis=0)
        P=np.stack([r["model"] for r in rows], axis=0)
        return y,Pm,P

    ytr,Pmtr,Ptr = to_arrays(train)
    yte,Pmte,Pte = to_arrays(test)

    metrics = {
        "train": {
            "n": int(len(train)),
            "logloss_market": _logloss(Pmtr, ytr),
            "logloss_model": _logloss(Ptr, ytr),
            "brier_market": _brier(Pmtr, ytr),
            "brier_model": _brier(Ptr, ytr),
        },
        "test": {
            "n": int(len(test)),
            "logloss_market": _logloss(Pmte, yte),
            "logloss_model": _logloss(Pte, yte),
            "brier_market": _brier(Pmte, yte),
            "brier_model": _brier(Pte, yte),
            "logloss_improvement_vs_market": float(_logloss(Pmte, yte) - _logloss(Pte, yte)),
        },
        "model_used": model_name,
        "split": {"train_n": int(len(train)), "test_n": int(len(test)), "cutoff_kickoff_utc": train[-1]["kickoff_utc"] if train else None},
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
    }

    # CLV simulation: pick best EV using early odds (as if we bet early) and compare to closing odds
    picks=[]
    for r in test:
        odds_e=r["odds_early"]
        odds_c=r["odds_close"]
        m_close=r["market"]
        p=r["model"]

        # evaluate edge vs market probs (closing baseline) - conservative
        edges = p - m_close
        best = int(np.argmax(edges))
        if edges[best] < args.min_edge:
            continue

        sel = KEYS_1X2[best]
        o_early=float(odds_e[sel])
        o_close=float(odds_c[sel])
        clv = (o_close - o_early) / o_early

        # realized profit with early odds, 1 unit
        y=r["y"]
        win = (y==best)
        profit = (o_early - 1.0) if win else -1.0

        picks.append({"match_id": r["match_id"], "sel": sel, "edge": float(edges[best]), "clv": float(clv), "profit": float(profit)})

    if picks:
        clv_avg=float(np.mean([p["clv"] for p in picks]))
        roi=float(np.mean([p["profit"] for p in picks]))
    else:
        clv_avg=0.0
        roi=0.0

    metrics["test"]["picks_n"]=int(len(picks))
    metrics["test"]["clv_avg"]=clv_avg
    metrics["test"]["roi_per_bet"]=roi

    # Gate decision
    thresholds = {
        "min_logloss_improvement_vs_market": 0.005,
        "min_clv": 0.0,
        "min_picks": 50,
    }
    enabled = (metrics["test"]["logloss_improvement_vs_market"] >= thresholds["min_logloss_improvement_vs_market"]
               and clv_avg >= thresholds["min_clv"]
               and len(picks) >= thresholds["min_picks"])

    gate = {
        "enabled": bool(enabled),
        "reason": "OK" if enabled else "NOT_ENOUGH_EDGE_OR_CV",
        "updated_at_utc": metrics["generated_at_utc"],
        "thresholds": thresholds,
        "evidence": {
            "model_used": model_name,
            "test_logloss_improvement_vs_market": metrics["test"]["logloss_improvement_vs_market"],
            "test_clv_avg": clv_avg,
            "test_picks_n": int(len(picks)),
            "test_roi_per_bet": roi,
            "split_cutoff_kickoff_utc": metrics["split"]["cutoff_kickoff_utc"],
        },
    }

    os.makedirs(os.path.dirname(args.report_out), exist_ok=True)
    with open(args.report_out,"w",encoding="utf-8") as f:
        json.dump({"metrics": metrics, "picks_sample": picks[:200]}, f, indent=2)

    os.makedirs(os.path.dirname(args.gate_out), exist_ok=True)
    with open(args.gate_out,"w",encoding="utf-8") as f:
        json.dump(gate, f, indent=2)

    print(json.dumps(metrics, indent=2))
    print("\nGate:", json.dumps(gate, indent=2))

if __name__=="__main__":
    main()
