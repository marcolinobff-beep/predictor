from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn
from app.core.logreg_np import fit_multinomial_logreg
from app.core.market_models import implied_probs_from_odds_1x2, load_multinomial
from app.core.dc_params import get_rho
from app.core.probabilities import match_probs

CLASSES = ["HOME", "DRAW", "AWAY"]

def _logloss(P: np.ndarray, y: np.ndarray, eps: float=1e-12) -> float:
    P = np.clip(P, eps, 1.0)
    return float(-np.mean(np.log(P[np.arange(len(y)), y])))

def _entropy(P: np.ndarray, eps: float=1e-12) -> np.ndarray:
    P = np.clip(P, eps, 1.0)
    return -np.sum(P*np.log(P), axis=1)

def _get_latest_prekickoff_odds(conn, match_id: str, kickoff_utc: str):
    rows = conn.execute(
        """
        SELECT selection, odds_decimal
        FROM odds_quotes
        WHERE match_id = ?
          AND market='1X2'
          AND retrieved_at_utc <= ?
        ORDER BY retrieved_at_utc DESC
        """,
        (match_id, kickoff_utc),
    ).fetchall()
    oh=od=oa=None
    for sel, odd in rows:
        if sel == "HOME" and oh is None: oh = odd
        if sel == "DRAW" and od is None: od = odd
        if sel == "AWAY" and oa is None: oa = odd
        if oh and od and oa:
            break
    if not (oh and od and oa):
        return None
    if min(oh,od,oa) <= 1.01:
        return None
    return float(oh), float(od), float(oa)

def build_dataset(features_version: str, residual_path: str) -> Tuple[np.ndarray, np.ndarray, List[str], Dict[str, float]]:
    residual = load_multinomial(residual_path)
    if not residual:
        raise SystemExit(f"Residual model non trovato: {residual_path}")

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
            (features_version,),
        ).fetchall()

        X=[]
        y=[]
        marketP=[]
        dcP=[]
        resP=[]

        feature_names = [
            "market_home_win","market_draw","market_away_win",
            "dc_home_win","dc_draw","dc_away_win",
            "res_home_win","res_draw","res_away_win",
            "ent_market","ent_dc","ent_res",
            "maxdiff_mkt_dc","maxdiff_mkt_res",
        ]

        for match_id, comp, kickoff_utc, hg, ag, feats_json in rows:
            odds = _get_latest_prekickoff_odds(conn, match_id, kickoff_utc)
            if not odds:
                continue
            oh,od,oa = odds
            mp = implied_probs_from_odds_1x2(oh,od,oa)
            mvec = np.array([mp["home_win"], mp["draw"], mp["away_win"]], dtype=float)

            feats = json.loads(feats_json)
            lam_h = float((feats.get("lambda_home", 0.0) or 0.0))
            lam_a = float((feats.get("lambda_away", 0.0) or 0.0))
            # se understat_v5 non contiene lambda, non possiamo calcolare DC -> salta
            if lam_h <= 0 or lam_a <= 0:
                continue
            rho = get_rho(comp, None)
            probs_dc = match_probs(lam_h, lam_a, rho=rho, cap=8)
            dvec = np.array([probs_dc["home_win"], probs_dc["draw"], probs_dc["away_win"]], dtype=float)

            # residual inference: costruiamo X con feature list del residual model
            # NOTE: nel residual model feature_names include logit_mkt_* e varie
            # Qui, ricostruiamo input usando quelle convenzioni.
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
            rvec = np.array([pr[0],pr[1],pr[2]], dtype=float)

            # target
            if hg > ag: yy=0
            elif hg == ag: yy=1
            else: yy=2

            ent_m = float(-np.sum(np.clip(mvec,1e-12,1.0)*np.log(np.clip(mvec,1e-12,1.0))))
            ent_d = float(-np.sum(np.clip(dvec,1e-12,1.0)*np.log(np.clip(dvec,1e-12,1.0))))
            ent_r = float(-np.sum(np.clip(rvec,1e-12,1.0)*np.log(np.clip(rvec,1e-12,1.0))))
            maxdiff_mkt_dc = float(np.max(np.abs(mvec-dvec)))
            maxdiff_mkt_res = float(np.max(np.abs(mvec-rvec)))

            X.append([
                mvec[0],mvec[1],mvec[2],
                dvec[0],dvec[1],dvec[2],
                rvec[0],rvec[1],rvec[2],
                ent_m, ent_d, ent_r,
                maxdiff_mkt_dc, maxdiff_mkt_res,
            ])
            y.append(yy)
            marketP.append(mvec)
            dcP.append(dvec)
            resP.append(rvec)

        X=np.array(X, float)
        y=np.array(y, int)
        marketP=np.array(marketP, float)
        dcP=np.array(dcP, float)
        resP=np.array(resP, float)

        baselines={
            "logloss_market": _logloss(marketP, y),
            "logloss_dc": _logloss(dcP, y),
            "logloss_residual": _logloss(resP, y),
        }
        return X,y,feature_names, baselines

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--residual", default=os.path.join(ROOT, "data/models/residual_vs_market_1x2.json"))
    ap.add_argument("--out", default=os.path.join(ROOT, "data/models/stack_1x2.json"))
    ap.add_argument("--test-frac", type=float, default=0.2)
    ap.add_argument("--seed", type=int, default=11)
    args=ap.parse_args()

    X,y,feature_names, baselines = build_dataset(args.features_version, args.residual)
    n=len(y)
    if n < 500:
        raise SystemExit(f"Pochi dati utili: {n}")

    rng=np.random.default_rng(args.seed)
    idx=rng.permutation(n)
    n_test=int(n*args.test_frac)
    te=idx[:n_test]
    tr=idx[n_test:]

    model=fit_multinomial_logreg(
        X[tr], y[tr], classes=CLASSES, feature_names=feature_names,
        lr=0.08, epochs=900, batch_size=512, l2=1e-2, seed=args.seed
    )

    Ptr=model.predict_proba(X[tr])
    Pte=model.predict_proba(X[te])

    report={
        "n": int(n),
        "baselines_full": baselines,
        "train": {"n": int(len(tr)), "logloss": _logloss(Ptr, y[tr])},
        "test": {
            "n": int(len(te)),
            "logloss": _logloss(Pte, y[te]),
        },
        "test_improvement_vs_market": float(baselines["logloss_market"] - _logloss(Pte, y[te])),
        "trained_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
        "features_version": args.features_version,
        "notes": [
            "Stacking multinomiale: combina market+DC+residual e poche meta-feature (entropie/differenze).",
            "Split random (MVP). Per walk-forward usa step1."
        ],
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out,"w",encoding="utf-8") as f:
        json.dump(model.to_dict(), f, indent=2)

    rep_path=os.path.join(ROOT,"data/reports/step3_stack_report.json")
    os.makedirs(os.path.dirname(rep_path), exist_ok=True)
    with open(rep_path,"w",encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report, indent=2))

if __name__=="__main__":
    main()
