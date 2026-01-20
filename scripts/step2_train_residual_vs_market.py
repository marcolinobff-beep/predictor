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
from app.core.market_models import implied_probs_from_odds_1x2

CLASSES = ["HOME", "DRAW", "AWAY"]

# feature set "minimo ma sensato" per residuo vs mercato
BASE_FEATURES = [
    "elo_home", "elo_away", "elo_diff",
    "attack_delta_form_home", "attack_delta_form_away",
    "defense_delta_form_home", "defense_delta_form_away",
    "home_xg_for_form", "home_xg_against_form",
    "away_xg_for_form", "away_xg_against_form",
    "overall_xg_for_form_home", "overall_xg_for_form_away",
    "rest_days_home", "rest_days_away",
    "schedule_factor_home", "schedule_factor_away",
    "xg_for_form_std_home", "xg_for_form_std_away",
    "xg_against_form_std_home", "xg_against_form_std_away",
]

def _logit(p: float) -> float:
    p = min(0.999999, max(1e-6, float(p)))
    return math.log(p/(1.0-p))

def _logloss_multiclass(P: np.ndarray, y: np.ndarray, eps: float=1e-12) -> float:
    P = np.clip(P, eps, 1.0)
    return float(-np.mean(np.log(P[np.arange(len(y)), y])))

def build_dataset(features_version: str, cutoff_pre_kickoff: bool = True) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    """
    X: [logit(market_home), logit(market_draw), logit(market_away),
        BASE_FEATURES..., comp one-hot...]
    y: 0/1/2 (HOME/DRAW/AWAY)
    """
    with get_conn() as conn:
        conn.row_factory = None
        # join matches+understat results+features
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

        # load odds latest pre-kickoff per match
        # we'll query per match (DB non enorme; va bene)
        X = []
        y = []
        comps = set()

        for match_id, comp, kickoff_utc, hg, ag, feats_json in rows:
            comps.add(comp)
            kickoff = kickoff_utc
            if cutoff_pre_kickoff:
                odds_rows = conn.execute(
                    """
                    SELECT selection, odds_decimal
                    FROM odds_quotes
                    WHERE match_id = ?
                      AND market = '1X2'
                      AND retrieved_at_utc <= ?
                    ORDER BY retrieved_at_utc DESC
                    """,
                    (match_id, kickoff),
                ).fetchall()
            else:
                odds_rows = conn.execute(
                    """
                    SELECT selection, odds_decimal
                    FROM odds_quotes
                    WHERE match_id = ? AND market = '1X2'
                    ORDER BY retrieved_at_utc DESC
                    """,
                    (match_id,),
                ).fetchall()

            oh=od=oa=None
            for sel, odd in odds_rows:
                if sel == "HOME" and oh is None: oh = odd
                if sel == "DRAW" and od is None: od = odd
                if sel == "AWAY" and oa is None: oa = odd
                if oh and od and oa:
                    break
            if not (oh and od and oa):
                continue
            if min(oh,od,oa) <= 1.01:
                continue

            mp = implied_probs_from_odds_1x2(float(oh), float(od), float(oa))

            feats = json.loads(feats_json)
            rowx = [_logit(mp["home_win"]), _logit(mp["draw"]), _logit(mp["away_win"])]
            for k in BASE_FEATURES:
                rowx.append(float(feats.get(k, 0.0) or 0.0))

            # y
            if hg > ag: yy = 0
            elif hg == ag: yy = 1
            else: yy = 2

            X.append(rowx + [comp])  # temp store comp for one-hot
            y.append(yy)

        comp_list = sorted(comps)
        feature_names = ["logit_mkt_home", "logit_mkt_draw", "logit_mkt_away"] + BASE_FEATURES + [f"comp_{c}" for c in comp_list]

        # finalize one-hot
        X_final = []
        for row in X:
            comp = row[-1]
            base = row[:-1]
            oh = [1.0 if comp == c else 0.0 for c in comp_list]
            X_final.append(base + oh)

        return np.array(X_final, dtype=float), np.array(y, dtype=int), feature_names

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--out", default=os.path.join(ROOT, "data/models/residual_vs_market_1x2.json"))
    ap.add_argument("--test-frac", type=float, default=0.2)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    X, y, feature_names = build_dataset(args.features_version, cutoff_pre_kickoff=True)
    n = len(y)
    if n < 500:
        raise SystemExit(f"Pochi dati utili: {n}")

    # time split: usa l'ordine match_id? meglio sort kickoff: ricostruisco kickoff per split
    # semplice: random split ma con seed fisso (MVP). Per walk-forward usa step1 script.
    rng = np.random.default_rng(args.seed)
    idx = rng.permutation(n)
    n_test = int(n * args.test_frac)
    test_idx = idx[:n_test]
    train_idx = idx[n_test:]

    Xtr, ytr = X[train_idx], y[train_idx]
    Xte, yte = X[test_idx], y[test_idx]

    model = fit_multinomial_logreg(
        Xtr, ytr, classes=CLASSES, feature_names=feature_names,
        lr=0.05, epochs=900, batch_size=512, l2=5e-3, seed=args.seed
    )

    Ptr = model.predict_proba(Xtr)
    Pte = model.predict_proba(Xte)

    # baseline market probs from first 3 features (logits -> probs)
    # recover probs: sigmoid each logit then renormalize? no: logits were separate; reconstruct from original? use softmax of logits as approximation
    # better: compute from logits via inverse logit then normalize.
    def logits_to_probs(X):
        ph = 1/(1+np.exp(-X[:,0]))
        pd = 1/(1+np.exp(-X[:,1]))
        pa = 1/(1+np.exp(-X[:,2]))
        s = ph+pd+pa
        return np.stack([ph/s, pd/s, pa/s], axis=1)

    Mte = logits_to_probs(Xte)

    report = {
        "n": int(n),
        "train": {
            "n": int(len(ytr)),
            "logloss_model": _logloss_multiclass(Ptr, ytr),
        },
        "test": {
            "n": int(len(yte)),
            "logloss_model": _logloss_multiclass(Pte, yte),
            "logloss_market_baseline": _logloss_multiclass(Mte, yte),
            "logloss_improvement_vs_market": float(_logloss_multiclass(Mte, yte) - _logloss_multiclass(Pte, yte)),
        },
        "trained_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "features_version": args.features_version,
        "notes": [
            "Questo modello usa le quote (1X2) come baseline e impara un piccolo aggiustamento usando feature Understat v5 (xG form, Elo, rest days, schedule).",
            "Split random (MVP). Per walk-forward usa step1 che genera report/gate."
        ],
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(model.to_dict(), f, indent=2)

    rep_path = os.path.join(ROOT, "data/reports/step2_residual_report.json")
    os.makedirs(os.path.dirname(rep_path), exist_ok=True)
    with open(rep_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
