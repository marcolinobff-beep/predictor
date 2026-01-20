from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _logit(p: float) -> float:
    p = min(0.999999, max(1e-6, p))
    return math.log(p / (1.0 - p))


def _softmax(scores: List[float]) -> List[float]:
    m = max(scores)
    exps = [math.exp(s - m) for s in scores]
    total = sum(exps) or 1.0
    return [e / total for e in exps]


def _candidate_thresholds(values: List[float], max_bins: int) -> List[float]:
    uniq = sorted(set(values))
    if len(uniq) <= 1:
        return []
    if len(uniq) <= max_bins:
        return [(uniq[i] + uniq[i + 1]) / 2.0 for i in range(len(uniq) - 1)]
    step = max(1, len(uniq) // max_bins)
    thresholds = []
    for i in range(step, len(uniq) - step, step):
        thresholds.append((uniq[i] + uniq[i + 1]) / 2.0)
    return thresholds[:max_bins]


def _matrix_from_dicts(rows: List[Dict[str, float]], feature_names: List[str]) -> List[List[float]]:
    matrix = []
    for r in rows:
        matrix.append([float(r.get(f, 0.0) or 0.0) for f in feature_names])
    return matrix


def train_binary(
    rows: List[Dict[str, float]],
    y: List[int],
    feature_names: List[str],
    n_estimators: int = 60,
    learning_rate: float = 0.1,
    max_bins: int = 8,
    min_leaf: int = 20,
) -> Dict[str, object]:
    if not rows:
        raise ValueError("No training rows provided.")
    if len(rows) != len(y):
        raise ValueError("X/y length mismatch.")

    X = _matrix_from_dicts(rows, feature_names)
    n = len(X)
    pos_rate = sum(y) / float(n)
    init = _logit(pos_rate)
    scores = [init] * n
    stumps = []

    for _ in range(n_estimators):
        probs = [_sigmoid(s) for s in scores]
        residuals = [yi - pi for yi, pi in zip(y, probs)]

        best = None
        for j, _fname in enumerate(feature_names):
            values = [row[j] for row in X]
            thresholds = _candidate_thresholds(values, max_bins)
            for thr in thresholds:
                left_idx = [i for i, v in enumerate(values) if v <= thr]
                right_idx = [i for i, v in enumerate(values) if v > thr]
                if len(left_idx) < min_leaf or len(right_idx) < min_leaf:
                    continue
                left_val = sum(residuals[i] for i in left_idx) / len(left_idx)
                right_val = sum(residuals[i] for i in right_idx) / len(right_idx)
                mse = 0.0
                for i in left_idx:
                    diff = residuals[i] - left_val
                    mse += diff * diff
                for i in right_idx:
                    diff = residuals[i] - right_val
                    mse += diff * diff
                if best is None or mse < best["mse"]:
                    best = {
                        "feature": feature_names[j],
                        "feature_index": j,
                        "threshold": float(thr),
                        "left_value": float(left_val),
                        "right_value": float(right_val),
                        "mse": float(mse),
                    }
        if not best:
            break

        idx = int(best.get("feature_index", 0))
        for i, row in enumerate(X):
            val = row[idx]
            scores[i] += learning_rate * (best["left_value"] if val <= best["threshold"] else best["right_value"])

        best.pop("mse", None)
        best.pop("feature_index", None)
        stumps.append(best)

    return {
        "init": float(init),
        "learning_rate": float(learning_rate),
        "n_estimators": int(len(stumps)),
        "stumps": stumps,
    }


def predict_binary(model: Dict[str, object], features: Dict[str, float]) -> float:
    if not model:
        return 0.5
    score = float(model.get("init", 0.0))
    lr = float(model.get("learning_rate", 0.1))
    stumps = model.get("stumps") or []
    for s in stumps:
        feat = s["feature"]
        thr = float(s["threshold"])
        left = float(s["left_value"])
        right = float(s["right_value"])
        val = float(features.get(feat, 0.0) or 0.0)
        score += lr * (left if val <= thr else right)
    return _sigmoid(score)


def predict_multiclass(models: Dict[str, object], features: Dict[str, float]) -> Dict[str, float]:
    scores = []
    classes = ["home", "draw", "away"]
    for cls in classes:
        model = models.get(cls) if models else None
        score = float(model.get("init", 0.0)) if model else 0.0
        lr = float(model.get("learning_rate", 0.1)) if model else 0.1
        stumps = (model.get("stumps") if model else []) or []
        for s in stumps:
            val = float(features.get(s["feature"], 0.0) or 0.0)
            score += lr * (float(s["left_value"]) if val <= float(s["threshold"]) else float(s["right_value"]))
        scores.append(score)
    probs = _softmax(scores)
    return {
        "home_win": probs[0],
        "draw": probs[1],
        "away_win": probs[2],
    }


def load_model(path: str, league: Optional[str] = None) -> Optional[Dict[str, object]]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not data:
        return None
    if league and isinstance(data, dict) and "by_league" in data:
        return (data.get("by_league") or {}).get(league)
    if not league and isinstance(data, dict) and "by_league" in data:
        return None
    return data


def predict_probs(model: Dict[str, object], features: Dict[str, float]) -> Dict[str, float]:
    if not model:
        return {}
    models = model.get("models") or {}
    out = {}

    one_x2 = models.get("1x2")
    if one_x2:
        out.update(predict_multiclass(one_x2, features))

    ou = models.get("ou_2_5")
    if ou:
        p_over = predict_binary(ou, features)
        out["over_2_5"] = p_over
        out["under_2_5"] = 1.0 - p_over

    btts = models.get("btts")
    if btts:
        p_yes = predict_binary(btts, features)
        out["btts_yes"] = p_yes
        out["btts_no"] = 1.0 - p_yes

    return out
