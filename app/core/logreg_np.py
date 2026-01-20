from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Literal, Optional, Sequence, Tuple

import numpy as np


def _softmax(z: np.ndarray) -> np.ndarray:
    z = z - np.max(z, axis=-1, keepdims=True)
    e = np.exp(z)
    return e / np.sum(e, axis=-1, keepdims=True)


def _sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-z))


def _safe_logit(p: np.ndarray, eps: float = 1e-6) -> np.ndarray:
    p = np.clip(p, eps, 1.0 - eps)
    return np.log(p / (1.0 - p))


@dataclass
class Standardizer:
    mean: np.ndarray
    std: np.ndarray

    def transform(self, X: np.ndarray) -> np.ndarray:
        std = np.where(self.std <= 1e-12, 1.0, self.std)
        return (X - self.mean) / std

    @staticmethod
    def fit(X: np.ndarray) -> "Standardizer":
        return Standardizer(mean=X.mean(axis=0), std=X.std(axis=0))


@dataclass
class MultinomialLogReg:
    classes: List[str]
    feature_names: List[str]
    W: np.ndarray  # (K, D)
    b: np.ndarray  # (K,)
    standardizer: Standardizer
    l2: float
    trained_at_utc: str

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        Xs = self.standardizer.transform(X)
        logits = Xs @ self.W.T + self.b
        return _softmax(logits)

    def to_dict(self) -> Dict:
        return {
            "model_type": "multinomial_logreg",
            "classes": self.classes,
            "feature_names": self.feature_names,
            "coef": self.W.tolist(),
            "intercept": self.b.tolist(),
            "mean": self.standardizer.mean.tolist(),
            "std": self.standardizer.std.tolist(),
            "l2": float(self.l2),
            "trained_at_utc": self.trained_at_utc,
        }

    @staticmethod
    def from_dict(d: Dict) -> "MultinomialLogReg":
        return MultinomialLogReg(
            classes=list(d["classes"]),
            feature_names=list(d["feature_names"]),
            W=np.array(d["coef"], dtype=float),
            b=np.array(d["intercept"], dtype=float),
            standardizer=Standardizer(mean=np.array(d["mean"], float), std=np.array(d["std"], float)),
            l2=float(d.get("l2", 1e-3)),
            trained_at_utc=str(d.get("trained_at_utc", "")),
        )


@dataclass
class BinaryLogReg:
    feature_names: List[str]
    w: np.ndarray  # (D,)
    b: float
    standardizer: Standardizer
    l2: float
    trained_at_utc: str

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        Xs = self.standardizer.transform(X)
        z = Xs @ self.w + self.b
        return _sigmoid(z)

    def to_dict(self) -> Dict:
        return {
            "model_type": "binary_logreg",
            "feature_names": self.feature_names,
            "coef": self.w.tolist(),
            "intercept": float(self.b),
            "mean": self.standardizer.mean.tolist(),
            "std": self.standardizer.std.tolist(),
            "l2": float(self.l2),
            "trained_at_utc": self.trained_at_utc,
        }

    @staticmethod
    def from_dict(d: Dict) -> "BinaryLogReg":
        return BinaryLogReg(
            feature_names=list(d["feature_names"]),
            w=np.array(d["coef"], dtype=float),
            b=float(d["intercept"]),
            standardizer=Standardizer(mean=np.array(d["mean"], float), std=np.array(d["std"], float)),
            l2=float(d.get("l2", 1e-3)),
            trained_at_utc=str(d.get("trained_at_utc", "")),
        )


def fit_multinomial_logreg(
    X: np.ndarray,
    y: np.ndarray,
    classes: Sequence[str],
    feature_names: Sequence[str],
    lr: float = 0.05,
    epochs: int = 800,
    batch_size: int = 512,
    l2: float = 1e-3,
    seed: int = 7,
) -> MultinomialLogReg:
    """
    Multinomial logistic regression (softmax) con L2 e standardizzazione.
    y: int in [0..K-1]
    """
    rng = np.random.default_rng(seed)
    K = len(classes)
    N, D = X.shape

    stdz = Standardizer.fit(X)
    Xs = stdz.transform(X)

    W = rng.normal(0, 0.05, size=(K, D))
    b = np.zeros(K, dtype=float)

    # one-hot
    Y = np.zeros((N, K), dtype=float)
    Y[np.arange(N), y] = 1.0

    for _ in range(epochs):
        idx = rng.permutation(N)
        for start in range(0, N, batch_size):
            batch = idx[start : start + batch_size]
            Xb = Xs[batch]
            Yb = Y[batch]

            logits = Xb @ W.T + b
            P = _softmax(logits)

            # gradients (cross-entropy)
            G = (P - Yb) / len(batch)  # (B,K)
            dW = G.T @ Xb + l2 * W
            db = G.sum(axis=0)

            W -= lr * dW
            b -= lr * db

    return MultinomialLogReg(
        classes=list(classes),
        feature_names=list(feature_names),
        W=W,
        b=b,
        standardizer=stdz,
        l2=l2,
        trained_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )


def fit_binary_logreg(
    X: np.ndarray,
    y: np.ndarray,
    feature_names: Sequence[str],
    lr: float = 0.05,
    epochs: int = 600,
    batch_size: int = 512,
    l2: float = 1e-3,
    seed: int = 7,
) -> BinaryLogReg:
    rng = np.random.default_rng(seed)
    N, D = X.shape

    stdz = Standardizer.fit(X)
    Xs = stdz.transform(X)

    w = rng.normal(0, 0.05, size=(D,))
    b = 0.0

    y = y.astype(float)

    for _ in range(epochs):
        idx = rng.permutation(N)
        for start in range(0, N, batch_size):
            batch = idx[start : start + batch_size]
            Xb = Xs[batch]
            yb = y[batch]

            z = Xb @ w + b
            p = _sigmoid(z)

            # gradients
            g = (p - yb) / len(batch)
            dw = Xb.T @ g + l2 * w
            db = g.sum()

            w -= lr * dw
            b -= lr * db

    return BinaryLogReg(
        feature_names=list(feature_names),
        w=w,
        b=float(b),
        standardizer=stdz,
        l2=l2,
        trained_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )
