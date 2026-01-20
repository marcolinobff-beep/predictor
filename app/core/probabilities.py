from __future__ import annotations

from typing import Dict


def poisson_pmf(lam: float, k: int) -> float:
    if k < 0:
        return 0.0
    num = 1.0
    for i in range(1, k + 1):
        num *= lam / i
    return num * (2.718281828459045 ** (-lam))


def dixon_coles_tau(i: int, j: int, lam_h: float, lam_a: float, rho: float) -> float:
    if rho == 0.0:
        return 1.0
    if i == 0 and j == 0:
        return 1.0 - (lam_h * lam_a * rho)
    if i == 0 and j == 1:
        return 1.0 + (lam_h * rho)
    if i == 1 and j == 0:
        return 1.0 + (lam_a * rho)
    if i == 1 and j == 1:
        return 1.0 - rho
    return 1.0


def match_probs(lam_h: float, lam_a: float, cap: int = 8, rho: float = 0.0) -> Dict[str, float]:
    p_h = [poisson_pmf(lam_h, k) for k in range(cap + 1)]
    p_a = [poisson_pmf(lam_a, k) for k in range(cap + 1)]

    p_home = p_draw = p_away = 0.0
    p_over = p_btts_yes = 0.0
    total = 0.0

    for i, ph in enumerate(p_h):
        for j, pa in enumerate(p_a):
            tau = dixon_coles_tau(i, j, lam_h, lam_a, rho)
            p = ph * pa * max(0.0, tau)
            total += p
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

    if total <= 0:
        return {
            "home_win": 0.0,
            "draw": 0.0,
            "away_win": 0.0,
            "over_2_5": 0.0,
            "under_2_5": 0.0,
            "btts_yes": 0.0,
            "btts_no": 0.0,
        }

    p_home /= total
    p_draw /= total
    p_away /= total
    p_over /= total
    p_btts_yes /= total

    return {
        "home_win": p_home,
        "draw": p_draw,
        "away_win": p_away,
        "over_2_5": p_over,
        "under_2_5": 1.0 - p_over,
        "btts_yes": p_btts_yes,
        "btts_no": 1.0 - p_btts_yes,
    }


def scoreline_prob(lam_h: float, lam_a: float, hg: int, ag: int, cap: int = 8, rho: float = 0.0) -> float:
    max_goal = max(cap, int(hg), int(ag))
    p_h = [poisson_pmf(lam_h, k) for k in range(max_goal + 1)]
    p_a = [poisson_pmf(lam_a, k) for k in range(max_goal + 1)]

    total = 0.0
    target = 0.0
    for i, ph in enumerate(p_h):
        for j, pa in enumerate(p_a):
            tau = dixon_coles_tau(i, j, lam_h, lam_a, rho)
            p = ph * pa * max(0.0, tau)
            total += p
            if i == hg and j == ag:
                target = p

    if total <= 0:
        return 0.0
    return target / total
