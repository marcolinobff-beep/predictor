from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Dict, Any


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--oos", default="data/reports/oos_backtest.json")
    ap.add_argument("--out", default="data/calibration/calibration_policy.json")
    ap.add_argument("--min-delta", type=float, default=0.0005)
    args = ap.parse_args()

    with open(args.oos, "r", encoding="utf-8") as f:
        data = json.load(f)

    out: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": args.oos,
        "split_date": data.get("split_date"),
        "min_delta": args.min_delta,
        "by_league": {},
    }

    by_league = data.get("by_league") or {}
    for league, res in by_league.items():
        if not res:
            continue
        base = res.get("test_logloss_1x2")
        cal = res.get("test_logloss_1x2_calibrated")
        calibrate_1x2 = False
        if base is not None and cal is not None:
            calibrate_1x2 = (cal + args.min_delta) < base
        out["by_league"][league] = {
            "calibrate_1x2": calibrate_1x2,
            "base_logloss_1x2": base,
            "cal_logloss_1x2": cal,
        }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=True, indent=2)

    print(f"OK: wrote calibration policy to {args.out}")


if __name__ == "__main__":
    main()
