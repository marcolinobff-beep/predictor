from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Dict, Any


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--oos", default="data/reports/oos_backtest.json")
    ap.add_argument("--out", default="data/calibration/temp_scale_1x2.json")
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
        temp_loss = res.get("test_logloss_1x2_temp")
        temp = res.get("temp_scale_1x2")
        enabled = False
        if base is not None and temp_loss is not None and temp is not None:
            enabled = (temp_loss + args.min_delta) < base
        out["by_league"][league] = {
            "enabled": enabled,
            "temp": temp,
            "base_logloss_1x2": base,
            "temp_logloss_1x2": temp_loss,
        }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=True, indent=2)

    print(f"OK: wrote temp scale config to {args.out}")


if __name__ == "__main__":
    main()
