import json
from datetime import datetime, timezone
from app.db.sqlite import get_conn

def main():
    match_id = input("match_id: ").strip()
    features_version = input("features_version (es. manual_v1): ").strip() or "manual_v1"

    lambda_home = float(input("lambda_home (es. 1.35): ").strip())
    lambda_away = float(input("lambda_away (es. 1.05): ").strip())

    features = {
        "lambda_home": lambda_home,
        "lambda_away": lambda_away
    }

    created_at_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO match_features (match_id, features_version, features_json, created_at_utc)
            VALUES (?, ?, ?, ?)
            """,
            (match_id, features_version, json.dumps(features), created_at_utc)
        )

    print("OK: features saved")

if __name__ == "__main__":
    main()
