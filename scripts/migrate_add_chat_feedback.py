import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


DDL = """
CREATE TABLE IF NOT EXISTS chat_feedback (
  feedback_id TEXT PRIMARY KEY,
  created_at_utc TEXT NOT NULL,
  query TEXT NOT NULL,
  response TEXT NOT NULL,
  label TEXT NOT NULL,
  notes TEXT,
  match_id TEXT,
  meta_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_chat_feedback_created ON chat_feedback(created_at_utc);
CREATE INDEX IF NOT EXISTS idx_chat_feedback_label ON chat_feedback(label);
"""


def main() -> None:
    with get_conn() as conn:
        conn.executescript(DDL)
    print("OK: chat_feedback table ensured")


if __name__ == "__main__":
    main()
