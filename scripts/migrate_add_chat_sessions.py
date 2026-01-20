import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


DDL = """
CREATE TABLE IF NOT EXISTS chat_sessions (
  session_id TEXT PRIMARY KEY,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  last_competition TEXT,
  last_day_utc TEXT,
  last_match_id TEXT,
  last_intent TEXT,
  meta_json TEXT
);

CREATE TABLE IF NOT EXISTS chat_messages (
  message_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  meta_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_session ON chat_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_chat_messages_created ON chat_messages(created_at_utc);
"""


def main() -> None:
    with get_conn() as conn:
        conn.executescript(DDL)
    print("OK: chat sessions tables ensured")


if __name__ == "__main__":
    main()
