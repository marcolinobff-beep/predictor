from app.db.sqlite import get_conn

DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS matches (
  match_id TEXT PRIMARY KEY,
  competition TEXT NOT NULL,
  season TEXT,
  kickoff_utc TEXT NOT NULL,
  home TEXT NOT NULL,
  away TEXT NOT NULL,
  venue TEXT
);

-- Feature store MVP (vuoto per ora; lo riempiamo dopo con pipeline)
CREATE TABLE IF NOT EXISTS match_features (
  match_id TEXT NOT NULL,
  features_version TEXT NOT NULL,
  features_json TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  PRIMARY KEY (match_id, features_version)
);

-- Audit minimale
CREATE TABLE IF NOT EXISTS audit_log (
  request_id TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  PRIMARY KEY (request_id)
);

-- Web cache placeholder (lo implementiamo nei prossimi step)
CREATE TABLE IF NOT EXISTS web_cache (
  cache_key TEXT PRIMARY KEY,
  fetched_at_utc TEXT NOT NULL,
  ttl_seconds INTEGER NOT NULL,
  reliability_score REAL NOT NULL,
  raw_json TEXT NOT NULL
);

-- News articles (manual ingest per MVP)
CREATE TABLE IF NOT EXISTS news_articles (
  news_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  title TEXT NOT NULL,
  url TEXT,
  published_at_utc TEXT NOT NULL,
  reliability_score REAL NOT NULL,
  related_match_id TEXT,
  related_team TEXT,
  related_player TEXT,
  event_type TEXT,
  summary TEXT,
  raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_news_match ON news_articles(related_match_id);
CREATE INDEX IF NOT EXISTS idx_news_team ON news_articles(related_team);
CREATE INDEX IF NOT EXISTS idx_news_pub ON news_articles(published_at_utc);

-- Chat feedback (manual labeling per training)
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

-- Chat sessions + messages (conversation memory)
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

def main():
    with get_conn() as conn:
        conn.executescript(DDL)
    print("OK: DB initialized")

if __name__ == "__main__":
    main()
