from app.db.sqlite import get_conn

DDL = """
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
"""


def main():
    with get_conn() as conn:
        conn.executescript(DDL)
    print("OK: news_articles ready")


if __name__ == "__main__":
    main()
