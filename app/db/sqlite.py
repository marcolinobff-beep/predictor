from __future__ import annotations

import sqlite3
from contextlib import contextmanager
import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

def _project_root() -> Path:
    # questo file è: <root>/app/db/sqlite.py -> root = 3 livelli su
    return Path(__file__).resolve().parents[2]

# carica .env dalla root progetto (anche se lanciato da altre directory)
load_dotenv((_project_root() / ".env"))

def _db_path() -> Path:
    root = _project_root()
    rel = os.getenv("SQLITE_PATH", "data/sqlite/football.db")
    p = (root / rel).resolve() if not os.path.isabs(rel) else Path(rel).resolve()
    return p

@contextmanager
def get_conn():
    db_path = _db_path()

    # FAIL FAST: se non esiste, non creare DB vuoti tipo data/app.db per sbaglio
    if not db_path.exists():
        raise FileNotFoundError(
            f"SQLite DB non trovato: {db_path}\n"
            f"Controlla SQLITE_PATH nel .env (root progetto)."
        )

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
