import hashlib
import json
from typing import Any

def stable_hash(obj: Any) -> str:
    payload = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
