import time
import threading
from typing import Any, Optional


class TTLCache:
    def __init__(self, default_ttl: int = 60):
        self._store: dict[str, tuple[Any, float]] = {}
        self._default_ttl = default_ttl
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            data, expiry = entry
            if time.time() > expiry:
                del self._store[key]
                return None
            return data

    def set(self, key: str, data: Any, ttl: Optional[int] = None) -> None:
        with self._lock:
            expiry = time.time() + (ttl or self._default_ttl)
            self._store[key] = (data, expiry)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def stats(self) -> dict:
        with self._lock:
            now = time.time()
            total = len(self._store)
            active = sum(1 for _, (_, exp) in self._store.items() if exp > now)
            return {"total_entries": total, "active_entries": active}
