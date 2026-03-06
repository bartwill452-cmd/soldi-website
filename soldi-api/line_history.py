"""
Line history storage using SQLite.

Records odds snapshots over time, only inserting new rows when a line
(price or point) actually changes. Provides query functions for the
line-history API endpoint.
"""

import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from models import OddsEvent

logger = logging.getLogger(__name__)

_db_path = "line_history.db"
_lock = threading.Lock()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(db_path: str = "line_history.db") -> None:
    """Create the database and tables if they don't exist."""
    global _db_path
    _db_path = db_path

    conn = _connect()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS line_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                sport_key TEXT NOT NULL,
                bookmaker_key TEXT NOT NULL,
                market_key TEXT NOT NULL,
                outcome_name TEXT NOT NULL,
                price REAL NOT NULL,
                point REAL,
                recorded_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_lookup
                ON line_snapshots(event_id, bookmaker_key, market_key, outcome_name);

            CREATE INDEX IF NOT EXISTS idx_sport
                ON line_snapshots(sport_key);

            CREATE INDEX IF NOT EXISTS idx_recorded_at
                ON line_snapshots(recorded_at);
        """)
        conn.commit()
        logger.info("Line history database initialized at %s", db_path)
    finally:
        conn.close()


def record_snapshots(events: List[OddsEvent], sport_key: str) -> None:
    """Record odds snapshots, only inserting rows where price/point changed."""
    if not events:
        return

    with _lock:
        conn = _connect()
        try:
            # Load all latest snapshots for this sport in one query
            latest = {}  # type: Dict[Tuple[str, str, str, str], Tuple[float, Optional[float]]]
            rows = conn.execute(
                """
                SELECT event_id, bookmaker_key, market_key, outcome_name, price, point
                FROM line_snapshots
                WHERE id IN (
                    SELECT MAX(id)
                    FROM line_snapshots
                    WHERE sport_key = ?
                    GROUP BY event_id, bookmaker_key, market_key, outcome_name
                )
                """,
                (sport_key,),
            ).fetchall()

            for row in rows:
                key = (row["event_id"], row["bookmaker_key"], row["market_key"], row["outcome_name"])
                latest[key] = (row["price"], row["point"])

            now = datetime.now(timezone.utc).isoformat()
            inserts = []  # type: List[Tuple[str, str, str, str, str, float, Optional[float], str]]

            for event in events:
                for bm in event.bookmakers:
                    for market in bm.markets:
                        for outcome in market.outcomes:
                            key = (event.id, bm.key, market.key, outcome.name)
                            prev = latest.get(key)

                            price = float(outcome.price)
                            point = float(outcome.point) if outcome.point is not None else None

                            if prev is None:
                                # First time seeing this line
                                inserts.append((
                                    event.id, sport_key, bm.key, market.key,
                                    outcome.name, price, point, now,
                                ))
                            else:
                                prev_price, prev_point = prev
                                if prev_price != price or prev_point != point:
                                    inserts.append((
                                        event.id, sport_key, bm.key, market.key,
                                        outcome.name, price, point, now,
                                    ))

            if inserts:
                conn.executemany(
                    """
                    INSERT INTO line_snapshots
                        (event_id, sport_key, bookmaker_key, market_key,
                         outcome_name, price, point, recorded_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    inserts,
                )
                conn.commit()
                logger.info("Recorded %d line changes for %s", len(inserts), sport_key)
        except Exception as e:
            logger.error("Failed to record snapshots: %s", e)
        finally:
            conn.close()


def get_line_history(
    event_id: str,
    market_key: Optional[str] = None,
    bookmaker_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Query line history for an event, optionally filtered by market/bookmaker."""
    conn = _connect()
    try:
        query = "SELECT * FROM line_snapshots WHERE event_id = ?"
        params = [event_id]  # type: List[Any]

        if market_key:
            query += " AND market_key = ?"
            params.append(market_key)
        if bookmaker_key:
            query += " AND bookmaker_key = ?"
            params.append(bookmaker_key)

        query += " ORDER BY recorded_at ASC"

        rows = conn.execute(query, params).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "sport_key": row["sport_key"],
                "bookmaker_key": row["bookmaker_key"],
                "market_key": row["market_key"],
                "outcome_name": row["outcome_name"],
                "price": row["price"],
                "point": row["point"],
                "recorded_at": row["recorded_at"],
            }
            for row in rows
        ]
    finally:
        conn.close()


def purge_old_snapshots(days: int = 7) -> int:
    """Delete snapshots older than the specified number of days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with _lock:
        conn = _connect()
        try:
            cursor = conn.execute(
                "DELETE FROM line_snapshots WHERE recorded_at < ?",
                (cutoff,),
            )
            conn.commit()
            deleted = cursor.rowcount
            if deleted:
                logger.info("Purged %d old line snapshots (older than %d days)", deleted, days)
            return deleted
        finally:
            conn.close()
