from dataclasses import dataclass
from knbase.sqlite3_pool import SQLite3Pool
from .events import EventKind, EventTarget

@dataclass
class Event:
  id: int
  kind: EventKind
  target: EventTarget
  base_id: str
  path: str
  removed_hash: bytes | None
  mtime: float
  db: SQLite3Pool | None = None

  def close(self, new_hash: bytes | None = None):
    if self.db is None:
      return

    with self.db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        if new_hash is not None:
          cursor.execute(
            "UPDATE files SET last_hash = ? WHERE base = ? AND path = ?",
            (new_hash, self.base_id, self.path),
          )
        cursor.execute("DELETE FROM events WHERE id = ?", (self.id,))
        conn.commit()

      except Exception as e:
        conn.rollback()
        raise e

class EventParser:
  def __init__(self, db: SQLite3Pool):
    self._db: SQLite3Pool = db

  def parse(self, event_id: int) -> Event:
    with self._db.connect() as (cursor, _):
      cursor.execute(
        "SELECT kind, target, path, base, mtime, removed_hash FROM events WHERE id = ?",
        (event_id,)
      )
      row = cursor.fetchone()
      if row is None:
        raise ValueError(f"Event not found: {event_id}")

      return Event(
        id=event_id,
        kind=EventKind(row[0]),
        target=EventTarget(row[1]),
        path=row[2],
        base_id=row[3],
        mtime=row[4],
        removed_hash=row[5],
        db=self._db,
      )