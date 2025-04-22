from dataclasses import dataclass
from knbase.sqlite3_pool import SQLite3Pool
from .events import EventKind, EventTarget

@dataclass
class Event:
  id: int
  kind: EventKind
  target: EventTarget
  scope: str
  path: str
  mtime: float
  db: SQLite3Pool | None = None

  def close(self):
    if self.db is not None:
      with self.db.connect() as (cursor, conn):
        cursor.execute("DELETE FROM events WHERE id = ?", (self.id,))
        conn.commit()

class EventParser:
  def __init__(self, db: SQLite3Pool):
    self._db: SQLite3Pool = db

  def parse(self, event_id: int) -> Event:
    with self._db.connect() as (cursor, _):
      cursor.execute(
        "SELECT kind, target, path, scope, mtime FROM events WHERE id = ?",
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
        scope=row[3],
        mtime=row[4],
        db=self._db,
      )