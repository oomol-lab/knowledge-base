from sqlite3 import Cursor
from enum import Enum
from typing import Generator

class EventKind(Enum):
  Added = 0
  Updated = 1
  Removed = 2

class EventTarget(Enum):
  File = 0
  Directory = 1

def scan_events(cursor: Cursor) -> Generator[int, None, None]:
  cursor.execute("SELECT id FROM events ORDER BY id")
  while True:
    rows = cursor.fetchmany(size=100)
    if len(rows) == 0:
      break
    for row in rows:
      yield row[0]

def record_added_event(cursor: Cursor, target: EventTarget, path: str, base_id: int, mtime: float):
  cursor.execute(
    "SELECT kind, mtime FROM events WHERE base = ? AND path = ? AND target = ?",
    (base_id, path, target.value),
  )
  row = cursor.fetchone()

  if row is None:
    cursor.execute(
      "INSERT INTO events (kind, target, path, base, mtime) VALUES (?, ?, ?, ?, ?)",
      (EventKind.Added.value, target.value, path, base_id, mtime),
    )
  else:
    kind = EventKind(row[0])
    origin_mtime = row[1]
    _handle_updated_when_exits_row(
      cursor, target, kind,
      mtime, origin_mtime, base_id, path,
    )

def record_updated_event(cursor: Cursor, target: EventTarget, path: str, base_id:  int, mtime: float):
  cursor.execute(
    "SELECT kind, mtime FROM events WHERE base = ? AND path = ? AND target = ?",
    (base_id, path, target.value),
  )
  row = cursor.fetchone()

  if row is None:
    cursor.execute(
      "INSERT INTO events (kind, target, path, base, mtime) VALUES (?, ?, ?, ?, ?)",
      (EventKind.Updated.value, target.value, path, base_id, mtime),
    )
  else:
    kind = EventKind(row[0])
    origin_mtime = row[1]
    _handle_updated_when_exits_row(
      cursor, target, kind,
      mtime, origin_mtime, base_id, path,
    )

def _handle_updated_when_exits_row(
    cursor: Cursor, target: EventTarget, kind: EventKind,
    mtime: float, origin_mtime: float, base_id: int, path: str):

  if kind == EventKind.Removed:
    if mtime == origin_mtime:
      cursor.execute(
        "DELETE FROM events WHERE base = ? AND path = ? AND target = ?",
        (base_id, path, target.value),
      )
    else:
      cursor.execute(
        "UPDATE events SET kind = ?, mtime = ? WHERE base = ? AND path = ? AND target = ?",
        (EventKind.Updated.value, mtime, base_id, path, target.value),
      )
  elif mtime != origin_mtime:
    cursor.execute(
      "UPDATE events SET mtime = ? WHERE base = ? AND path = ? AND target = ?",
      (mtime, base_id, path, target.value),
    )

def record_removed_event(cursor: Cursor, target: EventTarget, path: str, base_id: int, mtime: float, removed_hash: bytes | None):
  cursor.execute(
    "SELECT kind, mtime, removed_hash FROM events WHERE base = ? AND path = ? AND target = ?",
    (base_id, path, target.value),
  )
  row = cursor.fetchone()

  if row is None:
    cursor.execute(
      "INSERT INTO events (kind, target, path, base, mtime, removed_hash) VALUES (?, ?, ?, ?, ?)",
      (EventKind.Removed.value, target.value, path, base_id, mtime, removed_hash),
    )
  else:
    kind = EventKind(row[0])
    origin_mtime = row[1]
    origin_removed_hash = row[2]

    if kind == EventKind.Added:
      cursor.execute(
        "DELETE FROM events WHERE base = ? AND path = ? AND target = ?",
        (base_id, path, target.value),
      )
    elif kind == EventKind.Updated:
      cursor.execute(
        "UPDATE events SET kind = ?, mtime = ? WHERE base = ? AND path = ? AND target = ? AND removed_hash = ?",
        (EventKind.Removed.value, mtime, base_id, path, target.value, removed_hash),
      )
    elif kind == EventKind.Removed and (
      mtime != origin_mtime or \
      removed_hash != origin_removed_hash
    ):
      cursor.execute(
        "UPDATE events SET mtime = ? WHERE base = ? AND path = ? AND target = ? AND removed_hash = ?",
        (mtime, base_id, path, target.value, removed_hash),
      )