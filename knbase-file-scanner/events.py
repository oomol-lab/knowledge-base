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

def record_added_event(cursor: Cursor, target: EventTarget, path: str, scope: str, mtime: float):
  cursor.execute(
    "SELECT kind, mtime FROM events WHERE scope = ? AND path = ? AND target = ?",
    (scope, path, target.value),
  )
  row = cursor.fetchone()

  if row is None:
    cursor.execute(
      "INSERT INTO events (kind, target, path, scope, mtime) VALUES (?, ?, ?, ?, ?)",
      (EventKind.Added.value, target.value, path, scope, mtime),
    )
  else:
    kind = EventKind(row[0])
    origin_mtime = row[1]
    _handle_updated_when_exits_row(cursor, target, kind, mtime, origin_mtime, scope, path)

def record_updated_event(cursor: Cursor, target: EventTarget, path: str, scope: str, mtime: float):
  cursor.execute(
    "SELECT kind, mtime FROM events WHERE scope = ? AND path = ? AND target = ?",
    (scope, path, target.value),
  )
  row = cursor.fetchone()

  if row is None:
    cursor.execute(
      "INSERT INTO events (kind, target, path, scope, mtime) VALUES (?, ?, ?, ?, ?)",
      (EventKind.Updated.value, target.value, path, scope, mtime),
    )
  else:
    kind = EventKind(row[0])
    origin_mtime = row[1]
    _handle_updated_when_exits_row(cursor, target, kind, mtime, origin_mtime, scope, path)

def _handle_updated_when_exits_row(
    cursor: Cursor, target: EventTarget, kind: EventKind,
    mtime: float, origin_mtime: float, scope: str, path: str):

  if kind == EventKind.Removed:
    if mtime == origin_mtime:
      cursor.execute(
        "DELETE FROM events WHERE scope = ? AND path = ? AND target = ?",
        (scope, path, target.value),
      )
    else:
      cursor.execute(
        "UPDATE events SET kind = ?, mtime = ? WHERE scope = ? AND path = ? AND target = ?",
        (EventKind.Updated.value, mtime, scope, path, target.value),
      )
  elif mtime != origin_mtime:
    cursor.execute(
      "UPDATE events SET mtime = ? WHERE scope = ? AND path = ? AND target = ?",
      (mtime, scope, path, target.value),
    )

def record_removed_event(cursor: Cursor, target: EventTarget, path: str, scope: str, mtime: float):
  cursor.execute(
    "SELECT kind, mtime FROM events WHERE scope = ? AND path = ? AND target = ?",
    (scope, path, target.value),
  )
  row = cursor.fetchone()

  if row is None:
    cursor.execute(
      "INSERT INTO events (kind, target, path, scope, mtime) VALUES (?, ?, ?, ?, ?)",
      (EventKind.Removed.value, target.value, path, scope, mtime),
    )
  else:
    kind = EventKind(row[0])
    origin_mtime = row[1]

    if kind == EventKind.Added:
      cursor.execute(
        "DELETE FROM events WHERE scope = ? AND path = ? AND target = ?",
        (scope, path, target.value),
      )
    elif kind == EventKind.Updated:
      cursor.execute(
        "UPDATE events SET kind = ?, mtime = ? WHERE scope = ? AND path = ? AND target = ?",
        (EventKind.Removed.value, mtime, scope, path, target.value),
      )
    elif kind == EventKind.Removed and mtime != origin_mtime:
      cursor.execute(
        "UPDATE events SET mtime = ? WHERE scope = ? AND path = ? AND target = ?",
        (mtime, scope, path, target.value),
      )