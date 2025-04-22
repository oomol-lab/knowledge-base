from abc import ABC, abstractmethod
from sqlite3 import Cursor
from knbase.sqlite3_pool import SQLite3Pool
from .event_parser import EventTarget
from .events import record_removed_event

class Scope(ABC):

  @property
  @abstractmethod
  def scopes(self) -> list[str]:
    pass

  @abstractmethod
  def scope_path(self, scope: str) -> str | None:
    pass

class ScopeManager(Scope):
  def __init__(self, db: SQLite3Pool):
    self._db: SQLite3Pool = db
    self._removed_sources: dict[str, str] = {}
    with self._db.connect() as (cursor, _):
      self._sources = self._fill_sources(cursor)

  @property
  def scopes(self) -> list[str]:
    return list(self._sources.keys())

  def scope_path(self, scope: str) -> str | None:
    scope_path = self._sources.get(scope, None)
    if scope_path is None:
      scope_path = self._removed_sources.get(scope, None)
    return scope_path

  def commit_sources(self, sources: dict[str, str]):
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        origin_sources = self._fill_sources(cursor)
        removed_scopes: list[str] = []

        for name, path in sources.items():
          origin_path = origin_sources.get(name, None)
          if origin_path is None:
            cursor.execute("INSERT INTO scopes (name, path) VALUES (?, ?)", (name, path))
          else:
            origin_sources.pop(name)
            if origin_path != path:
              cursor.execute("UPDATE scopes SET path = ? WHERE name = ?", (path, name))

        for name in origin_sources.keys():
          cursor.execute("DELETE FROM scopes WHERE name = ?", (name,))
          removed_scopes.append(name)

        for scope_name in removed_scopes:
          self._removed_sources[scope_name] = origin_sources[scope_name]
          self._record_events_about_scope_removed(cursor, scope_name)

        self._sources = sources
        conn.commit()

      except Exception as e:
        conn.rollback()
        raise e

  def _fill_sources(self, cursor: Cursor) -> dict[str, str]:
    cursor.execute("SELECT name, path FROM scopes")
    sources: dict[str, str] = {}
    for name, path in cursor.fetchall():
      sources[name] = path
    return sources

  def _record_events_about_scope_removed(self, cursor: Cursor, scope: str):
    cursor.execute("SELECT path, mtime, children FROM files WHERE scope = ?", (scope,))

    while True:
      rows = cursor.fetchmany(size=100)
      if len(rows) == 0:
        break
      for row in rows:
        path, mtime, children = row
        target = EventTarget.File if children is None else EventTarget.Directory
        record_removed_event(cursor, target, path, scope, mtime)

    cursor.execute("DELETE FROM files WHERE scope = ?", (scope,))
