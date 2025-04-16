from __future__ import annotations

import os
import sqlite3

from threading import Lock
from typing import Callable

_FORMATS_LOCK: Lock = Lock()
_FORMATS: dict[str, _SQLite3Format] = {}

def register_table_creators(format_name: str, create_table: Callable[[sqlite3.Cursor], None]) -> None:
  get_format(format_name).register(create_table)

def get_format(format_name: str) -> _SQLite3Format:
  with _FORMATS_LOCK:
    pool: _SQLite3Format | None = _FORMATS.get(format_name, None)
    if pool is None:
      pool = _SQLite3Format()
      _FORMATS[format_name] = pool
  return pool

class _SQLite3Format:
  def __init__(self) -> None:
    self._lock: Lock = Lock()
    self._table_creators: list[Callable[[sqlite3.Cursor], None]] = []
    self._lock_table_creators: bool = False

  def register(self, create_table: Callable[[sqlite3.Cursor], None]) -> None:
    with self._lock:
      if self._lock_table_creators:
        raise RuntimeError("Cannot register table creator after created any pools")
      self._table_creators.append(create_table)

  def create_tables(self, path: str):
    with self._lock:
      self._lock_table_creators = True

    if not os.path.exists(path):
      with sqlite3.connect(path) as conn:
        for create_table in self._table_creators:
          cursor = conn.cursor()
          try:
            create_table(cursor)
          finally:
            cursor.close()
        conn.commit()
