from __future__ import annotations
import sqlite3
import threading

from typing import Callable


_THREAD_POOL = threading.local()
_MAX_STACK_SIZE = 2

def build_thread_pool():
  if not hasattr(_THREAD_POOL, "value"):
    setattr(_THREAD_POOL, "value", ThreadPool())

def release_thread_pool():
  if hasattr(_THREAD_POOL, "value"):
    # pylint: disable=E1101
    getattr(_THREAD_POOL, "value").release()

def get_thread_pool() -> ThreadPool | None:
  if hasattr(_THREAD_POOL, "value"):
    return getattr(_THREAD_POOL, "value")
  return None


class SQLite3ConnectionSession:
  def __init__(self, conn: sqlite3.Connection, send_back: Callable[[sqlite3.Connection], None]):
    self._conn: sqlite3.Connection = conn
    self._cursor: sqlite3.Cursor = conn.cursor()
    self._send_back: Callable[[sqlite3.Connection], None] = send_back
    self._is_closed: bool = False

  @property
  def conn(self) -> sqlite3.Connection:
    return self._conn

  @property
  def cursor(self) -> sqlite3.Cursor:
    return self._cursor

  def close(self):
    if self._is_closed:
      return
    self._is_closed = True
    self._cursor.close()
    if self._conn.in_transaction:
      self._conn.rollback()
    self._send_back(self._conn)

  def __enter__(self) -> tuple[sqlite3.Cursor, sqlite3.Connection]:
    return self._cursor, self._conn

  def __exit__(self, exc_type, exc_value, traceback):
    self.close()

class ThreadPool():
  def __init__(self):
    self._stacks: dict[str, list[sqlite3.Connection]] = {}

  def get(self, format_name: str) -> sqlite3.Connection | None:
    stack = self._stack(format_name)
    if len(stack) == 0:
      return None
    return stack.pop()

  def send_back(self, format_name: str, conn: sqlite3.Connection):
    stack = self._stack(format_name)
    if len(stack) >= _MAX_STACK_SIZE:
      conn.close()
    else:
      stack.append(conn)

  def release(self):
    for stack in self._stacks.values():
      for conn in stack:
        conn.close()
    self._stacks.clear()

  def _stack(self, format_name: str) -> list[sqlite3.Connection]:
    stack = self._stacks.get(format_name, None)
    if stack is None:
      stack = []
      self._stacks[format_name] = stack
    return stack
