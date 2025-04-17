from typing import Any, Generator, Iterable, TypeVar
from sqlite3 import Cursor


T = TypeVar("T")

def chunks(target: Iterable[T], size: int) -> Iterable[list[T]]:
  buffer: list[T] = []
  for item in target:
    buffer.append(item)
    if len(buffer) >= size:
      yield buffer
      buffer = []
  if len(buffer) > 0:
    yield buffer

def fetchmany(cursor: Cursor, size: int) -> Generator[Any, Any, None]:
  while True:
    rows = cursor.fetchmany(size)
    if not rows:
      break
    yield from rows