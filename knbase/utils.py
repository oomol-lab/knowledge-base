from typing import Any, Generator, Iterable, TypeVar
from sqlite3 import Cursor


_CHUNK_SIZE = 36

T = TypeVar("T")

def chunks(target: Iterable[T], size: int=_CHUNK_SIZE) -> Iterable[list[T]]:
  buffer: list[T] = []
  for item in target:
    buffer.append(item)
    if len(buffer) >= size:
      yield buffer
      buffer = []
  if len(buffer) > 0:
    yield buffer

def fetchmany(cursor: Cursor, size: int=_CHUNK_SIZE) -> Generator[Any, Any, None]:
  while True:
    rows = cursor.fetchmany(size)
    if not rows:
      break
    yield from rows