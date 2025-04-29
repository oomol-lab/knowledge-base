import os
import sqlite3

from typing import Generator


class Sources:
  def __init__(self, db_path: str) -> None:
    self._db_path: str = db_path
    is_first_time = not os.path.exists(db_path)
    if is_first_time:
      conn = sqlite3.connect(db_path)
      cursor = conn.cursor()
      try:
        cursor.execute("""
          CREATE TABLE sources (
            name TEXT PRIMARY KEY,
            path TEXT not NULL
          )
        """)
        conn.commit()

      finally:
        cursor.close()
        conn.close()

  def path(self, name: str) -> str:
    conn = sqlite3.connect(self._db_path)
    cursor = conn.cursor()
    try:
      cursor.execute("SELECT path FROM sources WHERE name = ?", (name,))
      row = cursor.fetchone()
      assert row is not None, f"Source {name} not found"
      return row[0]

    finally:
      cursor.close()
      conn.close()

  def items(self) -> Generator[tuple[str, str], None, None]:
    conn = sqlite3.connect(self._db_path)
    cursor = conn.cursor()
    try:
      cursor.execute("SELECT name, path FROM sources")
      for row in cursor.fetchall():
        name, path = row
        yield name, path

    finally:
      cursor.close()
      conn.close()

  def put(self, name: str, path: str) -> None:
    conn = sqlite3.connect(self._db_path)
    cursor = conn.cursor()
    try:
      cursor.execute("SELECT path FROM sources WHERE name = ?", (name,))
      row = cursor.fetchone()
      if row is not None:
        cursor.execute("UPDATE sources SET path = ? WHERE name = ?", (path, name))
      else:
        cursor.execute("INSERT INTO sources (name, path) VALUES (?, ?)", (name, path))
      conn.commit()

    finally:
      cursor.close()
      conn.close()

  def remove(self, name: str) -> None:
    conn = sqlite3.connect(self._db_path)
    cursor = conn.cursor()
    try:
      cursor.execute("DELETE FROM sources WHERE name = ?", (name,))
      conn.commit()

    finally:
      cursor.close()
      conn.close()