import json

from typing import Any, Generator
from sqlite3 import Cursor

from .common import FRAMEWORK_DB
from .module_context import ModuleContext
from ..sqlite3_pool import register_table_creators
from ..module import KnowledgeBase, Resource


class ResourceModel:
  def __init__(self, modules_context: ModuleContext):
    self._ctx: ModuleContext = modules_context

  def get_resource(
        self,
        cursor: Cursor,
        knbase: KnowledgeBase,
        resource_id: str,
      ) -> Resource | None:

    cursor.execute(
      "SELECT hash, content_type, meta, updated_at FROM resources WHERE knbase =? AND id = ?",
      (knbase.id, resource_id),
    )
    row = cursor.fetchone()
    if row is None:
      return None

    hash, content_type, meta_text, updated_at = row
    return Resource(
      id=resource_id,
      base=knbase,
      hash=hash,
      content_type=content_type,
      meta=json.loads(meta_text),
      updated_at=updated_at,
    )

  def count_resources(
        self,
        cursor: Cursor,
        knbase: KnowledgeBase,
        hash: bytes,
      ) -> int:

    cursor.execute(
      "SELECT COUNT(*) FROM resources WHERE knbase = ? AND hash = ?",
      (knbase.id, hash),
    )
    row = cursor.fetchone()
    if row is None:
      return 0
    return row[0]

  def get_resources(
        self,
        cursor: Cursor,
        knbase: KnowledgeBase,
        hash: bytes,
      ) -> Generator[Resource, None, None]:

    cursor.execute(
      "SELECT id, content_type, meta, updated_at FROM resources WHERE knbase = ? AND hash = ? ORDER BY updated_at DESC",
      (knbase.id, hash),
    )
    for row in cursor.fetchall():
      resource_id, content_type, meta_text, updated_at = row
      yield Resource(
        id=resource_id,
        hash=hash,
        base=knbase,
        content_type=content_type,
        meta=json.loads(meta_text),
        updated_at=updated_at,
      )

  def save_resource(self, cursor: Cursor, resource: Resource) -> None:
    cursor.execute(
      "INSERT INTO resources (knbase, id, hash, content_type, meta, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
      (
        resource.base.id,
        resource.id,
        resource.hash,
        resource.content_type,
        json.dumps(resource.meta),
        resource.updated_at,
      ),
    )

  def update_resource(
        self,
        cursor: Cursor,
        origin_resource: Resource,
        hash: bytes | None = None,
        content_type: str = None,
        meta: Any | None = None,
        updated_at: int | None = None,
      ) -> None:

    if hash is None:
      hash = origin_resource.hash
    if content_type is None:
      content_type = origin_resource.content_type
    if meta is None:
      meta = origin_resource.meta
    if updated_at is None:
      updated_at = origin_resource.updated_at

    cursor.execute(
      "UPDATE resources SET hash = ?, content_type = ?, meta = ?, updated_at = ? WHERE id = ? AND knbase = ?",
      (
        hash,
        content_type,
        json.dumps(meta),
        updated_at,
        origin_resource.id,
        origin_resource.base.id,
      ),
    )

  def remove_resource(self, cursor: Cursor, knbase: KnowledgeBase, resource_id: str) -> None:
    cursor.execute(
      "DELETE FROM resources WHERE id = ? AND knbase = ?",
      (resource_id, knbase.id),
    )

def _create_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE resources (
      knbase INTEGER KEY,
      id TEXT KEY,
      hash BLOB NOT NULL,
      content_type TEXT NOT NULL,
      meta TEXT NOT NULL,
      updated_at INTEGER,
      PRIMARY KEY (knbase, id)
    )
  """)

  cursor.execute("""
    CREATE INDEX idx_resource_hash ON resources (knbase, hash)
  """)

register_table_creators(FRAMEWORK_DB, _create_tables)