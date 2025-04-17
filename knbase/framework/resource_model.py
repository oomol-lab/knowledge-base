import json
import time

from typing import Any, Generator
from sqlite3 import Cursor

from .common import FRAMEWORK_DB
from .module_context import ModuleContext
from ..sqlite3_pool import register_table_creators
from ..modules import Module, Resource, ResourceBase


class ResourceModel:
  def __init__(self, modules_context: ModuleContext):
    self._ctx: ModuleContext = modules_context

  def create_resource_base(self, cursor: Cursor, module: Module) -> ResourceBase:
    module_id = self._ctx.module_id(module)
    created_at = int(time.time() * 1000)
    cursor.execute(
      "INSERT INTO resource_bases (module, created_at, created_at) VALUES (?, ?, ?)",
      (module_id, created_at, created_at),
    )
    base_id = cursor.lastrowid
    return ResourceBase(
      id=base_id,
      module=module,
    )

  def get_resource_base(self, cursor: Cursor, base_id: int) -> ResourceBase:
    cursor.execute(
      "SELECT module FROM resource_bases WHERE id = ?",
      (base_id,),
    )
    row = cursor.fetchone()
    if row is None:
      raise ValueError(f"Base with id {base_id} not found")

    module_id = row[0]
    module = self._ctx.module(module_id)
    return ResourceBase(
      id=base_id,
      module=module,
    )

  def get_resource(self, cursor: Cursor, resource_id: int) -> Resource | None:
    cursor.execute(
      "SELECT hash, base, content_type, meta, updated_at FROM resources WHERE id = ?",
      (resource_id,),
    )
    row = cursor.fetchone()
    if row is None:
      return None

    hash, base_id, content_type, meta_text, updated_at = row
    return Resource(
      id=resource_id,
      hash=hash,
      base=self.get_resource_base(cursor, base_id),
      content_type=content_type,
      meta=json.loads(meta_text),
      updated_at=updated_at,
    )

  def count_resources(
      self,
      cursor: Cursor,
      resource_base: ResourceBase,
      hash: bytes,
    ) -> int:
    cursor.execute(
      "SELECT COUNT(*) FROM resources WHERE base = ? AND hash = ?",
      (resource_base.id, hash),
    )
    row = cursor.fetchone()
    if row is None:
      return 0
    return row[0]

  def get_resources(
        self,
        cursor: Cursor,
        resource_base: ResourceBase,
        hash: bytes,
      ) -> Generator[Resource, None, None]:

    cursor.execute(
      "SELECT id, content_type, meta, updated_at FROM resources WHERE base = ? AND hash = ? ORDER BY updated_at DESC",
      (resource_base.id, hash),
    )
    for row in cursor.fetchall():
      resource_id, content_type, meta_text, updated_at = row
      yield Resource(
        id=resource_id,
        hash=hash,
        base=resource_base,
        content_type=content_type,
        meta=json.loads(meta_text),
        updated_at=updated_at,
      )

  def save_resource(self, cursor: Cursor, resource: Resource) -> None:
    cursor.execute(
      "INSERT INTO resources (id, hash, base, content_type, meta, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
      (
        resource.id,
        resource.hash,
        resource.base.id,
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
      "UPDATE resources SET hash = ?, content_type = ?, meta = ?, updated_at = ? WHERE id = ?",
      (
        hash,
        content_type,
        json.dumps(meta),
        updated_at,
        origin_resource.id,
      ),
    )

  def remove_resource(self, cursor: Cursor, resource_id: int) -> None:
    cursor.execute(
      "DELETE FROM resources WHERE id = ?",
      (resource_id,),
    )

def _create_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE resource_bases (
      id INTEGER PRIMARY KEY,
      module INTEGER NOT NULL,
      created_at INTEGER,
      updated_at INTEGER
    )
  """)

  cursor.execute("""
    CREATE TABLE resources (
      id INTEGER PRIMARY KEY,
      hash BLOB NOT NULL,
      base INTEGER NOT NULL,
      content_type TEXT NOT NULL,
      meta TEXT NOT NULL,
      updated_at INTEGER
    )
  """)

  cursor.execute("""
    CREATE INDEX idx_resource_hash ON resources (base, hash)
  """)

register_table_creators(FRAMEWORK_DB, _create_tables)