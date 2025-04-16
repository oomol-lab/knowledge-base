import json
import time

from typing import Any, Generator
from sqlite3 import Cursor

from .common import FRAMEWORK_DB, ConnSession
from .module_context import ModuleContext
from ..sqlite3_pool import register_table_creators
from ..modules import Module, Resource, ResourceBase


class ResourceModel:
  def __init__(self, modules_context: ModuleContext):
    self._ctx: ModuleContext = modules_context

  def create_resource_base(self, session: ConnSession, module: Module, meta: Any) -> ResourceBase:
    cursor, conn = session
    meta_text = json.dumps(meta)
    module_id = self._ctx.model_id(module)
    created_at = int(time.time() * 1000)
    cursor.execute(
      "INSERT INTO bases (model, meta, created_at, created_at) VALUES (?, ?, ?, ?)",
      parameters=(module_id, meta_text, created_at, created_at),
    )
    conn.commit()
    base_id = cursor.lastrowid
    return ResourceBase(
      id=base_id,
      module=module,
      meta=meta,
    )

  def get_resource_base(self, session: ConnSession, base_id: int) -> ResourceBase:
    cursor, _ = session
    cursor.execute(
      "SELECT model, meta FROM bases WHERE id = ?",
      parameters=(base_id,),
    )
    row = cursor.fetchone()
    if row is None:
      raise ValueError(f"Base with id {base_id} not found")

    model_id, meta_text = row
    module = self._ctx.module(model_id)
    return ResourceBase(
      id=base_id,
      module=module,
      meta=json.loads(meta_text),
    )

  def get_resource(self, session: ConnSession, resource_id: int) -> Resource:
    cursor, _ = session
    cursor.execute(
      "SELECT hash, base, content_type, meta, updated_at FROM resources WHERE id = ?",
      parameters=(resource_id,),
    )
    row = cursor.fetchone()
    if row is None:
      raise ValueError(f"Resource with id {resource_id} not found")

    hash, base_id, content_type, meta_text, updated_at = row
    return Resource(
      id=resource_id,
      hash=hash,
      base=self.get_resource_base(session, base_id),
      content_type=content_type,
      meta=json.loads(meta_text),
      updated_at=updated_at,
    )

  def count_resources(
      self,
      session: ConnSession,
      resource_base: ResourceBase,
      hash: bytes,
    ) -> int:
    cursor, _ = session
    cursor.execute(
      "SELECT COUNT(*) FROM resources WHERE base = ? AND hash = ?",
      parameters=(resource_base.id, hash),
    )
    row = cursor.fetchone()
    if row is None:
      return 0
    return row[0]

  def get_resources(
        self,
        session: ConnSession,
        resource_base: ResourceBase,
        hash: bytes,
      ) -> Generator[Resource, None, None]:

    cursor, _ = session
    cursor.execute(
      "SELECT id, content_type, meta, updated_at FROM resources WHERE base = ? AND hash = ?",
      parameters=(resource_base.id, hash),
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

  def create_resource(
        self,
        session: ConnSession,
        hash: bytes,
        resource_base: ResourceBase,
        content_type: str,
        meta: Any,
        updated_at: int,
      ) -> Resource:

    cursor, conn = session
    meta_text = json.dumps(meta)
    cursor.execute(
      "INSERT INTO resources (hash, base, content_type, meta, updated_at) VALUES (?, ?, ?, ?, ?)",
      parameters=(
        hash,
        resource_base.id,
        content_type,
        meta_text,
        updated_at,
      ),
    )
    conn.commit()
    resource_id = cursor.lastrowid
    return Resource(
      id=resource_id,
      hash=hash,
      base=resource_base,
      content_type=content_type,
      meta=meta,
      updated_at=updated_at,
    )

  def update_resource(
        self,
        session: ConnSession,
        resource_id: int,
        hash: bytes | None = None,
        content_type: str = None,
        meta: Any | None = None,
        updated_at: int | None = None,
      ) -> Resource:

    cursor, conn = session
    origin_resource = self.get_resource(session, resource_id)

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
      parameters=(
        hash,
        content_type,
        json.dumps(meta),
        updated_at,
        resource_id,
      ),
    )
    conn.commit()
    return origin_resource

  def remove_resource(self, session: ConnSession, resource_id: int) -> None:
    cursor, conn = session
    cursor.execute(
      "DELETE FROM resources WHERE id = ?",
      parameters=(resource_id,),
    )
    conn.commit()

def _create_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE bases (
      id INTEGER PRIMARY KEY,
      model INTEGER NOT NULL,
      meta TEXT NOT NULL,
      created_at INTEGER,
      updated_at INTEGER,
    )
  """)

  cursor.execute("""
    CREATE TABLE resources (
      id INTEGER PRIMARY KEY,
      hash BLOB NOT NULL,
      base INTEGER NOT NULL,
      content_type TEXT NOT NULL,
      meta TEXT NOT NULL,
      updated_at INTEGER,
    )
  """)

  cursor.execute("""
    CREATE INDEX idx_resource_hash ON resources (base, hash)
  """)

register_table_creators(FRAMEWORK_DB, _create_tables)