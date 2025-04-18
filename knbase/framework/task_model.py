from __future__ import annotations

from dataclasses import dataclass
from time import time
from enum import Enum
from typing import Generator
from sqlite3 import Cursor
from pathlib import Path

from ..sqlite3_pool import register_table_creators
from ..module import KnowledgeBase
from ..utils import fetchmany
from .common import FRAMEWORK_DB
from .document_model import Document
from .module_context import ModuleContext, PreprocessingModule, IndexModule


@dataclass
class PreprocessingTask:
  id: int
  preproc_module: PreprocessingModule
  base: KnowledgeBase
  resource_hash: bytes
  from_resource_hash: bytes | None
  event_id: int
  path: Path
  created_at: int

@dataclass
class IndexTask:
  id: int
  event: int
  document_id: int
  index_module: IndexModule
  operation: IndexTaskOperation
  created_at: int

class IndexTaskOperation(Enum):
  CREATE = 0
  REMOVE = 1

class TaskModel:
  def __init__(self, modules_context: ModuleContext):
    self._ctx: ModuleContext = modules_context

  def get_preproc_tasks(self, cursor: Cursor, base: KnowledgeBase) -> Generator[PreprocessingTask, None, None]:
    cursor.execute(
      """
      SELECT id, preproc_module, res_hash, from_res_hash, event, path, created_at
      FROM preproc_tasks WHERE knbase = ? ORDER BY created_at, id DESC
      """,
      (base.id,),
    )
    for row in fetchmany(cursor):
      task_id, preproc_module_id, resource_hash, from_resource_hash, event_id, path, created_at = row
      preproc_module = self._ctx.module(preproc_module_id)
      yield PreprocessingTask(
        id=task_id,
        preproc_module=preproc_module,
        base=base,
        resource_hash=resource_hash,
        from_resource_hash=from_resource_hash,
        event_id=event_id,
        path=Path(path),
        created_at=created_at,
      )

  def get_index_tasks(self, cursor: Cursor) -> Generator[IndexTask, None, None]:
    cursor.execute(
      """
      SELECT id, index_module, document, operation, event, created_at
      FROM index_tasks ORDER BY created_at, id DESC
      """,
    )
    for row in fetchmany(cursor):
      task_id, index_module_id, document_id, operation_id, event_id, created_at = row
      yield IndexTask(
        id=task_id,
        index_module=self._ctx.module(index_module_id),
        document_id=document_id,
        operation=IndexTaskOperation(operation_id),
        event=event_id,
        created_at=created_at,
      )

  def create_preproc_task(
        self,
        cursor: Cursor,
        event_id: int,
        preproc_module: PreprocessingModule,
        base: KnowledgeBase,
        resource_hash: bytes,
        from_resource_hash: bytes | None,
        path: Path,
      ) -> PreprocessingTask:

    created_at = int(time() * 1000)
    cursor.execute(
      """
      INSERT INTO preproc_tasks (preproc_module, knbase, res_hash, from_res_hash, event, path, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      """,
      (
        self._ctx.module_id(preproc_module),
        base.id,
        resource_hash,
        from_resource_hash,
        event_id,
        str(path),
        created_at,
      ),
    )
    return PreprocessingTask(
      id=cursor.lastrowid,
      preproc_module=preproc_module,
      base=base,
      resource_hash=resource_hash,
      from_resource_hash=from_resource_hash,
      event_id=event_id,
      path=path,
      created_at=created_at,
    )

  def create_index_task(
        self,
        cursor: Cursor,
        event_id: int,
        index_module: IndexModule,
        document: Document,
        operation: IndexTaskOperation,
      ) -> IndexTask:

    created_at = int(time() * 1000)
    cursor.execute(
      """
      INSERT INTO index_tasks (index_module, document, operation, event, created_at)
      VALUES (?, ?, ?, ?, ?)
      """,
      (
        self._ctx.module_id(index_module),
        document.id,
        operation.value,
        event_id,
        created_at,
      ),
    )
    return IndexTask(
      id=cursor.lastrowid,
      index_module=index_module,
      document_id=document.id,
      operation=operation,
      event=event_id,
      created_at=created_at,
    )

  def remove_preproc_task(self, cursor: Cursor, preproc_task: PreprocessingTask) -> None:
    cursor.execute(
      "DELETE FROM preproc_tasks WHERE id = ?",
      (preproc_task.id,),
    )

  def remove_index_task(self, cursor: Cursor, index_task: IndexTask) -> None:
    cursor.execute(
      "DELETE FROM index_tasks WHERE id = ?",
      (index_task.id,),
    )

  def count_resource_refs(
        self,
        cursor: Cursor,
        preproc_module: PreprocessingModule,
        base: KnowledgeBase,
        resource_hash: bytes,
      ) -> int:

    count: int = 0
    for field in ("res_hash", "from_res_hash"):
      cursor.execute(
        f"SELECT COUNT(*) FROM preproc_tasks WHERE preproc_module = ?, knbase = ? AND {field} = ?",
        (
          self._ctx.module_id(preproc_module),
          base.id,
          resource_hash,
        ),
      )
      row = cursor.fetchone()
      if row is not None:
        count += row[0]

    return count

  def count_document_refs(self, cursor: Cursor, document: Document) -> int:
    count: int = 0
    cursor.execute(
      "SELECT COUNT(*) FROM index_tasks WHERE document = ?",
      (document.id,),
    )
    row = cursor.fetchone()
    if row is not None:
      count += row[0]
    return count

def _create_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE preproc_tasks (
      id INTEGER PRIMARY KEY,
      preproc_module INTEGER,
      knbase INTEGER,
      res_hash BLOB NOT NULL,
      from_res_hash BLOB NULL,
      event INTEGER NOT NULL,
      path TEXT NOT NULL,
      created_at INTEGER NOT NULL
    )
  """)

  cursor.execute("""
    CREATE INDEX idx_preproc_tasks ON preproc_tasks (preproc_module, knbase, res_hash)
  """)
  cursor.execute("""
    CREATE INDEX idx_from_preproc_tasks ON preproc_tasks (preproc_module, knbase, from_res_hash)
  """)
  cursor.execute("""
    CREATE INDEX idx_time_preproc_tasks ON preproc_tasks (knbase, created_at, id)
  """)

  cursor.execute("""
    CREATE TABLE index_tasks (
      id INTEGER PRIMARY KEY,
      index_module INTEGER NOT NULL,
      document INTEGER NOT NULL,
      operation INTEGER NOT NULL,
      event INTEGER NOT NULL,
      created_at INTEGER NOT NULL
    )
  """)

  cursor.execute("""
    CREATE INDEX idx_doc_index_task ON index_tasks (document)
  """)
  cursor.execute("""
    CREATE INDEX idx_time_index_task ON index_tasks (created_at, id)
  """)


register_table_creators(FRAMEWORK_DB, _create_tables)