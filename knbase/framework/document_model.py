from __future__ import annotations

from dataclasses import dataclass
from time import time
from enum import Enum
from json import loads, dumps
from typing import Any, Generator, Iterable
from sqlite3 import Cursor
from pathlib import Path

from .common import FRAMEWORK_DB
from .module_context import ModuleContext, ResourceModule, PreprocessingModule, IndexModule
from ..sqlite3_pool import register_table_creators
from ..utils import fetchmany


_CHUNK_SIZE = 36

@dataclass
class Document:
  id: int
  path: Path
  hash: bytes
  preprocessing_module: PreprocessingModule
  meta: Any

@dataclass
class DocumentParams:
  path: Path
  meta: Any

@dataclass
class Task:
  id: int
  event_id: int
  resource_path: Path
  resource_hash: bytes
  resource_module: ResourceModule
  from_resource_hash: bytes | None
  step: TaskStep
  preprocessing_tasks: list[PreprocessingTask]
  index_tasks: list[IndexTask]
  created_at: int

class TaskStep(Enum):
  READY = 0
  PROCESSING = 1
  COMPLETED = 2

@dataclass
class PreprocessingTask:
  id: int
  module: PreprocessingModule
  created_at: int

@dataclass
class IndexTask:
  id: int
  document_id: int
  module: IndexModule
  operation: IndexTaskOperation
  created_at: int

class IndexTaskOperation(Enum):
  CREATE = 0
  REMOVE = 1

class DocumentModel:
  def __init__(self, modules_context: ModuleContext):
    self._ctx: ModuleContext = modules_context

  def get_documents(
      self,
      cursor: Cursor,
      resource_hash: bytes,
      preprocessing_module: PreprocessingModule,
    ):

    cursor.execute(
      "SELECT id, path, res_hash, meta FROM documents WHERE res_hash = ? AND preproc_module = ?",
      (resource_hash, self._ctx.model_id(preprocessing_module)),
    )
    for row in fetchmany(cursor, _CHUNK_SIZE):
      document_id, path, res_hash, meta_text = row
      yield Document(
        id=document_id,
        path=Path(path),
        hash=res_hash,
        preprocessing_module=preprocessing_module,
        meta=loads(meta_text),
      )

  def get_task(
      self,
      cursor: Cursor,
      unexpected_tasks: Iterable[Task] = (),
    ) -> None | Task:

    unexpected_tasks_ids = list(task.id for task in unexpected_tasks)
    if len(unexpected_tasks_ids) == 0:
      cursor.execute(
        """
          SELECT id, event, res_path, res_hash, res_model, from_res_hash, step, created_at
          FROM tasks LIMIT 1 ORDER BY created_at
        """
      )
    else:
      cursor.execute(
        """
          SELECT id, event, res_path, res_hash, res_model, from_res_hash, step, created_at
          FROM tasks WHERE id NOT IN ({}) LIMIT 1 ORDER BY created_at
        """.format(
          ", ".join("?" for _ in unexpected_tasks_ids)
        ),
        unexpected_tasks_ids,
      )
    row = cursor.fetchone()
    if row is None:
      return None

    task_id, event_id, resource_path, resource_hash, resource_model, from_res_hash, step, created_at = row
    task = Task(
      id=task_id,
      event_id=event_id,
      resource_path=Path(resource_path),
      resource_hash=resource_hash,
      resource_module=self._ctx.module(resource_model),
      from_resource_hash=from_res_hash,
      step=TaskStep(step),
      preprocessing_tasks=[],
      index_tasks=[],
      created_at=created_at,
    )
    return task

  def get_tasks(
      self,
      cursor: Cursor,
      resource_hash: bytes,
    ) -> Generator[Task, None, None]:

    for field in ("res_hash", "from_res_hash"):
      cursor.execute(
        "SELECT id, event, res_path, res_hash, res_model, from_res_hash, step, created_at FROM tasks WHERE {} = ?".format(field),
        (resource_hash,),
      )
      for row in fetchmany(cursor, _CHUNK_SIZE):
        task_id, event_id, resource_path, resource_hash, resource_model, from_res_hash, step, created_at = row
        yield Task(
          id=task_id,
          event_id=event_id,
          resource_path=Path(resource_path),
          resource_hash=resource_hash,
          resource_module=self._ctx.module(resource_model),
          from_resource_hash=from_res_hash,
          step=TaskStep(step),
          preprocessing_tasks=[],
          index_tasks=[],
          created_at=created_at,
        )

  def create_task(
        self,
        cursor: Cursor,
        event_id: int,
        resource_path: Path,
        resource_hash: bytes,
        resource_module: ResourceModule,
        from_resource_hash: bytes | None = None,
      ) -> Task:

    step = TaskStep.READY
    created_at = int(time() * 1000)
    cursor.execute(
      "INSERT INTO tasks (event, res_path, res_hash, res_model, from_res_hash, step, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
      (
        event_id,
        str(resource_path),
        resource_hash,
        self._ctx.model_id(resource_module),
        from_resource_hash,
        step.value,
        created_at,
      ),
    )
    task_id = cursor.lastrowid
    return Task(
      id=task_id,
      event_id=event_id,
      resource_path=resource_path,
      resource_hash=resource_hash,
      resource_module=resource_module,
      from_resource_hash=from_resource_hash,
      step=step,
      preprocessing_tasks=[],
      index_tasks=[],
      created_at=created_at,
    )

  def remove_task(self, cursor: Cursor, task: Task) -> None:
    cursor.execute(
      "DELETE FROM tasks WHERE id = ?",
      (task.id,),
    )

  def go_to_remove(
        self,
        cursor: Cursor,
        origin: Task,
        index_modules: Iterable[IndexModule],
      ) -> None:
    assert origin.step == TaskStep.READY
    assert origin.from_resource_hash is None

    created_at = int(time() * 1000)
    index_tasks: list[IndexTask] = []
    cursor.execute(
      "SELECT id FROM documents WHERE res_hash = ?",
      (origin.resource_hash,),
    )
    for row in fetchmany(cursor, _CHUNK_SIZE):
      document_id = row[0]
      for index_module in index_modules:
        cursor.execute(
          "INSERT INTO index_tasks (parent, document, operation, index_module, created_at) VALUES (?, ?, ?, ?, ?)",
          (
            origin.id,
            document_id,
            IndexTaskOperation.REMOVE.value,
            self._ctx.model_id(index_module),
            created_at,
          ),
        )
        index_tasks.append(
          IndexTask(
            id=cursor.lastrowid,
            document_id=document_id,
            module=index_module,
            operation=IndexTaskOperation.CREATE,
            created_at=created_at,
          ),
        )
    cursor.execute(
      "UPDATE tasks SET step = ? WHERE id = ?",
      (
        TaskStep.PROCESSING.value,
        origin.id,
      ),
    )
    return Task(
      id=origin.id,
      event_id=origin.event_id,
      resource_path=origin.resource_path,
      resource_hash=origin.resource_hash,
      resource_module=origin.resource_module,
      from_resource_hash=origin.from_resource_hash,
      step=TaskStep.PROCESSING,
      preprocessing_tasks=[],
      index_tasks=index_tasks,
      created_at=origin.created_at,
    )

  def go_to_preprocess(
        self,
        cursor: Cursor,
        origin: Task,
        modules: Iterable[PreprocessingModule],
      ) -> Task:

    created_at = int(time() * 1000)
    preprocessing_tasks: list[PreprocessingTask] = []
    assert origin.step == TaskStep.READY

    for module in modules:
      module_id = self._ctx.model_id(module)
      cursor.execute(
        "INSERT INTO preproc_tasks (parent, preproc_module, created_at) VALUES (?, ?, ?)",
        (
          origin.id,
          module_id,
          created_at,
        ),
      )
      task_id = cursor.lastrowid
      preprocessing_tasks.append(
        PreprocessingTask(
          id=task_id,
          module=module,
          created_at=created_at,
        ),
      )
    cursor.execute(
      "UPDATE tasks SET step = ? WHERE id = ?",
      (
        TaskStep.PROCESSING.value,
        origin.id,
      ),
    )
    return Task(
      id=origin.id,
      event_id=origin.event_id,
      resource_path=origin.resource_path,
      resource_hash=origin.resource_hash,
      resource_module=origin.resource_module,
      from_resource_hash=origin.from_resource_hash,
      step=TaskStep.PROCESSING,
      preprocessing_tasks=preprocessing_tasks,
      index_tasks=[],
      created_at=origin.created_at,
    )

  def complete_preprocess(
        self,
        cursor: Cursor,
        origin: Task,
        preprocessing_task: PreprocessingTask,
        index_modules: Iterable[IndexModule],
        added_documents: Iterable[DocumentParams],
        removed_document_ids: Iterable[int],
      ) -> Task:

    created_at = int(time() * 1000)
    preprocessing_tasks: list[PreprocessingTask] = []
    index_tasks: list[IndexTask] = [*origin.index_tasks]
    found_sub_task = False

    for sub_task in origin.preprocessing_tasks:
      if sub_task.id != preprocessing_task.id:
        found_sub_task = True
      else:
        preprocessing_tasks.append(sub_task)

    assert found_sub_task, "Preprocessing task not found"

    for index_task in self._gen_added_index_tasks(
      cursor=cursor,
      origin=origin,
      created_at=created_at,
      preprocessing_module=preprocessing_task.module,
      index_modules=index_modules,
      added_documents=added_documents,
    ):
      index_tasks.append(index_task)

    for index_task in self._gen_removed_index_tasks(
      cursor=cursor,
      origin=origin,
      created_at=created_at,
      preprocessing_module=preprocessing_task.module,
      index_modules=index_modules,
      removed_document_ids=removed_document_ids,
    ):
      index_tasks.append(index_task)

    index_tasks.sort(key=lambda x: (x.operation.value, x.created_at))

    return Task(
      id=origin.id,
      event_id=origin.event_id,
      resource_path=origin.resource_path,
      resource_hash=origin.resource_hash,
      resource_module=origin.resource_module,
      from_resource_hash=origin.from_resource_hash,
      step=TaskStep.PROCESSING,
      preprocessing_tasks=preprocessing_tasks,
      index_tasks=index_tasks,
      created_at=origin.created_at,
    )

  def complete_handle_index(
        self,
        cursor: Cursor,
        origin: Task,
        index_tasks: Iterable[IndexTask],
      ) -> Task:

    index_tasks_ids = set(task.id for task in index_tasks)
    remain_index_tasks: list[IndexTask] = []
    completed_task_ids: list[int] = []

    for index_task in origin.index_tasks:
      if index_task.id not in index_tasks_ids:
        remain_index_tasks.append(index_task)
      else:
        completed_task_ids.append(index_task.id)

    for i in range(0, len(completed_task_ids), _CHUNK_SIZE):
      for chunk_task_ids in completed_task_ids[i : i + _CHUNK_SIZE]:
        cursor.execute(
          "REMOVE FROM index_tasks WHERE id IN ({})".format(
            ", ".join("?" for _ in chunk_task_ids)
          ),
          chunk_task_ids,
        )

    preprocess_tasks_count = self._sub_tasks_count(cursor, "preproc_tasks", origin)
    index_tasks_count = self._sub_tasks_count(cursor, "index_tasks", origin)

    task = Task(
      id=origin.id,
      event_id=origin.event_id,
      resource_path=origin.resource_path,
      resource_hash=origin.resource_hash,
      resource_module=origin.resource_module,
      step=TaskStep.PROCESSING,
      from_resource_hash=origin.from_resource_hash,
      preprocessing_tasks=origin.preprocessing_tasks,
      index_tasks=remain_index_tasks,
      created_at=origin.created_at,
    )
    if preprocess_tasks_count == 0 and index_tasks_count == 0:
      task.preprocessing_tasks = []
      task.index_tasks = []
      task.step = TaskStep.COMPLETED
      cursor.execute(
        "REMOVE FROM tasks WHERE id = ?",
        (origin.id,),
      )

    return task

  def _gen_added_index_tasks(
      self,
      cursor: Cursor,
      origin: Task,
      created_at: int,
      preprocessing_module: PreprocessingModule,
      index_modules: Iterable[IndexModule],
      added_documents: Iterable[DocumentParams],
    ) -> Generator[IndexTask, None, None]:

    preprocessing_module_id = self._ctx.model_id(preprocessing_module)

    for document_params in added_documents:
      cursor.execute(
        "INSERT INTO documents (path, res_hash, preproc_module, meta) VALUES (?, ?, ?, ?)",
        (
          str(document_params.path),
          origin.resource_hash,
          preprocessing_module_id,
          dumps(document_params.meta),
        ),
      )
      document_id = cursor.lastrowid
      for index_module in index_modules:
        cursor.execute(
          "INSERT INTO index_tasks (parent, document, operation, index_module, created_at) VALUES (?, ?, ?, ?, ?)",
          (
            origin.id,
            document_id,
            IndexTaskOperation.CREATE.value,
            self._ctx.model_id(index_module),
            created_at,
          ),
        )
        index_task_id = cursor.lastrowid
        yield IndexTask(
          id=index_task_id,
          document_id=document_id,
          module=index_module,
          operation=IndexTaskOperation.CREATE,
          created_at=created_at,
        )

  def _gen_removed_index_tasks(
      self,
      cursor: Cursor,
      origin: Task,
      created_at: int,
      preprocessing_module: PreprocessingModule,
      index_modules: Iterable[IndexModule],
      removed_document_ids: Iterable[int],
  ) -> Generator[IndexTask, None, None]:

    for removed_document_id in removed_document_ids:
      for index_module in index_modules:
        cursor.execute(
          "INSERT INTO index_tasks (parent, document, operation, index_module, created_at) VALUES (?, ?, ?, ?, ?)",
          (
            origin.id,
            removed_document_id,
            IndexTaskOperation.REMOVE.value,
            self._ctx.model_id(preprocessing_module),
            created_at,
          ),
        )
        index_task_id = cursor.lastrowid
        yield IndexTask(
          id=index_task_id,
          document_id=removed_document_id,
          module=index_module,
          operation=IndexTaskOperation.REMOVE,
          created_at=created_at,
        )

    for removed_document_id in removed_document_ids:
      cursor.execute(
        "DELETE FROM documents WHERE id = ?",
        (removed_document_id,),
      )

  def _sub_tasks_count(self, cursor: Cursor, table_name: str, parent: Task):
    cursor.execute(
      f"SELECT count(*) FROM {table_name} WHERE parent = ?",
      (parent.id,),
    )
    row = cursor.fetchone()
    if row is None:
      return 0
    else:
      return row[0]


def _create_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE documents (
      id INTEGER PRIMARY KEY,
      path TEXT NOT NULL,
      res_hash BLOB NOT NULL,
      preproc_module INTEGER NOT NULL,
      meta TEXT NOT NULL
    )
  """)

  cursor.execute("""
    CREATE INDEX idx_document ON documents (res_hash, preproc_module)
  """)

  cursor.execute("""
    CREATE TABLE tasks (
      id INTEGER PRIMARY KEY,
      event INTEGER NOT NULL,
      res_path TEXT NOT NULL,
      res_hash BLOB NOT NULL,
      res_model INTEGER NOT NULL,
      from_res_hash BLOB NULL,
      step INTEGER NOT NULL,
      created_at INTEGER NOT NULL
    )
  """)

  cursor.execute("""
    CREATE INDEX idx_task ON tasks (created_at)
  """)
  cursor.execute("""
    CREATE INDEX idx_res_task ON tasks (res_hash)
  """)
  cursor.execute("""
    CREATE INDEX idx_f_res_task ON tasks (from_res_hash)
  """)

  cursor.execute("""
    CREATE TABLE preproc_tasks (
      id INTEGER PRIMARY KEY,
      parent INTEGER NOT NULL,
      preproc_module INTEGER NOT NULL,
      created_at INTEGER NOT NULL
    )
  """)

  cursor.execute("""
    CREATE INDEX idx_preproc_task ON preproc_tasks (parent, created_at)
  """)

  cursor.execute("""
    CREATE TABLE index_tasks (
      id INTEGER PRIMARY KEY,
      parent INTEGER NOT NULL,
      document INTEGER NOT NULL,
      operation INTEGER NOT NULL,
      index_module INTEGER NOT NULL,
      created_at INTEGER NOT NULL
  """)

  cursor.execute("""
    CREATE INDEX idx_index_task ON index_tasks (parent, created_at, operation)
  """)


register_table_creators(FRAMEWORK_DB, _create_tables)