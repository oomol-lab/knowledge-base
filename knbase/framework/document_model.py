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
from ..utils import chunks, fetchmany


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
    ) -> Generator[Document, None, None]:

    cursor.execute(
      "SELECT id, path, res_hash, meta FROM documents WHERE res_hash = ? AND preproc_module = ?",
      (resource_hash, self._ctx.model_id(preprocessing_module)),
    )
    for row in fetchmany(cursor):
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
          FROM tasks ORDER BY created_at LIMIT 1
        """
      )
    else:
      cursor.execute(
        """
          SELECT id, event, res_path, res_hash, res_model, from_res_hash, step, created_at
          FROM tasks WHERE id NOT IN ({}) ORDER BY created_at LIMIT 1
        """.format(
          ", ".join("?" for _ in unexpected_tasks_ids)
        ),
        unexpected_tasks_ids,
      )
    row = cursor.fetchone()
    if row is None:
      return None

    return self._build_task_with_row(cursor, row)

  def get_tasks(
      self,
      cursor: Cursor,
      resource_hash: bytes,
    ) -> Generator[Task, None, None]:

    cursor.execute(
      """
      SELECT id, event, res_path, res_hash, res_model, from_res_hash, step, created_at
      FROM tasks WHERE res_hash = ? ORDER BY id DESC
      """,
      (resource_hash,),
    )
    for row in fetchmany(cursor):
      yield self._build_task_with_row(cursor, row)

  def _build_task_with_row(self, cursor: Cursor, row: Any):
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
    cursor.execute(
      "SELECT id, preproc_module, created_at FROM preproc_tasks WHERE parent = ?",
      (task_id,),
    )
    for row in fetchmany(cursor):
      task_id, preproc_module, created_at = row
      task.preprocessing_tasks.append(
        PreprocessingTask(
          id=task_id,
          module=self._ctx.module(preproc_module),
          created_at=created_at,
        ),
      )
    cursor.execute(
      "SELECT id, document, operation, index_module, created_at FROM index_tasks WHERE parent = ?",
      (task_id,),
    )
    for row in fetchmany(cursor):
      task_id, document_id, operation, index_module, created_at = row
      task.index_tasks.append(
        IndexTask(
          id=task_id,
          document_id=document_id,
          module=self._ctx.module(index_module),
          operation=IndexTaskOperation(operation),
          created_at=created_at,
        ),
      )
    return task

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
        task: Task,
        index_modules: Iterable[IndexModule],
      ) -> Task:
    assert task.step == TaskStep.READY
    assert task.from_resource_hash is None

    created_at = int(time() * 1000)
    index_tasks: list[IndexTask] = []
    cursor.execute(
      "SELECT id FROM documents WHERE res_hash = ?",
      (task.resource_hash,),
    )
    for row in fetchmany(cursor):
      document_id = row[0]
      for index_module in index_modules:
        cursor.execute(
          "INSERT INTO index_tasks (parent, document, operation, index_module, created_at) VALUES (?, ?, ?, ?, ?)",
          (
            task.id,
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
            operation=IndexTaskOperation.REMOVE,
            created_at=created_at,
          ),
        )
    cursor.execute(
      "UPDATE tasks SET step = ? WHERE id = ?",
      (
        TaskStep.PROCESSING.value,
        task.id,
      ),
    )
    return Task(
      id=task.id,
      event_id=task.event_id,
      resource_path=task.resource_path,
      resource_hash=task.resource_hash,
      resource_module=task.resource_module,
      from_resource_hash=task.from_resource_hash,
      step=TaskStep.PROCESSING,
      preprocessing_tasks=[],
      index_tasks=index_tasks,
      created_at=task.created_at,
    )

  def go_to_preprocess(
        self,
        cursor: Cursor,
        task: Task,
        modules: Iterable[PreprocessingModule],
      ) -> Task:

    created_at = int(time() * 1000)
    preprocessing_tasks: list[PreprocessingTask] = []
    assert task.step == TaskStep.READY

    for module in modules:
      module_id = self._ctx.model_id(module)
      cursor.execute(
        "INSERT INTO preproc_tasks (parent, preproc_module, created_at) VALUES (?, ?, ?)",
        (
          task.id,
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
        task.id,
      ),
    )
    return Task(
      id=task.id,
      event_id=task.event_id,
      resource_path=task.resource_path,
      resource_hash=task.resource_hash,
      resource_module=task.resource_module,
      from_resource_hash=task.from_resource_hash,
      step=TaskStep.PROCESSING,
      preprocessing_tasks=preprocessing_tasks,
      index_tasks=[],
      created_at=task.created_at,
    )

  def complete_preprocess(
        self,
        cursor: Cursor,
        task: Task,
        preprocessing_task: PreprocessingTask,
        index_modules: Iterable[IndexModule],
        added_documents: Iterable[DocumentParams],
        removed_document_ids: Iterable[int],
      ) -> Task:

    created_at = int(time() * 1000)
    preprocessing_tasks: list[PreprocessingTask] = []
    index_tasks: list[IndexTask] = [*task.index_tasks]
    found_sub_task = False

    for sub_task in task.preprocessing_tasks:
      if sub_task.id == preprocessing_task.id:
        found_sub_task = True
      else:
        preprocessing_tasks.append(sub_task)

    assert found_sub_task, "Preprocessing task not found"

    for index_task in self._gen_added_index_tasks(
      cursor=cursor,
      task=task,
      created_at=created_at,
      preprocessing_module=preprocessing_task.module,
      index_modules=index_modules,
      added_documents=added_documents,
    ):
      index_tasks.append(index_task)

    for index_task in self._gen_removed_index_tasks(
      cursor=cursor,
      task=task,
      created_at=created_at,
      preprocessing_module=preprocessing_task.module,
      index_modules=index_modules,
      removed_document_ids=removed_document_ids,
    ):
      index_tasks.append(index_task)

    cursor.execute(
      "DELETE FROM preproc_tasks WHERE id = ?",
      (preprocessing_task.id,),
    )
    index_tasks.sort(key=lambda x: (x.operation.value, x.created_at))

    return Task(
      id=task.id,
      event_id=task.event_id,
      resource_path=task.resource_path,
      resource_hash=task.resource_hash,
      resource_module=task.resource_module,
      from_resource_hash=task.from_resource_hash,
      step=TaskStep.PROCESSING,
      preprocessing_tasks=preprocessing_tasks,
      index_tasks=index_tasks,
      created_at=task.created_at,
    )

  def complete_handle_index(
        self,
        cursor: Cursor,
        task: Task,
        index_tasks: Iterable[IndexTask],
      ) -> Task:

    index_tasks_ids = set(task.id for task in index_tasks)
    remain_index_tasks: list[IndexTask] = []
    completed_task_ids: list[int] = []
    to_remove_document_ids: set[int] = set()

    for index_task in task.index_tasks:
      if index_task.id not in index_tasks_ids:
        remain_index_tasks.append(index_task)
      else:
        completed_task_ids.append(index_task.id)

    for chunk_task_ids in chunks(completed_task_ids):
      question_marks = ", ".join("?" for _ in chunk_task_ids)
      cursor.execute(
        "SELECT document, operation FROM index_tasks WHERE id IN ({})".format(question_marks),
        chunk_task_ids,
      )
      for row in cursor.fetchall():
        document_id: int = row[0]
        operation = IndexTaskOperation(row[1])
        if operation == IndexTaskOperation.REMOVE:
          to_remove_document_ids.add(document_id)

      cursor.execute(
        "DELETE FROM index_tasks WHERE id IN ({})".format(question_marks),
        chunk_task_ids,
      )

    for document_ids in chunks(sorted(list(to_remove_document_ids))):
      cursor.execute(
        "DELETE FROM documents WHERE id IN ({})".format(
          ", ".join("?" for _ in document_ids),
        ),
        document_ids,
      )

    preprocess_tasks_count = self._sub_tasks_count(cursor, "preproc_tasks", task)
    index_tasks_count = self._sub_tasks_count(cursor, "index_tasks", task)

    new_task = Task(
      id=task.id,
      event_id=task.event_id,
      resource_path=task.resource_path,
      resource_hash=task.resource_hash,
      resource_module=task.resource_module,
      step=TaskStep.PROCESSING,
      from_resource_hash=task.from_resource_hash,
      preprocessing_tasks=task.preprocessing_tasks,
      index_tasks=remain_index_tasks,
      created_at=task.created_at,
    )
    if preprocess_tasks_count == 0 and index_tasks_count == 0:
      new_task.preprocessing_tasks = []
      new_task.index_tasks = []
      new_task.step = TaskStep.COMPLETED
      cursor.execute(
        "DELETE FROM tasks WHERE id = ?",
        (task.id,),
      )

    return new_task

  def _gen_added_index_tasks(
      self,
      cursor: Cursor,
      task: Task,
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
          task.resource_hash,
          preprocessing_module_id,
          dumps(document_params.meta),
        ),
      )
      document_id = cursor.lastrowid
      for index_module in index_modules:
        cursor.execute(
          "INSERT INTO index_tasks (parent, document, operation, index_module, created_at) VALUES (?, ?, ?, ?, ?)",
          (
            task.id,
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
      task: Task,
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
            task.id,
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
    )
  """)

  cursor.execute("""
    CREATE INDEX idx_index_task ON index_tasks (parent, created_at, operation)
  """)


register_table_creators(FRAMEWORK_DB, _create_tables)