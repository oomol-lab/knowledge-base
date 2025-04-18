from pathlib import Path
from enum import Enum
from sqlite3 import Cursor
from typing import Any, Iterable, Generator

from ..sqlite3_pool import SQLite3Pool
from ..module import (
  Module,
  Resource,
  ResourceModule,
  PreprocessingModule,
  IndexModule,
)
from .common import FRAMEWORK_DB
from .module_context import ModuleContext
from .knowledge_base_model import KnowledgeBase, KnowledgeBaseModel
from .resource_model import ResourceModel
from .document_model import DocumentModel, Task, TaskReason, IndexTask, IndexTaskOperation

class PreprocessEvent:
  task_id: int
  preproc_task_id: int
  resource_path: Path
  resource_hash: bytes
  resource_module: ResourceModule
  from_resource_hash: bytes | None
  preproc_module: PreprocessingModule
  created_at: int

class HandleIndexEvent:
  task_id: int
  index_task_id: int
  document_id: int
  resource_path: Path
  resource_hash: bytes
  resource_module: ResourceModule
  index_module: IndexModule
  operation: IndexTaskOperation
  created_at: int

class StateMachineState(Enum):
  SETTING = 0
  SCANNING = 1
  PROCESSING = 2

class StateMachine:
  def __init__(self, db_path: Path, modules: Iterable[Module]):
    self._db: SQLite3Pool = SQLite3Pool(FRAMEWORK_DB, db_path)
    with self._db.connect() as (cursor, conn):
      model_context = ModuleContext(cursor, modules)
      self._base_model: KnowledgeBaseModel = KnowledgeBaseModel(model_context)
      self._resource_model: ResourceModel = ResourceModel(model_context)
      self._document_model: DocumentModel = DocumentModel(model_context)
      self._preproc_events_queue: list[PreprocessEvent] = []
      self._index_events_queue: list[HandleIndexEvent] = []
      self._state: StateMachineState = StateMachineState.SETTING

      for task in self._document_model.get_tasks(cursor):
        self._register_preprocess_events(task)
        self._register_handle_index_events(task, task.index_tasks)
      conn.commit()

  def get_knowledge_bases(self) -> Generator[KnowledgeBase, None, None]:
    with self._db.connect() as (cursor, _):
      yield from self._base_model.get_knowledge_bases(cursor)

  def create_knowledge_base(
        self,
        resource_param: tuple[ResourceModule, Any],
        preproc_params: Iterable[tuple[PreprocessingModule, Any]],
        index_params: Iterable[tuple[IndexModule, Any]],
      ) -> KnowledgeBase:

    assert self._state == StateMachineState.SETTING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        resource_module, resource_params = resource_param
        records: Iterable[tuple[PreprocessingModule | IndexModule, Any]] = []
        for module, params in preproc_params:
          records.append((module, params))
        for module, params in index_params:
          records.append((module, params))

        base = self._base_model.create_knowledge_base(
          cursor=cursor,
          resource_module=resource_module,
          resource_params=resource_params,
          records=records,
        )
        conn.commit()
        return base

      except BaseException as e:
        conn.rollback()
        raise e

  def create_resource(
        self,
        base: KnowledgeBase,
        event_id: int,
        path: Path,
        resource: Resource,
      ) -> None:

    assert self._state == StateMachineState.SCANNING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        assert self._resource_model.get_resource(cursor, resource.base, resource.id) is None
        hash_refs = self._count_resource_hash(cursor, base, resource.hash)
        self._resource_model.save_resource(cursor, resource)
        if hash_refs == 0:
          self._submit_task_hash_created(
            cursor=cursor,
            event_id=event_id,
            reason=TaskReason.CREATE,
            base=base,
            resource_path=path,
            resource_hash=resource.hash,
            from_resource_hash=None,
          )
        conn.commit()

      except BaseException as e:
        conn.rollback()
        raise e

  def update_resource(
        self,
        base: KnowledgeBase,
        event_id: int,
        path: Path,
        resource: Resource,
      ) -> None:

    assert self._state == StateMachineState.SCANNING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        origin_resource = self._resource_model.get_resource(cursor, resource.base, resource.id)
        assert origin_resource is not None
        hash_refs = self._count_resource_hash(cursor, base, resource.hash)
        self._resource_model.update_resource(
          cursor=cursor,
          origin_resource=origin_resource,
          hash=resource.hash,
          content_type=resource.content_type,
          meta=resource.meta,
          updated_at=resource.updated_at,
        )
        if resource.hash != origin_resource.hash:
          if hash_refs == 0:
            self._submit_task_hash_created(
              cursor=cursor,
              event_id=event_id,
              reason=TaskReason.UPDATE,
              base=base,
              resource_path=path,
              resource_hash=resource.hash,
              from_resource_hash=origin_resource.hash,
            )
          if self._count_resource_hash(cursor, base, origin_resource.hash) == 0:
            self._submit_task_hash_removed(
              cursor=cursor,
              event_id=event_id,
              base=base,
              resource_hash=origin_resource.hash,
              resource_module=base.resource_module,
            )
        conn.commit()

      except BaseException as e:
        conn.rollback()
        raise e

  def remove_resource(
        self,
        base: KnowledgeBase,
        event_id: int,
        resource: Resource,
      ) -> None:

    assert self._state == StateMachineState.SCANNING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        assert self._resource_model.get_resource(cursor, resource.base, resource.id) is not None
        self._resource_model.remove_resource(cursor, resource.base, resource.id)
        if self._count_resource_hash(cursor, base, resource.hash) == 0:
          self._submit_task_hash_removed(
            event_id=event_id,
            cursor=cursor,
            base=base,
            resource_hash=resource.hash,
            resource_module=base.resource_module,
          )
        conn.commit()

      except BaseException as e:
        conn.rollback()
        raise e

  def _submit_task_hash_created(
        self,
        cursor: Cursor,
        event_id: int,
        reason: TaskReason.CREATE | TaskReason.UPDATE,
        base: KnowledgeBase,
        resource_path: Path,
        resource_hash: bytes,
        from_resource_hash: bytes | None,
      ) -> Task | None:

    last_task = self._last_defined_task(cursor, resource_hash)
    submit_reason: TaskReason.CREATE | TaskReason.UPDATE | None = None

    if last_task is None:
      submit_reason = reason
    else:
      if last_task.reason == TaskReason.CREATE:
        submit_reason = TaskReason.CREATE
      elif last_task.reason == TaskReason.UPDATE:
        submit_reason = TaskReason.UPDATE

      self._document_model.remove_task(cursor, last_task)
      self._remove_events_associated_with_task(last_task)

    if submit_reason is None:
      return

    task = self._document_model.create_task(
      cursor=cursor,
      event_id=event_id,
      resource_path=resource_path,
      resource_hash=resource_hash,
      resource_module=base.resource_module,
      from_resource_hash=from_resource_hash,
    )
    task = self._document_model.go_to_preprocess(
      cursor=cursor,
      task=task,
      reason=submit_reason,
      preproc_modules=base.preproc_modules,
    )
    self._register_preprocess_events(task)
    return task

  def _submit_task_hash_removed(
      self,
      cursor: Cursor,
      event_id: int,
      base: KnowledgeBase,
      resource_hash: bytes,
      resource_module: ResourceModule,
    ) -> Task | None:

    task: Task | None = None
    last_task = self._last_defined_task(cursor, resource_hash)

    if last_task is not None:
      # If creation and deletion meet, they will be canceled directly.
      # Otherwise, it means there is a deletion task, so no operation is required.
      if last_task.reason in (TaskReason.CREATE, TaskReason.UPDATE):
        self._document_model.remove_task(cursor, last_task)
        self._remove_events_associated_with_task(last_task)

    else:
      task = self._document_model.create_task(
        cursor=cursor,
        event_id=event_id,
        resource_path=Path(),
        resource_hash=resource_hash,
        resource_module=resource_module,
        from_resource_hash=None,
      )
      task = self._document_model.go_to_remove(
        cursor=cursor,
        task=task,
        index_modules=base.index_modules,
      )
      self._register_preprocess_events(task)

    return task

  def _remove_events_associated_with_task(self, task: Task) -> None:
    self._preproc_events_queue = [
      event for event in self._preproc_events_queue
      if event.task_id != task.id
    ]
    self._index_events_queue = [
      event for event in self._index_events_queue
      if event.task_id != task.id
    ]

  def _register_preprocess_events(self, task: Task) -> None:
    for preproc_task in task.preprocessing_tasks:
      self._preproc_events_queue.append(PreprocessEvent(
        task_id=task.id,
        preproc_task_id=preproc_task.id,
        resource_path=task.resource_path,
        resource_hash=task.resource_hash,
        resource_module=task.resource_module,
        from_resource_hash=task.from_resource_hash,
        preproc_module=preproc_task.module,
        created_at=preproc_task.created_at,
      ))

  def _register_handle_index_events(self, task: Task, added_index_tasks: list[IndexTask]) -> None:
    for index_task in added_index_tasks:
      self._index_events_queue.append(HandleIndexEvent(
        task_id=task.id,
        index_task_id=index_task.id,
        document_id=index_task.document_id,
        resource_path=task.resource_path,
        resource_hash=task.resource_hash,
        resource_module=task.resource_module,
        index_module=index_task.module,
        operation=index_task.operation,
        created_at=index_task.created_at,
      ))

  def _count_resource_hash(self, cursor: Cursor, knbase: KnowledgeBase, hash: bytes) -> int:
    count: int = 0
    count += self._resource_model.count_resources(
      cursor=cursor,
      knbase=knbase,
      hash=hash,
    )
    count += self._document_model.count_resource_hash_refs(
      cursor=cursor,
      resource_hash=hash,
    )
    return count

  def _last_defined_task(self, cursor: Cursor, resource_hash: bytes) -> None | Task:
    for task in self._document_model.get_tasks(cursor, resource_hash):
      if task.reason != TaskReason.UNDEFINED:
        return task
    return None
