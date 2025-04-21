from typing import Any, Iterable, Generator
from pathlib import Path
from enum import Enum
from sqlite3 import Cursor

from ..sqlite3_pool import SQLite3Pool
from ..module import Resource, Module, ResourceModule, PreprocessingModule, IndexModule
from .common import FRAMEWORK_DB
from .types import DocumentDescription, PreprocessingEvent, HandleIndexEvent
from .module_context import ModuleContext
from .knowledge_base_model import KnowledgeBase, KnowledgeBaseModel
from .resource_model import ResourceModel
from .document_model import Document, DocumentModel
from .task_model import TaskModel, PreprocessingTask, IndexTask, IndexTaskOperation


class StateMachineState(Enum):
  SETTING = 0
  SCANNING = 1
  PROCESSING = 2

class StateMachine:
  def __init__(self, db_path: Path, modules: Iterable[Module]):
    self._db: SQLite3Pool = SQLite3Pool(FRAMEWORK_DB, db_path)
    self._preproc_tasks: list[PreprocessingTask] = []
    self._index_tasks: list[IndexTask] = []
    self._removed_resource_hashes: list[bytes] = []

    with self._db.connect() as (cursor, conn):
      model_context = ModuleContext(cursor, modules)
      self._base_model: KnowledgeBaseModel = KnowledgeBaseModel(model_context)
      self._resource_model: ResourceModel = ResourceModel(model_context)
      self._document_model: DocumentModel = DocumentModel(model_context)
      self._task_model: TaskModel = TaskModel(model_context)
      self._state: StateMachineState = StateMachineState.SETTING

      self._load_tasks(cursor)
      conn.commit()

    if self._preproc_tasks or self._index_tasks:
      self._state = StateMachineState.PROCESSING

  @property
  def state(self) -> StateMachineState:
    return self._state

  def goto_setting(self) -> None:
    if self._state != StateMachineState.SETTING:
      assert not self._preproc_tasks, "preprocessing tasks are not empty"
      assert not self._index_tasks, "index tasks are not empty"
      self._state = StateMachineState.SETTING

  def goto_scanning(self) -> None:
    if self._state != StateMachineState.SCANNING:
      assert not self._preproc_tasks, "preprocessing tasks are not empty"
      assert not self._index_tasks, "index tasks are not empty"
      self._state = StateMachineState.SCANNING

  def goto_processing(self) -> None:
    if self._state != StateMachineState.PROCESSING:
      with self._db.connect() as (cursor, conn):
        self._load_tasks(cursor)
        self._state = StateMachineState.PROCESSING
        conn.commit()

  def _load_tasks(self, cursor: Cursor):
    self._preproc_tasks.clear()
    self._index_tasks.clear()

    for base in list(self._base_model.get_knowledge_bases(cursor)):
      self._preproc_tasks.extend(
        self._task_model.get_preproc_tasks(cursor, base),
      )
      self._index_tasks.extend(
        self._task_model.get_index_tasks(cursor, base),
      )

    self._preproc_tasks.sort(key=lambda x: (-x.created_at, -x.id))
    self._index_tasks.sort(key=lambda x: (-x.created_at, -x.id))

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

  def put_resource(
        self,
        event_id: int,
        resource: Resource,
        path: Path,
      ) -> None:

    assert self._state == StateMachineState.SCANNING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        target_last_refs = self._resource_hash_refs(
          cursor=cursor,
          knbase=resource.base,
          hash=resource.hash,
        )
        origin_resource = self._resource_model.get_resource(
          cursor=cursor,
          knbase=resource.base,
          resource_id=resource.id,
        )
        if origin_resource is None:
          self._resource_model.save_resource(cursor, resource)
        else:
          self._resource_model.update_resource(
            cursor=cursor,
            origin_resource=origin_resource,
            hash=resource.hash,
            content_type=resource.content_type,
            meta=resource.meta,
            updated_at=resource.updated_at,
          )
          if resource.hash != origin_resource.hash:
            origin_current_refs = self._resource_hash_refs(
              cursor=cursor,
              knbase=resource.base,
              hash=origin_resource.hash,
            )
            if origin_current_refs == 0:
              self._submit_resource_hash_removed(
                cursor=cursor,
                event_id=event_id,
                base=origin_resource.base,
                resource_hash=origin_resource.hash,
              )
        if target_last_refs == 0:
          self._submit_resource_hash_created(
            cursor=cursor,
            event_id=event_id,
            first_resource=resource,
            from_resource=origin_resource,
            path=path,
          )
        conn.commit()

      except BaseException as e:
        conn.rollback()
        raise e

  def remove_resource(self, event_id: int, resource: Resource) -> None:
    assert self._state == StateMachineState.SCANNING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        origin_resource = self._resource_model.get_resource(
          cursor=cursor,
          knbase=resource.base,
          resource_id=resource.id,
        )
        assert origin_resource is not None, f"Resource not found (id={resource.id})"
        self._resource_model.remove_resource(
          cursor=cursor,
          knbase=resource.base,
          resource_id=resource.id,
        )
        current_refs = self._resource_hash_refs(
          cursor=cursor,
          knbase=resource.base,
          hash=resource.hash,
        )
        if current_refs == 0:
          self._submit_resource_hash_removed(
            cursor=cursor,
            event_id=event_id,
            base=resource.base,
            resource_hash=resource.hash,
          )
        conn.commit()

      except BaseException as e:
        conn.rollback()
        raise e

  def pop_preproc_event(self) -> PreprocessingEvent | None:
    assert self._state == StateMachineState.PROCESSING
    if not self._preproc_tasks:
      return None

    task = self._preproc_tasks.pop()
    return PreprocessingEvent(
      proto_event_id=task.event_id,
      task_id=task.id,
      base=task.base,
      module=task.preproc_module,
      resource_hash=task.resource_hash,
      from_resource_hash=task.from_resource_hash,
      path=task.path,
      created_at=task.created_at,
    )

  def pop_handle_index_event(self) -> HandleIndexEvent | None:
    assert self._state == StateMachineState.PROCESSING
    if not self._index_tasks:
      return None

    task = self._index_tasks.pop()
    return HandleIndexEvent(
      proto_event_id=task.event_id,
      task_id=task.id,
      base=task.base,
      module=task.index_module,
      operation=task.operation,
      created_at=task.created_at,
    )

  def complete_preproc_task(
        self,
        event: PreprocessingEvent,
        document_descriptions: Iterable[DocumentDescription],
      ) -> None:

    assert self._state == StateMachineState.PROCESSING
    with self._db.connect() as (cursor, conn):
      try:
        task = self._task_model.get_preproc_task(
          cursor=cursor,
          base=event.base,
          task_id=event.task_id,
        )
        assert task is not None, f"Task not found (id={event.task_id})"
        self._task_model.remove_preproc_task(cursor, task)

        for descr in document_descriptions:
          document = self._document_model.append_document(
            cursor=cursor,
            preprocessing_module=task.preproc_module,
            base=task.base,
            resource_hash=task.resource_hash,
            document_hash=descr.hash,
            path=descr.path,
            meta=descr.meta,
          )
          for index_module in task.base.index_modules:
            last_task = next(
              self._task_model.get_index_tasks_of_document(
                cursor=cursor,
                index_module=index_module,
                document=document,
              ),
              None,
            )
            if last_task is None:
              self._task_model.create_index_task(
                cursor=cursor,
                event_id=task.event_id,
                index_module=index_module,
                base=task.base,
                document=document,
                operation=IndexTaskOperation.CREATE,
              )
            elif last_task.operation == IndexTaskOperation.REMOVE:
              # cancel each other out
              self._task_model.remove_index_task(cursor, last_task)

        for resource_hash in self._all_resource_hash(task):
          current_refs = self._resource_hash_refs(
            cursor=cursor,
            knbase=task.base,
            hash=resource_hash,
          )
          if current_refs == 0:
            self._submit_resource_hash_removed(
              cursor=cursor,
              event_id=task.event_id,
              base=task.base,
              resource_hash=resource_hash,
            )
        conn.commit()

      except BaseException as e:
        conn.rollback()
        raise e

  def complete_index_task(self, event: HandleIndexEvent) -> None:
    assert self._state == StateMachineState.PROCESSING
    with self._db.connect() as (cursor, conn):
      try:
        task = self._task_model.get_index_task(
          cursor=cursor,
          base=event.base,
          task_id=event.task_id,
        )
        assert task is not None, f"Task not found (id={event.task_id})"

        document = self._document_model.get_document(cursor, task.base, task.document_id)
        last_task = next(
          self._task_model.get_index_tasks_of_document(
            cursor=cursor,
            index_module=task,
            document=document,
          ),
          None,
        )
        should_check_document_refs = False
        if last_task is None:
          self._task_model.remove_index_task(cursor, task)
          if task.operation == IndexTaskOperation.CREATE:
            should_check_document_refs = True

        elif task.operation != last_task.operation: # cancel each other out
          self._task_model.remove_index_task(cursor, last_task)
          if last_task.operation == IndexTaskOperation.CREATE:
            should_check_document_refs = True

        if should_check_document_refs and \
           self._document_refs(cursor, document) == 0:
          self._document_model.remove_document(cursor, document)

        conn.commit()

      except BaseException as e:
        conn.rollback()
        raise e

  def _submit_resource_hash_created(
        self,
        cursor: Cursor,
        event_id: int,
        first_resource: Resource,
        from_resource: Resource | None,
        path: Path,
      ) -> None:

    for task in self._task_model.get_preproc_tasks(
        cursor=cursor,
        base=first_resource.base,
        resource_hash=first_resource.hash,
      ):
      self._task_model.remove_preproc_task(cursor, task)

    for preproc_module in first_resource.base.preproc_modules:
      self._task_model.create_preproc_task(
        cursor=cursor,
        event_id=event_id,
        preproc_module=preproc_module,
        base=first_resource.base,
        resource_hash=first_resource.hash,
        from_resource_hash=from_resource.hash if from_resource else None,
        path=path,
      )

    if first_resource.hash in self._removed_resource_hashes:
      self._removed_resource_hashes.remove(first_resource.hash)

  def _submit_resource_hash_removed(
        self,
        cursor: Cursor,
        event_id: int,
        base: KnowledgeBase,
        resource_hash: bytes,
      ) -> None:

    removed_documents_dict: dict[int, Document] = {}

    for preprocessing_module in base.preproc_modules:
      documents = list(self._document_model.get_documents(
        cursor=cursor,
        preprocessing_module=preprocessing_module,
        base=base,
        resource_hash=resource_hash,
      ))
      self._document_model.remove_references_from_resource(
        cursor=cursor,
        preprocessing_module=preprocessing_module,
        base=base,
        resource_hash=resource_hash,
      )
      for document in documents:
        if self._document_refs(cursor, document) == 0:
          removed_documents_dict[document.id] = document

    removed_documents = list(removed_documents_dict.values())
    removed_documents.sort(key=lambda d: d.id)
    index_modules = base.index_modules

    for document in removed_documents:
      if len(index_modules) == 0:
        self._document_model.remove_document(cursor, document)
      else:
        for index_module in index_modules:
          self._task_model.create_index_task(
            cursor=cursor,
            event_id=event_id,
            index_module=index_module,
            base=base,
            document=document,
            operation=IndexTaskOperation.REMOVE,
          )

    if resource_hash not in self._removed_resource_hashes:
      self._removed_resource_hashes.append(resource_hash)

  def _resource_hash_refs(self, cursor: Cursor, knbase: KnowledgeBase, hash: bytes) -> int:
    count: int = 0
    count += self._resource_model.count_resources(
      cursor=cursor,
      knbase=knbase,
      hash=hash,
    )
    count += self._task_model.count_resource_refs(
      cursor=cursor,
      base=knbase,
      resource_hash=hash,
    )
    return count

  def _document_refs(self, cursor: Cursor, document: Document) -> int:
    count: int = 0
    count += self._document_model.get_document_refs_count(
      cursor=cursor,
      document=document,
    )
    count += self._task_model.count_document_refs(
      cursor=cursor,
      document=document,
    )
    return count

  def _all_resource_hash(self, task: PreprocessingTask) -> Generator[bytes, None, None]:
    yield task.resource_hash
    if task.from_resource_hash is not None:
      yield task.from_resource_hash