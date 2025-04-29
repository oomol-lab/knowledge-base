from typing import Iterable, Generator
from pathlib import Path
from enum import Enum
from sqlite3 import Cursor

from ..sqlite3_pool import SQLite3Pool
from ..module import T, R, Resource, Module, ResourceModule, PreprocessingModule, IndexModule
from .common import FRAMEWORK_DB
from .types import DocumentDescription, PreprocessingEvent, HandleIndexEvent, RemovedResourceEvent
from .module_context import ModuleContext
from .knowledge_base_model import KnowledgeBase, KnowledgeBaseModel
from .resource_model import ResourceModel
from .document_model import Document, DocumentModel
from .task_model import TaskModel, FromResource, PreprocessingTask, IndexTask, IndexTaskOperation


class StateMachineState(Enum):
  SETTING = 0
  SCANNING = 1
  PROCESSING = 2

class StateMachine:
  def __init__(self, db_path: Path, modules: Iterable[Module]):
    self._db: SQLite3Pool = SQLite3Pool(FRAMEWORK_DB, db_path)
    self._preproc_tasks: list[PreprocessingTask] = []
    self._preproc_tasks_pop_count: int = 0
    self._index_tasks: list[IndexTask] = []
    self._index_tasks_pop_count: int = 0
    self._removed_resource_events: list[RemovedResourceEvent] = []

    with self._db.connect() as (cursor, conn):
      model_context = ModuleContext(cursor, modules)
      self._base_model: KnowledgeBaseModel = KnowledgeBaseModel(model_context)
      self._resource_model: ResourceModel = ResourceModel(model_context)
      self._document_model: DocumentModel = DocumentModel(model_context)
      self._task_model: TaskModel = TaskModel(model_context)
      self._state: StateMachineState = StateMachineState.SETTING

      self._load_tasks(cursor)
      conn.commit()

    self._modules: dict[str, Module] = dict(
      (module.id, module) for module in modules
    )
    if self._preproc_tasks or self._index_tasks:
      self._state = StateMachineState.PROCESSING

  @property
  def state(self) -> StateMachineState:
    return self._state

  def goto_setting(self) -> None:
    if self._state != StateMachineState.SETTING:
      self._assert_not_preprocessing()
      self._state = StateMachineState.SETTING

  def goto_scanning(self) -> None:
    if self._state != StateMachineState.SCANNING:
      self._assert_not_preprocessing()
      self._state = StateMachineState.SCANNING

  def goto_processing(self) -> None:
    if self._state != StateMachineState.PROCESSING:
      with self._db.connect() as (cursor, conn):
        self._load_tasks(cursor)
        self._state = StateMachineState.PROCESSING
        conn.commit()

  def _assert_not_preprocessing(self) -> None:
    assert not self._preproc_tasks, "preprocessing tasks are not empty"
    assert not self._index_tasks, "index tasks are not empty"
    assert self._preproc_tasks_pop_count == 0, "there are popped preprocessing tasks"
    assert self._index_tasks_pop_count == 0, "there are popped index tasks"

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

    self._preproc_tasks.sort(key=lambda x: (x.created_at, x.id))
    self._index_tasks.sort(key=lambda x: (x.created_at, x.id))

  def get_knowledge_base(self, id: int) -> KnowledgeBase:
    with self._db.connect() as (cursor, _):
      return self._base_model.get_knowledge_base(cursor, id)

  def get_knowledge_bases(self) -> Generator[KnowledgeBase, None, None]:
    with self._db.connect() as (cursor, _):
      yield from self._base_model.get_knowledge_bases(cursor)

  def create_knowledge_base(self, resource_param: tuple[ResourceModule[T, R], T]) -> KnowledgeBase[T, R]:
    assert self._state == StateMachineState.SETTING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        resource_module, resource_params = resource_param
        base = self._base_model.create_knowledge_base(
          cursor=cursor,
          resource_module=resource_module,
          resource_params=resource_params,
        )
        conn.commit()
        return base

      except BaseException as e:
        conn.rollback()
        raise e

  def remove_knowledge_base(self, base: KnowledgeBase) -> None:
    assert self._state == StateMachineState.SETTING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        if next()(
          self._resource_model.list_resource_hashes(cursor, base),
          None,
        ) is not None:
          raise ValueError(f"Cannot remove knowledge base {base.id} because it contains resources")
        self._base_model.remove_knowledge_base(cursor, base)
        conn.commit()

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
                resource_content_type=origin_resource.content_type,
              )
        if target_last_refs == 0:
          self._submit_resource_hash_created(
            cursor=cursor,
            event_id=event_id,
            first_resource=resource,
            from_resource=origin_resource,
            path=path,
            content_type=resource.content_type,
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
            resource_content_type=resource.content_type,
          )
        conn.commit()

      except BaseException as e:
        conn.rollback()
        raise e

  def clean_resources(self, event_id: int, base: KnowledgeBase) -> None:
    assert self._state == StateMachineState.SETTING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        for resource_hash in self._resource_model.list_resource_hashes(cursor, base):
          resource = next(self._resource_model.get_resources(
            cursor=cursor,
            knbase=base,
            hash=resource_hash,
          ))
          self._submit_resource_hash_removed(
            cursor=cursor,
            event_id=event_id,
            base=base,
            resource_hash=resource_hash,
            resource_content_type=resource.content_type,
          )
        self._resource_model.remove_resources(cursor, base)
        conn.commit()
        self._state = StateMachineState.PROCESSING
        return base

      except BaseException as e:
        conn.rollback()
        raise e

  def pop_preproc_event(self) -> PreprocessingEvent | None:
    assert self._state == StateMachineState.PROCESSING
    if not self._preproc_tasks:
      return None

    task = self._preproc_tasks.pop(0)
    self._preproc_tasks_pop_count += 1

    return PreprocessingEvent(
      proto_event_id=task.event_id,
      task_id=task.id,
      base=task.base,
      module=task.preproc_module,
      resource_hash=task.resource_hash,
      from_resource_hash=task.from_resource.hash if task.from_resource else None,
      resource_content_type=task.content_type,
      resource_path=task.path,
      created_at=task.created_at,
    )

  def pop_handle_index_event(self) -> HandleIndexEvent | None:
    assert self._state == StateMachineState.PROCESSING
    if not self._index_tasks:
      return None

    with self._db.connect() as (cursor, _):
      task = self._index_tasks.pop(0)
      self._index_tasks_pop_count += 1
      document = self._document_model.get_document(
        cursor=cursor,
        base=task.base,
        id=task.document_id,
      )

    return HandleIndexEvent(
      proto_event_id=task.event,
      task_id=task.id,
      base=task.base,
      module=task.index_module,
      operation=task.operation,
      document_hash=document.document_hash,
      document_path=document.path,
      document_meta=document.meta,
      created_at=task.created_at,
    )

  def pop_removed_resource_event(self) -> RemovedResourceEvent | None:
    if not self._removed_resource_events:
      return None
    return self._removed_resource_events.pop(0)

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
          for index_module in self._index_modules(task.base):
            last_task = next(
              self._task_model.get_index_tasks_of_document(
                cursor=cursor,
                index_module=index_module,
                document=document,
              ),
              None,
            )
            if last_task is None:
              index_task = self._task_model.create_index_task(
                cursor=cursor,
                event_id=task.event_id,
                index_module=index_module,
                base=task.base,
                document=document,
                operation=IndexTaskOperation.CREATE,
              )
              self._index_tasks.append(index_task)

            elif last_task.operation == IndexTaskOperation.REMOVE:
              # cancel each other out
              self._task_model.remove_index_task(cursor, last_task)

        for resource_hash, resource_content_type in self._hash_and_content_type_of(task):
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
              resource_content_type=resource_content_type,
            )

        self._preproc_tasks_pop_count -= 1
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
        self._task_model.remove_index_task(cursor, task)

        if task.operation == IndexTaskOperation.CREATE and \
           self._document_refs(cursor, document) == 0:
          self._document_model.remove_document(cursor, document)

        self._index_tasks_pop_count -= 1
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
        content_type: str,
      ) -> None:

    for task in self._task_model.get_preproc_tasks(
        cursor=cursor,
        base=first_resource.base,
        resource_hash=first_resource.hash,
      ):
      self._task_model.remove_preproc_task(cursor, task)

    for preproc_module in self._preprocess_modules(
      base=first_resource.base,
      content_type=first_resource.content_type
    ):
      task_from_resource: FromResource | None = None
      if from_resource is not None:
        task_from_resource = FromResource(
          hash=from_resource.hash,
          content_type=from_resource.content_type,
        )
      preproc_task = self._task_model.create_preproc_task(
        cursor=cursor,
        event_id=event_id,
        preproc_module=preproc_module,
        base=first_resource.base,
        resource_hash=first_resource.hash,
        from_resource=task_from_resource,
        path=path,
        content_type=content_type,
      )
      self._preproc_tasks.append(preproc_task)

    for i, event in enumerate(self._removed_resource_events):
      if event.hash == first_resource.hash:
        self._removed_resource_events.pop(i)
        break

  def _submit_resource_hash_removed(
        self,
        cursor: Cursor,
        event_id: int,
        base: KnowledgeBase,
        resource_hash: bytes,
        resource_content_type: str,
      ) -> None:

    removed_documents_dict: dict[int, Document] = {}

    for preproc_module in self._preprocess_modules(
      base=base,
      content_type=resource_content_type,
    ):
      documents = list(self._document_model.get_documents(
        cursor=cursor,
        preprocessing_module=preproc_module,
        base=base,
        resource_hash=resource_hash,
      ))
      self._document_model.remove_references_from_resource(
        cursor=cursor,
        preprocessing_module=preproc_module,
        base=base,
        resource_hash=resource_hash,
      )
      for document in documents:
        if self._document_refs(cursor, document) == 0:
          removed_documents_dict[document.id] = document

    removed_documents = list(removed_documents_dict.values())
    removed_documents.sort(key=lambda d: d.id)
    index_modules = list(self._index_modules(base))

    for document in removed_documents:
      if len(index_modules) == 0:
        self._document_model.remove_document(cursor, document)
      else:
        for index_module in index_modules:
          index_task = self._task_model.create_index_task(
            cursor=cursor,
            event_id=event_id,
            index_module=index_module,
            base=base,
            document=document,
            operation=IndexTaskOperation.REMOVE,
          )
          self._index_tasks.append(index_task)

    if all(e.hash != resource_hash for e in self._removed_resource_events):
      self._removed_resource_events.append(RemovedResourceEvent(
        proto_event_id=event_id,
        hash=resource_hash,
        base=base,
      ))

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

  def _preprocess_modules(self, base: KnowledgeBase, content_type: str) -> Generator[PreprocessingModule, None, None]:
    for id in base.resource_module.preprocess_module_ids(
      base=base,
      content_type=content_type,
    ):
      preproc_module = self._modules.get(id, None)
      if preproc_module is not None and \
         isinstance(preproc_module, PreprocessingModule):
        yield preproc_module

  def _index_modules(self, base: KnowledgeBase) -> Generator[IndexModule, None, None]:
    for id in base.resource_module.index_module_ids(base):
      index_module = self._modules.get(id, None)
      if index_module is not None and \
         isinstance(index_module, IndexModule):
        yield index_module

  def _hash_and_content_type_of(self, task: PreprocessingTask) -> Generator[tuple[bytes, str], None, None]:
    yield task.resource_hash, task.content_type
    from_resource = task.from_resource
    if from_resource is not None:
      yield from_resource.hash, from_resource.content_type