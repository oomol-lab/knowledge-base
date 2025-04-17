from pathlib import Path
from enum import Enum
from typing import Any, Iterable, Generator

from ..sqlite3_pool import SQLite3Pool
from ..modules import (
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
from .document_model import DocumentModel, Task


class StateMachineState(Enum):
  SETTING = 0
  SCANNING = 1
  PROCESSING = 2

class StateMachine:
  def __init__(self, db_path: Path, modules: Iterable[Module]):
    self._db: SQLite3Pool = SQLite3Pool(FRAMEWORK_DB, db_path)
    with self._db.connect() as (cursor, conn):
      model_context = ModuleContext(cursor, modules)
      conn.commit()

    self._base_model: KnowledgeBaseModel = KnowledgeBaseModel(model_context)
    self._resource_model: ResourceModel = ResourceModel(model_context)
    self._document_model: DocumentModel = DocumentModel(model_context)
    self._state: StateMachineState = StateMachineState.SETTING

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
        resource_base = self._resource_model.create_resource_base(
          cursor=cursor,
          module=resource_module,
        )
        records: Iterable[tuple[PreprocessingModule | IndexModule, Any]] = []
        for module, params in preproc_params:
          records.append((module, params))
        for module, params in index_params:
          records.append((module, params))

        return self._base_model.create_knowledge_base(
          cursor=cursor,
          resource_base=resource_base,
          resource_params=resource_params,
          records=records,
        )
      except BaseException as e:
        conn.rollback()
        raise e

  def create_resource(
        self,
        base: KnowledgeBase,
        event_id: int,
        path: Path,
        hash: bytes,
        content_type: str,
        meta: Any,
        updated_at: int
      ) -> Resource:

    assert self._state == StateMachineState.SCANNING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        resource = self._resource_model.create_resource(
          cursor=cursor,
          hash=hash,
          resource_base=base.resource_base,
          content_type=content_type,
          meta=meta,
          updated_at=updated_at,
        )
        self._submit_task_if_hash_created(
          event_id=event_id,
          hash=hash,
          resource_path=path,
          resource_hash=hash,
          resource_module=base.resource_base.module,
          from_resource_hash=None,
        )
        return resource

      except BaseException as e:
        conn.rollback()
        raise e

  def update_resource(
        self,
        base: KnowledgeBase,
        event_id: int,
        path: Path,
        hash: bytes,
        new_hash: bytes,
        new_content_type: str,
        new_meta: Any,
        updated_at: int,
      ):

    if hash == new_hash:
      return self.create_resource(
        base=base,
        event_id=event_id,
        path=path,
        hash=new_hash,
        content_type=new_content_type,
        meta=new_meta,
        updated_at=updated_at,
      )

    assert self._state == StateMachineState.SCANNING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        origin_resource = next(self._resource_model.get_resources(
          cursor=cursor,
          resource_base=base.resource_base,
          hash=hash,
        ))
        resource = self._resource_model.update_resource(
          cursor=cursor,
          resource_id=origin_resource.id,
          hash=new_hash,
          content_type=new_content_type,
          meta=new_meta,
          updated_at=updated_at,
        )
        self._submit_task_if_hash_created(
          event_id=event_id,
          hash=new_hash,
          resource_path=path,
          resource_hash=new_hash,
          resource_module=base.resource_base.module,
          from_resource_hash=hash,
        )
        self._submit_task_if_hash_removed(
          event_id=event_id,
          hash=hash,
          resource_path=path,
          resource_hash=new_hash,
          resource_module=base.resource_base.module,
        )
        return resource

      except BaseException as e:
        conn.rollback()
        raise e

  def remove_resource(
        self,
        base: KnowledgeBase,
        event_id: int,
        hash: bytes,
      ) -> None:
    assert self._state == StateMachineState.SCANNING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        # self._resource_model.remove_resource(
        #   cursor,
        # )
        # return resource

      except BaseException as e:
        conn.rollback()
        raise e

  def _submit_task_if_hash_created(
      self,
      event_id: int,
      hash: bytes,
      resource_path: Path,
      resource_hash: bytes,
      resource_module: ResourceModule,
      from_resource_hash: bytes | None,
    ):
    pass

  def _submit_task_if_hash_removed(
      self,
      event_id: int,
      hash: bytes,
      resource_path: Path,
      resource_hash: bytes,
      resource_module: ResourceModule,
    ):
    pass