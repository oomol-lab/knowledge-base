from pathlib import Path
from enum import Enum
from sqlite3 import Cursor
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
from .resource_model import ResourceModel, ResourceBase
from .document_model import DocumentModel


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
        resource: Resource,
      ) -> None:

    assert self._state == StateMachineState.SCANNING
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("BEGIN TRANSACTION")
        assert self._resource_model.get_resource(cursor, resource.id) is None
        hash_refs = self._count_resource_hash(cursor, base.resource_base, resource.hash)
        self._resource_model.save_resource(cursor, resource)
        if hash_refs == 0:
          self._submit_task_hash_created(
            cursor=cursor,
            event_id=event_id,
            resource_base=base.resource_base,
            resource_path=path,
            resource_hash=resource.hash,
            from_resource_hash=None,
          )
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
        origin_resource = self._resource_model.get_resource(cursor, resource.id)
        assert origin_resource is not None
        hash_refs = self._count_resource_hash(cursor, base.resource_base, resource.hash)
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
              resource_base=base.resource_base,
              resource_path=path,
              resource_hash=resource.hash,
              from_resource_hash=origin_resource.hash,
            )
          if self._count_resource_hash(cursor, base.resource_base, origin_resource.hash) == 0:
            self._submit_task_hash_removed(
              cursor=cursor,
              event_id=event_id,
              resource_base=base.resource_base,
              resource_hash=origin_resource.hash,
              resource_module=base.resource_base.module,
            )

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
        assert self._resource_model.get_resource(cursor, resource.id) is not None
        self._resource_model.remove_resource(cursor, resource.id)
        if self._count_resource_hash(cursor, base.resource_base, resource.hash) == 0:
          self._submit_task_hash_removed(
            event_id=event_id,
            cursor=cursor,
            resource_base=base.resource_base,
            resource_hash=resource.hash,
            resource_module=base.resource_base.module,
          )
      except BaseException as e:
        conn.rollback()
        raise e

  def _submit_task_hash_created(
      self,
      cursor: Cursor,
      event_id: int,
      resource_base: ResourceBase,
      resource_path: Path,
      resource_hash: bytes,
      from_resource_hash: bytes | None,
    ):
    pass

  def _submit_task_hash_removed(
      self,
      cursor: Cursor,
      event_id: int,
      resource_base: ResourceBase,
      resource_hash: bytes,
      resource_module: ResourceModule,
    ):
    pass

  def _count_resource_hash(self, cursor: Cursor, resource_base: ResourceBase, hash: bytes) -> int:
    count: int = 0
    count += self._resource_model.count_resources(
      cursor=cursor,
      hash=hash,
      resource_base=resource_base,
    )
    count += self._document_model.count_resource_hash_refs(
      cursor=cursor,
      resource_hash=hash,
    )
    return count