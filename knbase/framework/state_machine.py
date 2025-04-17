from pathlib import Path
from typing import Any, Iterable, Generator

from ..sqlite3_pool import SQLite3Pool
from ..modules import Module, ResourceModule, PreprocessingModule, IndexModule
from .common import FRAMEWORK_DB
from .module_context import ModuleContext
from .knowledge_base_model import KnowledgeBase, KnowledgeBaseModel
from .resource_model import ResourceModel
from .document_model import DocumentModel


class StateMachine:
  def __init__(self, db_path: Path, modules: Iterable[Module]):
    self._db: SQLite3Pool = SQLite3Pool(FRAMEWORK_DB, db_path)
    with self._db.connect() as (cursor, conn):
      model_context = ModuleContext(cursor, modules)
      conn.commit()

    self._base_model: KnowledgeBaseModel = KnowledgeBaseModel(model_context)
    self._resource_model: ResourceModel = ResourceModel(model_context)
    self._document_model: DocumentModel = DocumentModel(model_context)

  def get_knowledge_bases(self) -> Generator[KnowledgeBase, None, None]:
    with self._db.connect() as (cursor, _):
      yield from self._base_model.get_knowledge_bases(cursor)

  def create_knowledge_base(
        self,
        resource_param: tuple[ResourceModule, Any],
        preproc_params: Iterable[tuple[PreprocessingModule, Any]],
        index_params: Iterable[tuple[IndexModule, Any]],
      ) -> KnowledgeBase:

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
      except Exception as e:
        conn.rollback()
        raise e

  def create_resource(
        self,
        base: KnowledgeBase,
        event_id: int,
        hash: bytes,
        content_type: str,
        meta: Any,
        updated_at: int
      ):
    pass