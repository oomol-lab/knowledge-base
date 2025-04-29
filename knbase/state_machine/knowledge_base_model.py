from __future__ import annotations
from typing import Any, Generator
from json import loads, dumps
from sqlite3 import Cursor

from .common import FRAMEWORK_DB
from .module_context import ModuleContext
from ..utils import fetchmany
from ..sqlite3_pool import register_table_creators
from ..module import KnowledgeBase, ResourceModule


class KnowledgeBaseModel:
  def __init__(self, modules_context: ModuleContext):
    self._ctx: ModuleContext = modules_context

  def get_knowledge_bases(self, cursor: Cursor) -> Generator[KnowledgeBase, None, None]:
    cursor.execute(
      "SELECT id, res_module, res_params FROM knbases"
    )
    for row in fetchmany(cursor):
      knbase_id = row[0]
      resource_module_id = row[1]
      resource_params = loads(row[2])
      resource_module = self._ctx.module(resource_module_id)
      yield KnowledgeBase(
        id=knbase_id,
        resource_params=resource_params,
        resource_module=resource_module,
      )

  def create_knowledge_base(
        self,
        cursor: Cursor,
        resource_module: ResourceModule,
        resource_params: Any,
      ) -> KnowledgeBase:

    cursor.execute(
      "INSERT INTO knbases (res_module, res_params) VALUES (?, ?)",
      (
        self._ctx.module_id(resource_module),
        dumps(resource_params),
      ),
    )
    return KnowledgeBase(
      id=cursor.lastrowid,
      resource_module=resource_module,
      resource_params=resource_params,
    )

  def update_resource_params(
        self,
        cursor: Cursor,
        knbase: KnowledgeBase,
        resource_params: Any,
      ) -> KnowledgeBase:

    cursor.execute(
      "UPDATE knbases SET params = ? WHERE id = ?",
      (dumps(obj=resource_params), knbase.id),
    )
    return KnowledgeBase(
      id=knbase.id,
      resource_module=knbase.resource_module,
      resource_params=resource_params,
    )

def _create_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE knbases (
      id INTEGER PRIMARY KEY,
      res_module INTEGER NOT NULL,
      res_params TEXT NOT NULL
    )
  """)

register_table_creators(FRAMEWORK_DB, _create_tables)