from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Generator, Iterable
from enum import Enum
from json import dumps
from sqlite3 import Cursor

from .common import FRAMEWORK_DB
from .module_context import ModuleContext
from ..utils import fetchmany
from ..sqlite3_pool import register_table_creators
from ..modules import ResourceBase, PreprocessingModule, IndexModule


@dataclass
class KnowledgeBase:
  id: int
  resource_base: ResourceBase
  resource_params: Any
  process_records: list[ProcessRecord]

@dataclass
class ProcessRecord:
  id: int
  module: PreprocessingModule | IndexModule
  params: Any

class _ProcessKind(Enum):
  Preprocess = 0
  Index = 1

class KnowledgeBaseModel:
  def __init__(self, modules_context: ModuleContext):
    self._ctx: ModuleContext = modules_context

  def get_knowledge_bases(self, cursor: Cursor) -> Generator[KnowledgeBase, None, None]:
    cursor.execute(
      "SELECT id, res_base_id, params FROM knbases"
    )
    for row in fetchmany(cursor):
      knbase_id = row[0]
      resource_base_id = row[1]
      resource_params = row[2]
      resource_base = self._ctx.module(resource_base_id)
      knbase = KnowledgeBase(
        id=knbase_id,
        resource_base=resource_base,
        resource_params=resource_params,
        process_records=[],
      )
      cursor.execute(
        "SELECT kind, module_id, params FROM knbase_process_records WHERE knbase_id = ?",
        (knbase_id,),
      )
      for kind, module_id, params in fetchmany(cursor):
        if kind == _ProcessKind.Preprocess:
          module = self._ctx.module(module_id)
        elif kind == _ProcessKind.Index:
          module = self._ctx.module(module_id)
        else:
          raise ValueError(f"Unknown process kind {kind}")

        knbase.process_records.append(
          ProcessRecord(
            id=cursor.lastrowid,
            module=module,
            params=params,
          )
        )
      yield knbase

  def create_knowledge_base(
        self,
        cursor: Cursor,
        resource_base: ResourceBase,
        resource_params: Any,
        records: Iterable[tuple[PreprocessingModule | IndexModule, Any]],
      ) -> KnowledgeBase:

    cursor.execute(
      "INSERT INTO knbases (res_base_id, params) VALUES (?, ?)",
      (resource_base.id, dumps(resource_params)),
    )
    knbase_id = cursor.lastrowid
    knbase = KnowledgeBase(
      id=knbase_id,
      resource_base=resource_base,
      resource_params=resource_params,
      process_records=[],
    )
    for module, params in records:
      kind: _ProcessKind
      if isinstance(module, PreprocessingModule):
        kind = _ProcessKind.Preprocess
      elif isinstance(module, IndexModule):
        kind = _ProcessKind.Index
      else:
        raise ValueError(f"Unknown module type {module}")

      module_id = self._ctx.model_id(module)
      cursor.execute(
        "INSERT INTO knbase_process_records (kind, knbase_id, module_id, params) VALUES (?, ?, ?, ?)",
        (kind.value, knbase_id, module_id, dumps(params)),
      )
      knbase.process_records.append(
        ProcessRecord(
          id=cursor.lastrowid,
          module=module,
          params=params,
        )
      )
    return knbase

  def update_resource_params(
      self,
      cursor: Cursor,
      knbase: KnowledgeBase,
      resource_params: Any,
    ):
    cursor.execute(
      "UPDATE knbases SET params = ? WHERE id = ?",
      (dumps(obj=resource_params), knbase.id),
    )
    return KnowledgeBase(
      id=knbase.id,
      resource_base=knbase.resource_base,
      resource_params=resource_params,
      process_records=[*knbase.process_records],
    )

  def update_process_record(
        self,
        cursor: Cursor,
        knbase: KnowledgeBase,
        record: ProcessRecord,
        params: Any,
      ) -> KnowledgeBase:

    cursor.execute(
      "UPDATE knbase_process_records SET params = ? WHERE id = ?",
      (dumps(obj=params), record.id),
    )
    return KnowledgeBase(
      id=knbase.id,
      resource_base=knbase.resource_base,
      resource_params=knbase.resource_params,
      process_records=[
        ProcessRecord(
          id=record.id,
          module=record.module,
          params=params,
        )
        if r.id == record.id else r
        for r in knbase.process_records
      ],
    )

  def remove_process_record(
        self,
        cursor: Cursor,
        knbase: KnowledgeBase,
        record: ProcessRecord,
      ) -> KnowledgeBase:
    cursor.execute(
      "DELETE FROM knbase_process_records WHERE id = ?",
      (record.id,),
    )
    return KnowledgeBase(
      id=knbase.id,
      resource_base=knbase.resource_base,
      resource_params=knbase.resource_params,
      process_records=[
        r for r in knbase.process_records if r.id != record.id
      ],
    )

def _create_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE knbases (
      id INTEGER PRIMARY KEY,
      res_base_id INTEGER NOT NULL,
      params TEXT NOT NULL
    )
  """)

  cursor.execute("""
    CREATE TABLE knbase_process_records (
      id INTEGER PRIMARY KEY,
      kind INTEGER NOT NULL,
      knbase_id INTEGER NOT NULL,
      module id INTEGER NOT NULL,
      params TEXT NOT NULL
    )
  """)

register_table_creators(FRAMEWORK_DB, _create_tables)