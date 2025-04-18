from dataclasses import dataclass
from json import loads
from typing import Any, Generator
from sqlite3 import Cursor
from pathlib import Path

from .common import FRAMEWORK_DB
from .module_context import ModuleContext, PreprocessingModule
from ..sqlite3_pool import register_table_creators
from ..utils import fetchmany


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
      (resource_hash, self._ctx.module_id(preprocessing_module)),
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

register_table_creators(FRAMEWORK_DB, _create_tables)