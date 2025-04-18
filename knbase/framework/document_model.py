from dataclasses import dataclass
from json import loads, dumps
from typing import Any, Generator
from sqlite3 import Cursor
from pathlib import Path

from ..module import KnowledgeBase
from ..sqlite3_pool import register_table_creators
from ..utils import fetchmany
from .common import FRAMEWORK_DB
from .module_context import ModuleContext, PreprocessingModule


@dataclass
class Document:
  id: int
  preprocessing_module: PreprocessingModule
  base: KnowledgeBase
  resource_hash: bytes
  document_hash: bytes # 不会作为唯一性判定，仅供 preprocess 参考以去重
  path: Path
  meta: Any

class DocumentModel:
  def __init__(self, modules_context: ModuleContext):
    self._ctx: ModuleContext = modules_context

  def get_documents(
        self,
        cursor: Cursor,
        preprocessing_module: PreprocessingModule,
        base: KnowledgeBase,
        resource_hash: bytes,
      ) -> Generator[Document, None, None]:

    cursor.execute(
      """
      SELECT id, doc_hash, path, meta FROM documents
      WHERE preproc_module = ? AND knbase = ? AND res_hash = ?
      """,
      (
        self._ctx.module_id(preprocessing_module),
        base.id,
        resource_hash,
      ),
    )
    for row in fetchmany(cursor):
      document_id, document_hash, path, meta_text = row
      yield Document(
        id=document_id,
        preprocessing_module=preprocessing_module,
        base=base,
        resource_hash=resource_hash,
        document_hash=document_hash,
        path=Path(path),
        meta=loads(meta_text),
      )

  def create_document(
        self,
        cursor: Cursor,
        preprocessing_module: PreprocessingModule,
        base: KnowledgeBase,
        resource_hash: bytes,
        document_hash: bytes,
        path: Path,
        meta: Any,
      ) -> Document:

    cursor.execute(
      """
      INSERT INTO documents (
        preproc_module, knbase, res_hash, doc_hash, path, meta
      ) VALUES (?, ?, ?, ?, ?, ?)
      """,
      (
        self._ctx.module_id(preprocessing_module),
        base.id,
        resource_hash,
        document_hash,
        str(path),
        dumps(meta),
      ),
    )
    return Document(
      id=cursor.lastrowid,
      preprocessing_module=preprocessing_module,
      base=base,
      resource_hash=resource_hash,
      document_hash=document_hash,
      path=path,
      meta=meta,
    )

  def remove_document(self, cursor: Cursor, document_id: int) -> None:
    cursor.execute(
      "DELETE FROM documents WHERE id = ?",
      (document_id,),
    )

  def remove_documents(
        self,
        cursor: Cursor,
        preprocessing_module: PreprocessingModule,
        base: KnowledgeBase,
        resource_hash: bytes,
      ) -> None:

    cursor.execute(
      """
      DELETE FROM documents
      WHERE preproc_module = ? AND knbase = ? AND res_hash = ?
      """,
      (
        self._ctx.module_id(preprocessing_module),
        base.id,
        resource_hash,
      ),
    )

def _create_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE documents (
      id INTEGER PRIMARY KEY,
      preproc_module INTEGER,
      knbase INTEGER,
      res_hash BLOB,
      doc_hash TEXT,
      path TEXT NOT NULL,
      meta TEXT NOT NULL
    )
  """)

  cursor.execute("""
    CREATE INDEX idx_document ON documents (preproc_module, knbase, res_hash)
  """)

register_table_creators(FRAMEWORK_DB, _create_tables)