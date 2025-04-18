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
  document_hash: bytes
  path: Path
  meta: Any

class DocumentModel:
  def __init__(self, modules_context: ModuleContext):
    self._ctx: ModuleContext = modules_context

  def get_document_refs_count(self, cursor: Cursor, document: Document) -> int:
    cursor.execute(
      "SELECT COUNT(*) FROM document_refs WHERE ref = ?",
      (document.id,),
    )
    row = cursor.fetchone()
    if row is None:
      return 0
    return row[0]

  def get_documents(
        self,
        cursor: Cursor,
        preprocessing_module: PreprocessingModule,
        base: KnowledgeBase,
        resource_hash: bytes,
      ) -> Generator[Document, None, None]:

    cursor.execute(
      """
      SELECT ref FROM document_refs
      WHERE preproc_module =? AND knbase = ? AND res_hash = ?
      """,
      (
        self._ctx.module_id(preprocessing_module),
        base.id,
        resource_hash,
      ),
    )
    for row in fetchmany(cursor):
      document_id = row[0]
      cursor.execute(
        """
        SELECT preproc_module, doc_hash, path, meta FROM documents
        WHERE id = ? LIMIT 1
        """,
        (document_id,),
      )
      row = cursor.fetchone()
      if row is not None:
        preproc_module, document_hash, path, meta_text = row
        yield Document(
          id=document_id,
          preprocessing_module=self._ctx.module(preproc_module),
          base=base,
          resource_hash=resource_hash,
          document_hash=document_hash,
          path=Path(path),
          meta=loads(meta_text),
        )

  def append_document(
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
      "SELECT id FROM documents WHERE preproc_module = ? AND knbase = ? AND doc_hash = ?",
      (
        self._ctx.module_id(preprocessing_module),
        base.id,
        document_hash,
      ),
    )
    document_id: int
    row = cursor.fetchone()

    if row is not None:
      document_id = row[0]
      cursor.execute(
        "UPDATE documents SET res_hash = ?, path = ?, meta = ? WHERE id = ?",
        (
          resource_hash,
          str(path),
          dumps(meta),
          document_id,
        ),
      )
    else:
      cursor.execute(
        """
        INSERT INTO documents (preproc_module, knbase, doc_hash, res_hash, path, meta)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
          self._ctx.module_id(preprocessing_module),
          base.id,
          document_hash,
          resource_hash,
          str(path),
          dumps(meta),
        ),
      )
      document_id = cursor.lastrowid

    cursor.execute(
      """
      SELECT id FROM document_refs WHERE preproc_module = ?
      AND knbase = ? AND res_hash = ? AND doc_hash = ?
      """,
      (
        self._ctx.module_id(preprocessing_module),
        base.id,
        resource_hash,
        document_hash,
      ),
    )
    if cursor.fetchone() is None:
      cursor.execute(
        """
        INSERT INTO document_refs (preproc_module, knbase, res_hash, doc_hash, ref, path, meta)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
          self._ctx.module_id(preprocessing_module),
          base.id,
          resource_hash,
          document_hash,
          document_id,
          str(path),
          dumps(meta),
        ),
      )
    return Document(
      id=document_id,
      preprocessing_module=preprocessing_module,
      base=base,
      resource_hash=resource_hash,
      document_hash=document_hash,
      path=path,
      meta=meta,
    )

  def remove_document(self, cursor: Cursor, document: Document):
    cursor.execute(
      "DELETE FROM documents WHERE id = ?",
      (document.id,),
    )

  def remove_references_from_resource(
        self,
        cursor: Cursor,
        preprocessing_module: PreprocessingModule,
        base: KnowledgeBase,
        resource_hash: bytes,
      ) -> None:

    cursor.execute(
      """
      DELETE FROM document_refs
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
      doc_hash TEXT,
      res_hash BLOB,
      path TEXT NOT NULL,
      meta TEXT NOT NULL
    )
  """)

  cursor.execute("""
    CREATE INDEX idx_document ON documents (preproc_module, knbase, doc_hash)
  """)

  cursor.execute("""
    CREATE INDEX idx_res_document ON documents (preproc_module, knbase, res_hash)
  """)

  cursor.execute("""
    CREATE TABLE document_refs (
      id INTEGER PRIMARY KEY,
      preproc_module INTEGER,
      knbase INTEGER,
      doc_hash TEXT,
      res_hash BLOB,
      ref INTEGER,
      path TEXT NOT NULL,
      meta TEXT NOT NULL
    )
  """)

  cursor.execute("""
    CREATE INDEX idx_document_ref ON document_refs (preproc_module, knbase, res_hash, doc_hash)
  """)

  cursor.execute("""
    CREATE INDEX idx_ref_document_ref ON document_refs (ref)
  """)

register_table_creators(FRAMEWORK_DB, _create_tables)