import os
import unittest

from io import BufferedReader
from typing import Generator, Iterable
from pathlib import Path

from knbase.sqlite3_pool import SQLite3Pool
from knbase.framework.common import FRAMEWORK_DB
from knbase.framework.knowledge_base_model import KnowledgeBaseModel
from knbase.framework.module_context import ModuleContext
from knbase.framework.resource_model import ResourceModel
from knbase.framework.document_model import Document, DocumentModel
from knbase.module import (
  ResourceModule,
  PreprocessingModule,
  IndexModule,
  PreprocessingFile,
  PreprocessingResult,
  Resource,
  ResourceEvent,
  KnowledgeBase,
)


class _MyResourceModule(ResourceModule):
  def __init__(self):
    super().__init__("my_res")

  def scan(self, base: KnowledgeBase) -> Generator[ResourceEvent, None, None]:
    raise NotImplementedError()

  def open(self, resource: Resource) -> BufferedReader:
    raise NotImplementedError()

  def complete_event(self, event: ResourceEvent) -> None:
    raise NotImplementedError()

class _MyPreprocessingModule(PreprocessingModule):
  def __init__(self):
    super().__init__("my_preproc")

  def create(
    self,
    context: Path,
    file: PreprocessingFile,
    resource: Resource,
    recover: bool,
  ) -> Iterable[PreprocessingResult]:
    raise NotImplementedError()

  def update(
    self,
    context: Path,
    file: PreprocessingFile,
    prev_file: PreprocessingFile,
    prev_cache: Path | None,
    resource: Resource,
    recover: bool,
  ) -> Iterable[PreprocessingResult]:
    raise NotImplementedError()

class _MyIndexModule(IndexModule):
  def __init__(self):
    super().__init__("my_index")

  def create(self, id: int, document: Document):
    raise NotImplementedError()

  def remove(self, id: int):
    raise NotImplementedError()

class TestFrameworkModel(unittest.TestCase):

  def test_resource_models(self):
    db, ctx, resource_module, _, _ = _create_variables("test_resources.sqlite3")
    knbase_model = KnowledgeBaseModel(ctx)
    model = ResourceModel(ctx)

    with db.connect() as (cursor, conn):
      knbase: KnowledgeBase = knbase_model.create_knowledge_base(
        cursor=cursor,
        resource_module=resource_module,
        resource_params=None,
        records=[],
      )
      conn.commit()

    marked_resources1: list[Resource] = []
    marked_resources2: list[Resource] = []

    with db.connect() as (cursor, conn):
      self.assertEqual(
        first=model.count_resources(cursor, knbase, b"HASH1"),
        second=0,
      )
      resource = Resource(
        id=1,
        hash=b"HASH1",
        base=knbase,
        content_type="text/plain",
        meta="RES1",
        updated_at=110,
      )
      model.save_resource(cursor, resource)
      conn.commit()
      marked_resources1.append(resource)

      self.assertEqual(resource.hash, b"HASH1")
      self.assertEqual(resource.base.id, knbase.id)
      self.assertTrue(resource.base.resource_module == resource_module)
      self.assertEqual(resource.content_type, "text/plain")
      self.assertEqual(resource.meta, "RES1")
      self.assertEqual(resource.updated_at, 110)

    with db.connect() as (cursor, conn):
      self.assertEqual(
        first=model.count_resources(cursor, knbase, b"HASH1"),
        second=1,
      )
      resource = Resource(
        id=2,
        hash=b"HASH1",
        base=knbase,
        content_type="text/plain",
        meta="RES2",
        updated_at=120,
      )
      model.save_resource(cursor, resource)
      conn.commit()
      marked_resources2.append(resource)

      self.assertEqual(resource.hash, b"HASH1")
      self.assertEqual(resource.base.id, knbase.id)
      self.assertTrue(resource.base.resource_module == resource_module)
      self.assertEqual(resource.content_type, "text/plain")
      self.assertEqual(resource.meta, "RES2")
      self.assertEqual(resource.updated_at, 120)

    with db.connect() as (cursor, conn):
      self.assertEqual(
        first=model.count_resources(cursor, knbase, b"HASH1"),
        second=2,
      )
      resource = Resource(
        id=3,
        hash=b"HASH3",
        base=knbase,
        content_type="text/plain",
        meta="RES3",
        updated_at=119,
      )
      model.save_resource(cursor, resource)
      conn.commit()
      marked_resources1.append(resource)

      self.assertEqual(resource.hash, b"HASH3")
      self.assertEqual(resource.base.id, knbase.id)
      self.assertTrue(resource.base.resource_module == resource_module)
      self.assertEqual(resource.content_type, "text/plain")
      self.assertEqual(resource.meta, "RES3")
      self.assertEqual(resource.updated_at, 119)

    with db.connect() as (cursor, _):
      self.assertEqual(
        first=model.count_resources(cursor, knbase, b"HASH1"),
        second=2,
      )
      data = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, knbase, b"HASH1")
      ]
      self.assertListEqual(data, [
        (b"HASH1", "RES2", 120),
        (b"HASH1", "RES1", 110),
      ])

    marked_resources1.sort(key=lambda r: r.id)

    with db.connect() as (cursor, conn):
      for resource in marked_resources1:
        model.update_resource(cursor, resource, meta="NEW_RES")
      for resource in marked_resources2:
        model.update_resource(cursor, resource, hash=b"HASH2")
      conn.commit()

    with db.connect() as (cursor, _):
      self.assertEqual(
        first=model.count_resources(cursor, knbase, b"HASH1"),
        second=1,
      )
      self.assertEqual(
        first=model.count_resources(cursor, knbase, b"HASH2"),
        second=1,
      )
      self.assertEqual(
        first=model.count_resources(cursor, knbase, b"HASH3"),
        second=1,
      )
      data1 = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, knbase, b"HASH1")
      ]
      data2 = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, knbase, b"HASH2")
      ]
      data3 = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, knbase, b"HASH3")
      ]
      self.assertListEqual(data1, [
        (b"HASH1", "NEW_RES", 110),
      ])
      self.assertListEqual(data2, [
        (b"HASH2", "RES2", 120),
      ])
      self.assertListEqual(data3, [
        (b"HASH3", "NEW_RES", 119),
      ])

    with db.connect() as (cursor, conn):
      for resource in marked_resources1:
        model.remove_resource(cursor, knbase, resource.id)
      conn.commit()

    with db.connect() as (cursor, _):
      self.assertEqual(
        first=model.count_resources(cursor, knbase, b"HASH1"),
        second=0,
      )
      self.assertEqual(
        first=model.count_resources(cursor, knbase, b"HASH2"),
        second=1,
      )
      self.assertEqual(
        first=model.count_resources(cursor, knbase, b"HASH3"),
        second=0,
      )
      data1 = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, knbase, b"HASH1")
      ]
      data2 = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, knbase, b"HASH2")
      ]
      data3 = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, knbase, b"HASH3")
      ]
      self.assertListEqual(data1, [])
      self.assertListEqual(data2, [
        (b"HASH2", "RES2", 120),
      ])
      self.assertListEqual(data3, [])

  def test_document_models(self):
    db, ctx, resource_module, preproc_module, _ = _create_variables("test_documents.sqlite3")
    knbase_model = KnowledgeBaseModel(ctx)
    model = DocumentModel(ctx)

    with db.connect() as (cursor, conn):
      knbase: KnowledgeBase = knbase_model.create_knowledge_base(
        cursor=cursor,
        resource_module=resource_module,
        resource_params=None,
        records=[],
      )
      conn.commit()

    resource_key1 = (preproc_module, knbase, "HASH-1")
    resource_key2 = (preproc_module, knbase, "HASH-2")

    with db.connect() as (cursor, conn):
      document1 = model.append_document(
        cursor=cursor,
        preprocessing_module=resource_key1[0],
        base=resource_key1[1],
        resource_hash=resource_key1[2],
        document_hash=b"DOCUMENT-HASH-1",
        path="/path/to/document1",
        meta="META",
      )
      document2 = model.append_document(
        cursor=cursor,
        preprocessing_module=resource_key1[0],
        base=resource_key1[1],
        resource_hash=resource_key1[2],
        document_hash=b"DOCUMENT-HASH-2",
        path="/path/to/document2",
        meta="META",
      )
      document3 = model.append_document(
        cursor=cursor,
        preprocessing_module=resource_key2[0],
        base=resource_key2[1],
        resource_hash=resource_key2[2],
        document_hash=b"DOCUMENT-HASH-3",
        path="/path/to/document3",
        meta="META",
      )
      conn.commit()

    with db.connect() as (cursor, _):
      self.assertNotEqual(document1.id, document2.id)
      self.assertNotEqual(document1.id, document3.id)
      self.assertNotEqual(document2.id, document3.id)
      self.assertEqual(1, model.get_document_refs_count(cursor, document1))
      self.assertEqual(1, model.get_document_refs_count(cursor, document2))
      self.assertEqual(1, model.get_document_refs_count(cursor, document3))
      self.assertListEqual(
        list1=[document1.id, document2.id],
        list2=[d.id for d in model.get_documents(
          cursor=cursor,
          preprocessing_module=resource_key1[0],
          base=resource_key1[1],
          resource_hash=resource_key1[2],
        )],
      )
      self.assertListEqual(
        list1=[document3.id],
        list2=[d.id for d in model.get_documents(
          cursor=cursor,
          preprocessing_module=resource_key2[0],
          base=resource_key2[1],
          resource_hash=resource_key2[2],
        )],
      )

    with db.connect() as (cursor, conn):
      model.append_document(
        cursor=cursor,
        preprocessing_module=resource_key1[0],
        base=resource_key1[1],
        resource_hash=resource_key1[2],
        document_hash=b"DOCUMENT-HASH-3",
        path="/path/to/new-file-1",
        meta="META",
      )
      model.append_document(
        cursor=cursor,
        preprocessing_module=resource_key2[0],
        base=resource_key2[1],
        resource_hash=resource_key2[2],
        document_hash=b"DOCUMENT-HASH-2",
        path="/path/to/new-file-2",
        meta="META",
      )
      conn.commit()

    with db.connect() as (cursor, _):
      self.assertEqual(1, model.get_document_refs_count(cursor, document1))
      self.assertEqual(2, model.get_document_refs_count(cursor, document2))
      self.assertEqual(2, model.get_document_refs_count(cursor, document3))
      self.assertListEqual(
        list1=[document1.id, document2.id, document3.id],
        list2=[d.id for d in model.get_documents(
          cursor=cursor,
          preprocessing_module=resource_key1[0],
          base=resource_key1[1],
          resource_hash=resource_key1[2],
        )],
      )
      self.assertListEqual(
        list1=[document2.id, document3.id],
        list2=[d.id for d in model.get_documents(
          cursor=cursor,
          preprocessing_module=resource_key2[0],
          base=resource_key2[1],
          resource_hash=resource_key2[2],
        )],
      )

    with db.connect() as (cursor, conn):
      model.remove_references_from_resource(
        cursor=cursor,
        preprocessing_module=resource_key2[0],
        base=resource_key2[1],
        resource_hash=resource_key2[2],
      )
      conn.commit()

    with db.connect() as (cursor, _):
      self.assertEqual(1, model.get_document_refs_count(cursor, document1))
      self.assertEqual(1, model.get_document_refs_count(cursor, document2))
      self.assertEqual(1, model.get_document_refs_count(cursor, document3))

def _create_variables(file_name: str):
  db_path = _ensure_db_file_not_exist(file_name)
  db = SQLite3Pool(FRAMEWORK_DB, db_path)
  resource_module = _MyResourceModule()
  preproc_module = _MyPreprocessingModule()
  index_module = _MyIndexModule()
  modules = (
    resource_module,
    preproc_module,
    index_module,
  )
  with db.connect() as (cursor, conn):
    ctx = ModuleContext(cursor, modules)
    conn.commit()
    return db, ctx, resource_module, preproc_module, index_module

def _ensure_db_file_not_exist(file_name: str) -> Path:
  base_path = os.path.join(__file__, "..", "..", "tests_temp", "framework")
  base_path = os.path.abspath(base_path)
  os.makedirs(base_path, exist_ok=True)

  file_path = Path(base_path).joinpath(file_name)
  if file_path.exists():
    os.remove(file_path)

  return file_path