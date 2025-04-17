import os
import unittest

from io import BufferedReader
from typing import Generator, Iterable
from pathlib import Path

from knbase.framework.common import FRAMEWORK_DB
from knbase.framework.module_context import ModuleContext
from knbase.framework.resource_model import ResourceModel
from knbase.framework.document_model import DocumentModel, DocumentParams, IndexTaskOperation, TaskStep
from knbase.modules import ResourceModule, PreprocessingModule, IndexModule
from knbase.modules.preprocessing import Document, PreprocessingFile, PreprocessingResult
from knbase.modules.resource import Resource, ResourceBase, ResourceEvent
from knbase.sqlite3_pool import SQLite3Pool


class _MyResourceModule(ResourceModule):
  def __init__(self):
    super().__init__("my_res")

  def scan(self, base: ResourceBase) -> Generator[ResourceEvent, None, None]:
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
    model = ResourceModel(ctx)

    with db.connect() as (cursor, conn):
      base = model.create_resource_base(cursor, resource_module)
      conn.commit()

    with db.connect() as (cursor, _):
      base2 = model.get_resource_base(cursor, base.id)
      self.assertEqual(base2.id, base.id)
      self.assertTrue(base2.module == resource_module)

    marked_resources1: list[Resource] = []
    marked_resources2: list[Resource] = []

    with db.connect() as (cursor, conn):
      self.assertEqual(
        first=model.count_resources(cursor, base, b"HASH1"),
        second=0,
      )
      resource = model.create_resource(
        cursor=cursor,
        hash=b"HASH1",
        resource_base=base,
        content_type="text/plain",
        meta="RES1",
        updated_at=110,
      )
      conn.commit()
      marked_resources1.append(resource)

      self.assertEqual(resource.hash, b"HASH1")
      self.assertEqual(resource.base.id, base.id)
      self.assertTrue(resource.base.module == resource_module)
      self.assertEqual(resource.content_type, "text/plain")
      self.assertEqual(resource.meta, "RES1")
      self.assertEqual(resource.updated_at, 110)

    with db.connect() as (cursor, conn):
      self.assertEqual(
        first=model.count_resources(cursor, base, b"HASH1"),
        second=1,
      )
      resource = model.create_resource(
        cursor=cursor,
        hash=b"HASH1",
        resource_base=base,
        content_type="text/plain",
        meta="RES2",
        updated_at=120,
      )
      conn.commit()
      marked_resources2.append(resource)

      self.assertEqual(resource.hash, b"HASH1")
      self.assertEqual(resource.base.id, base.id)
      self.assertTrue(resource.base.module == resource_module)
      self.assertEqual(resource.content_type, "text/plain")
      self.assertEqual(resource.meta, "RES2")
      self.assertEqual(resource.updated_at, 120)

    with db.connect() as (cursor, conn):
      self.assertEqual(
        first=model.count_resources(cursor, base, b"HASH1"),
        second=2,
      )
      resource = model.create_resource(
        cursor=cursor,
        hash=b"HASH3",
        resource_base=base,
        content_type="text/plain",
        meta="RES3",
        updated_at=119,
      )
      conn.commit()
      marked_resources1.append(resource)

      self.assertEqual(resource.hash, b"HASH3")
      self.assertEqual(resource.base.id, base.id)
      self.assertTrue(resource.base.module == resource_module)
      self.assertEqual(resource.content_type, "text/plain")
      self.assertEqual(resource.meta, "RES3")
      self.assertEqual(resource.updated_at, 119)

    with db.connect() as (cursor, _):
      self.assertEqual(
        first=model.count_resources(cursor, base, b"HASH1"),
        second=2,
      )
      data = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, base, b"HASH1")
      ]
      self.assertListEqual(data, [
        (b"HASH1", "RES2", 120),
        (b"HASH1", "RES1", 110),
      ])

    marked_resources1.sort(key=lambda r: r.id)

    with db.connect() as (cursor, conn):
      for resource in marked_resources1:
        model.update_resource(cursor, resource.id, meta="NEW_RES")
      for resource in marked_resources2:
        model.update_resource(cursor, resource.id, hash=b"HASH2")
      conn.commit()

    with db.connect() as (cursor, _):
      self.assertEqual(
        first=model.count_resources(cursor, base, b"HASH1"),
        second=1,
      )
      self.assertEqual(
        first=model.count_resources(cursor, base, b"HASH2"),
        second=1,
      )
      self.assertEqual(
        first=model.count_resources(cursor, base, b"HASH3"),
        second=1,
      )
      data1 = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, base, b"HASH1")
      ]
      data2 = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, base, b"HASH2")
      ]
      data3 = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, base, b"HASH3")
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
        model.remove_resource(cursor, resource.id)
      conn.commit()

    with db.connect() as (cursor, _):
      self.assertEqual(
        first=model.count_resources(cursor, base, b"HASH1"),
        second=0,
      )
      self.assertEqual(
        first=model.count_resources(cursor, base, b"HASH2"),
        second=1,
      )
      self.assertEqual(
        first=model.count_resources(cursor, base, b"HASH3"),
        second=0,
      )
      data1 = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, base, b"HASH1")
      ]
      data2 = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, base, b"HASH2")
      ]
      data3 = [
        (r.hash, r.meta, r.updated_at)
        for r in model.get_resources(cursor, base, b"HASH3")
      ]
      self.assertListEqual(data1, [])
      self.assertListEqual(data2, [
        (b"HASH2", "RES2", 120),
      ])
      self.assertListEqual(data3, [])

  def test_document_models(self):
    db, ctx, resource_module, preproc_module, index_module = _create_variables("test_documents.sqlite3")
    model = DocumentModel(ctx)

    with db.connect() as (cursor, conn):
      task1 = model.create_task(
        cursor=cursor,
        event_id=42,
        resource_path=Path("/path/to/foobar"),
        resource_hash=b"HASH1",
        resource_module=resource_module,
      )
      conn.commit()

    with db.connect() as (cursor, _):
      got_task = model.get_task(cursor)
      self.assertEqual(got_task.id, task1.id)
      self.assertEqual(got_task.event_id, task1.event_id)
      self.assertEqual(got_task.resource_path, task1.resource_path)
      self.assertEqual(got_task.resource_hash, task1.resource_hash)
      self.assertIsNone(model.get_task(
        cursor=cursor,
        unexpected_tasks=(got_task,),
      ))
      ids1 = [t.id for t in model.get_tasks(
        cursor=cursor,
        resource_hash=b"HASH1",
      )]
      ids2 = [t.id for t in model.get_tasks(
        cursor=cursor,
        resource_hash=b"HASH2",
      )]
      self.assertListEqual(ids1, [task1.id])
      self.assertListEqual(ids2, [])

    with db.connect() as (cursor, conn):
      task2 = model.create_task(
        cursor=cursor,
        event_id=98,
        resource_path=Path("/path/to/foobar2"),
        from_resource_hash=b"HASH1",
        resource_hash=b"HASH2",
        resource_module=resource_module,
      )
      conn.commit()

    with db.connect() as (cursor, _):
      ids1 = [t.id for t in model.get_tasks(
        cursor=cursor,
        resource_hash=b"HASH1",
      )]
      ids2 = [t.id for t in model.get_tasks(
        cursor=cursor,
        resource_hash=b"HASH2",
      )]
      self.assertListEqual(ids1, [task1.id])
      self.assertListEqual(ids2, [task2.id])

      got_task = model.get_task(cursor)
      self.assertEqual(got_task.id, task1.id)
      got_task = model.get_task(cursor, (task1,))
      self.assertEqual(got_task.id, task2.id)

    with db.connect() as (cursor, conn):
      model.go_to_preprocess(cursor, task1, (preproc_module,))
      conn.commit()

    with db.connect() as (cursor, _):
      got_task = model.get_task(cursor, (task2,))
      self.assertEqual(got_task.id, task1.id)
      self.assertEqual(got_task.step, TaskStep.PROCESSING)
      self.assertListEqual(
        list1=[t.module.id for t in got_task.preprocessing_tasks],
        list2=[preproc_module.id],
      )
      task1 = got_task
      preprocessing_task = got_task.preprocessing_tasks[0]

    with db.connect() as (cursor, conn):
      task1 = model.complete_preprocess(
        cursor=cursor,
        task=task1,
        preprocessing_task=preprocessing_task,
        index_modules=(index_module,),
        removed_document_ids=(),
        added_documents=(
          DocumentParams(
            path="/documents/doc1.txt",
            meta="foobar",
          ),
          DocumentParams(
            path="/documents/doc2.txt",
            meta="hello world",
          ),
        ),
      )
      conn.commit()
      self.assertListEqual(task1.preprocessing_tasks, [])
      self.assertListEqual(
        list1=[t.operation for t in task1.index_tasks],
        list2=[IndexTaskOperation.CREATE] * 2,
      )

    added_document_ids = [t.document_id for t in task1.index_tasks]
    index_tasks1 = [*task1.index_tasks]

    with db.connect() as (cursor, _):
      self.assertListEqual(
        list1=added_document_ids,
        list2=[
          document.id
          for document in model.get_documents(
            cursor=cursor,
            resource_hash=task1.resource_hash,
            preprocessing_module=preproc_module,
          )
        ]
      )
      task1 = model.get_task(cursor, (task2,))
      self.assertListEqual(task1.preprocessing_tasks, [])
      self.assertListEqual(
        list1=[t.id for t in task1.index_tasks],
        list2=[t.id for t in index_tasks1],
      )

    with db.connect() as (cursor, conn):
      task1 = model.complete_handle_index(cursor, task1, (index_tasks1[0],))
      self.assertEqual(task1.step, TaskStep.PROCESSING)
      self.assertListEqual(task1.preprocessing_tasks, [])
      self.assertListEqual(
        list1=[t.id for t in task1.index_tasks],
        list2=[index_tasks1[1].id],
      )
      conn.commit()

    with db.connect() as (cursor, _):
      task1 = model.get_task(cursor, (task2,))
      self.assertListEqual(task1.preprocessing_tasks, [])
      self.assertListEqual(
        list1=[t.id for t in task1.index_tasks],
        list2=[index_tasks1[1].id],
      )

    with db.connect() as (cursor, conn):
      task1 = model.complete_handle_index(cursor, task1, index_tasks1)
      self.assertEqual(task1.step, TaskStep.COMPLETED)
      conn.commit()

    with db.connect() as (cursor, _):
      task1 = model.get_task(cursor, (task2,))
      self.assertIsNone(task1)
      self.assertListEqual(
        list1=added_document_ids,
        list2=[
          document.id
          for document in model.get_documents(
            cursor=cursor,
            resource_hash=b"HASH1",
            preprocessing_module=preproc_module,
          )
        ]
      )

    with db.connect() as (cursor, conn):
      task3 = model.create_task(
        cursor=cursor,
        event_id=120,
        resource_path=Path("/path/to/foobar"),
        resource_hash=b"HASH1",
        resource_module=resource_module,
      )
      self.assertEqual(task3.step, TaskStep.READY)
      task3 = model.go_to_remove(cursor, task3, (index_module,))
      index_tasks3 = task3.index_tasks
      self.assertEqual(task3.step, TaskStep.PROCESSING)
      self.assertListEqual(
        list1=added_document_ids,
        list2=[t.document_id for t in index_tasks3]
      )
      self.assertListEqual(
        list1=[IndexTaskOperation.REMOVE] * 2,
        list2=[t.operation for t in index_tasks3]
      )
      conn.commit()

    with db.connect() as (cursor, conn):
      task3 = model.complete_handle_index(cursor, task3, index_tasks3)
      self.assertEqual(task3.step, TaskStep.COMPLETED)
      conn.commit()

    with db.connect() as (cursor, _):
      task3 = model.get_task(cursor, (task2,))
      self.assertIsNone(task3)
      self.assertListEqual(
        list1=[],
        list2=list(model.get_documents(
          cursor=cursor,
          resource_hash=b"HASH1",
          preprocessing_module=preproc_module,
        )),
      )

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