import os
import unittest

from io import BufferedReader
from typing import Generator, Iterable
from pathlib import Path
from sqlite3 import Cursor

from knbase.sqlite3_pool import SQLite3Pool
from knbase.framework.common import FRAMEWORK_DB
from knbase.framework.knowledge_base_model import KnowledgeBaseModel
from knbase.framework.module_context import ModuleContext
from knbase.framework.resource_model import ResourceModel
from knbase.framework.document_model import Document, DocumentModel
from knbase.framework.task_model import TaskModel, IndexTaskOperation, TaskStep, TaskReason, IndexTask
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
      knbase = knbase_model.create_knowledge_base(
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
    db, ctx, resource_module, preproc_module, index_module = _create_variables("test_documents.sqlite3")
    knbase_model = KnowledgeBaseModel(ctx)
    doc_model = DocumentModel(ctx)
    task_model = TaskModel(ctx)

    with db.connect() as (cursor, conn):
      knbase = knbase_model.create_knowledge_base(
        cursor=cursor,
        resource_module=resource_module,
        resource_params=None,
        records=[],
      )
      task1 = task_model.create_task(
        cursor=cursor,
        event_id=42,
        resource_path=Path("/path/to/foobar"),
        resource_hash=b"HASH1",
        resource_module=resource_module,
      )
      conn.commit()

    with db.connect() as (cursor, _):
      got_task = task_model.get_task(cursor)
      self.assertEqual(got_task.id, task1.id)
      self.assertEqual(got_task.event_id, task1.event_id)
      self.assertEqual(got_task.resource_path, task1.resource_path)
      self.assertEqual(got_task.resource_hash, task1.resource_hash)
      self.assertIsNone(task_model.get_task(
        cursor=cursor,
        unexpected_tasks=(got_task,),
      ))
      ids1 = [t.id for t in task_model.get_tasks(
        cursor=cursor,
        resource_hash=b"HASH1",
      )]
      ids2 = [t.id for t in task_model.get_tasks(
        cursor=cursor,
        resource_hash=b"HASH2",
      )]
      self.assertListEqual(ids1, [task1.id])
      self.assertListEqual(ids2, [])

    with db.connect() as (cursor, conn):
      task2 = task_model.create_task(
        cursor=cursor,
        event_id=98,
        resource_path=Path("/path/to/foobar2"),
        from_resource_hash=b"HASH1",
        resource_hash=b"HASH2",
        resource_module=resource_module,
      )
      conn.commit()

    with db.connect() as (cursor, _):
      ids1 = [t.id for t in task_model.get_tasks(
        cursor=cursor,
        resource_hash=b"HASH1",
      )]
      ids2 = [t.id for t in task_model.get_tasks(
        cursor=cursor,
        resource_hash=b"HASH2",
      )]
      self.assertListEqual(ids1, [task1.id])
      self.assertListEqual(ids2, [task2.id])

      got_task = task_model.get_task(cursor)
      self.assertEqual(got_task.id, task1.id)
      got_task = task_model.get_task(cursor, (task1,))
      self.assertEqual(got_task.id, task2.id)

    with db.connect() as (cursor, conn):
      task_model.go_to_preprocess(
        cursor=cursor,
        task=task1,
        reason=TaskReason.CREATE,
        preproc_modules=(preproc_module,),
      )
      conn.commit()

    with db.connect() as (cursor, _):
      got_task = task_model.get_task(cursor, (task2,))
      self.assertEqual(got_task.id, task1.id)
      self.assertEqual(got_task.step, TaskStep.PROCESSING)
      self.assertListEqual(
        list1=[t.module.id for t in got_task.preprocessing_tasks],
        list2=[preproc_module.id],
      )
      task1 = got_task
      preprocessing_task = got_task.preprocessing_tasks[0]

    with db.connect() as (cursor, conn):
      added_documents: list[Document] = [
        doc_model.create_document(
          cursor=cursor,
          preprocessing_module=preproc_module,
          base=knbase,
          resource_hash=task1.resource_hash,
          document_hash=b"DOCUMENT",
          path="/documents/doc1.txt",
          meta="foobar",
        ),
        doc_model.create_document(
          cursor=cursor,
          preprocessing_module=preproc_module,
          base=knbase,
          resource_hash=task1.resource_hash,
          document_hash=b"DOCUMENT",
          path="/documents/doc12.txt",
          meta="hello world",
        ),
      ]
      task1 = task_model.complete_preprocess(
        cursor=cursor,
        task=task1,
        preprocessing_task=preprocessing_task,
        index_modules=(index_module,),
        added_documents=added_documents,
        removed_document_ids=(),
      )
      conn.commit()
      self.assertListEqual(task1.preprocessing_tasks, [])
      self.assertListEqual(
        list1=[t.operation for t in task1.index_tasks],
        list2=[IndexTaskOperation.CREATE] * 2,
      )

    index_tasks1 = [*task1.index_tasks]

    with db.connect() as (cursor, _):
      self.assertListEqual(
        list1=[d.id for d in added_documents],
        list2=[
          document.id
          for document in doc_model.get_documents(
            cursor=cursor,
            preprocessing_module=preproc_module,
            base=knbase,
            resource_hash=task1.resource_hash,
          )
        ]
      )
      task1 = task_model.get_task(cursor, (task2,))
      self.assertListEqual(task1.preprocessing_tasks, [])
      self.assertListEqual(
        list1=[t.id for t in task1.index_tasks],
        list2=[t.id for t in index_tasks1],
      )

    with db.connect() as (cursor, conn):
      task1 = task_model.complete_handle_index(cursor, task1, (index_tasks1[0],))
      self._remove_removed_documents(cursor, doc_model, (index_tasks1[0],))
      self.assertEqual(task1.step, TaskStep.PROCESSING)
      self.assertListEqual(task1.preprocessing_tasks, [])
      self.assertListEqual(
        list1=[t.id for t in task1.index_tasks],
        list2=[index_tasks1[1].id],
      )
      conn.commit()

    with db.connect() as (cursor, _):
      task1 = task_model.get_task(cursor, (task2,))
      self.assertListEqual(task1.preprocessing_tasks, [])
      self.assertListEqual(
        list1=[t.id for t in task1.index_tasks],
        list2=[index_tasks1[1].id],
      )

    with db.connect() as (cursor, conn):
      task1 = task_model.complete_handle_index(cursor, task1, index_tasks1)
      self._remove_removed_documents(cursor, doc_model, index_tasks1)
      self.assertEqual(task1.step, TaskStep.COMPLETED)
      conn.commit()

    with db.connect() as (cursor, _):
      resource_hash = task1.resource_hash
      task1 = task_model.get_task(cursor, (task2,))
      self.assertIsNone(task1)
      self.assertListEqual(
        list1=[d.id for d in added_documents],
        list2=[
          document.id
          for document in doc_model.get_documents(
            cursor=cursor,
            preprocessing_module=preproc_module,
            base=knbase,
            resource_hash=resource_hash,
          )
        ]
      )

    with db.connect() as (cursor, conn):
      task3 = task_model.create_task(
        cursor=cursor,
        event_id=120,
        resource_path=Path("/path/to/foobar"),
        resource_hash=b"HASH1",
        resource_module=resource_module,
      )
      self.assertEqual(task3.step, TaskStep.READY)
      task3 = task_model.go_to_remove(cursor, task3, (index_module,))
      index_tasks3 = task3.index_tasks
      self.assertEqual(task3.step, TaskStep.PROCESSING)
      self.assertListEqual(
        list1=[d.id for d in added_documents],
        list2=[t.document_id for t in index_tasks3]
      )
      self.assertListEqual(
        list1=[IndexTaskOperation.REMOVE] * 2,
        list2=[t.operation for t in index_tasks3]
      )
      conn.commit()

    with db.connect() as (cursor, conn):
      task3 = task_model.complete_handle_index(cursor, task3, index_tasks3)
      self._remove_removed_documents(cursor, doc_model, index_tasks3)
      self.assertEqual(task3.step, TaskStep.COMPLETED)
      conn.commit()

    with db.connect() as (cursor, _):
      resource_hash = task3.resource_hash
      task3 = task_model.get_task(cursor, (task2,))
      self.assertIsNone(task3)
      self.assertListEqual(
        list1=[],
        list2=[
          document.id
          for document in doc_model.get_documents(
            cursor=cursor,
            preprocessing_module=preproc_module,
            base=knbase,
            resource_hash=resource_hash,
          )
        ],
      )

  def _remove_removed_documents(
        self,
        cursor: Cursor,
        doc_model: DocumentModel,
        index_tasks: list[IndexTask],
      ) -> None:

    for index_task in index_tasks:
      if index_task.operation == IndexTaskOperation.REMOVE:
        doc_model.remove_document(cursor, index_task.document_id)

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