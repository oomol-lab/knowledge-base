import os
import unittest

from typing import Generator
from pathlib import Path
from io import BufferedReader

from knbase.modules import ResourceModule
from knbase.framework.common import FRAMEWORK_DB
from knbase.framework.module_context import ModuleContext
from knbase.framework.resource_model import ResourceModel
from knbase.modules.resource import Resource, ResourceBase, ResourceEvent
from knbase.sqlite3_pool import SQLite3Pool


class MyResourceModule(ResourceModule):
  def __init__(self):
    super().__init__("my_res")

  def scan(self, base: ResourceBase) -> Generator[ResourceEvent, None, None]:
    raise NotImplementedError()

  def open(self, resource: Resource) -> BufferedReader:
    raise NotImplementedError()

  def complete_event(self, event: ResourceEvent) -> None:
    raise NotImplementedError()


class TestFrameworkModel(unittest.TestCase):

  def test_resource_models(self):
    db_path = _ensure_db_file_not_exist("test_resources.sqlite3")
    db = SQLite3Pool(FRAMEWORK_DB, db_path)
    resource_module =  MyResourceModule()
    ctx: ModuleContext

    with db.connect() as (cursor, conn):
      ctx = ModuleContext(cursor, (resource_module,))
      conn.commit()

    model = ResourceModel(ctx)

    with db.connect() as (cursor, conn):
      base = model.create_resource_base(cursor, resource_module, {
        "foobar": "hello world",
      })
      conn.commit()

    with db.connect() as (cursor, _):
      base2 = model.get_resource_base(cursor, base.id)
      self.assertEqual(base2.id, base.id)
      self.assertTrue(base2.module == resource_module)
      self.assertEqual(base2.meta, {
        "foobar": "hello world",
      })

    marked1_ids: list[int] = []
    marked2_ids: list[int] = []

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
      marked1_ids.append(resource.id)

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
      marked2_ids.append(resource.id)

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
      marked1_ids.append(resource.id)

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

    marked1_ids.sort()

    with db.connect() as (cursor, conn):
      for id in marked1_ids:
        model.update_resource(cursor, id, meta="NEW_RES")
      for id in marked2_ids:
        model.update_resource(cursor, id, hash=b"HASH2")
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
      for id in marked1_ids:
        model.remove_resource(cursor, id)
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

def _ensure_db_file_not_exist(file_name: str) -> Path:
  base_path = os.path.join(__file__, "..", "..", "tests_temp", "framework")
  base_path = os.path.abspath(base_path)
  os.makedirs(base_path, exist_ok=True)

  file_path = Path(base_path).joinpath(file_name)
  if file_path.exists():
    os.remove(file_path)

  return file_path