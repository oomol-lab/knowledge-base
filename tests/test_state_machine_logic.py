import unittest

from typing import Generator, Callable, TypeVar
from pathlib import Path

from tests.my_modules import MyResourceModule, MyPreprocessingModule, MyIndexModule
from tests.utils import ensure_db_file_not_exist

from knbase.state_machine import StateMachine, StateMachineState, DocumentDescription
from knbase.module import Resource


_T = TypeVar("_T")

class TestStateMachineLogic(unittest.TestCase):

  def test_preprocess_all_in_once(self):
    db_path = ensure_db_file_not_exist("state-machine.sqlite3")
    modules = (
      MyResourceModule(),
      MyPreprocessingModule(),
      MyIndexModule(),
    )
    machine = StateMachine(db_path, modules)

    self.assertEqual(machine.state, StateMachineState.SETTING)
    self.assertListEqual(
      list1=[b.id for b in machine.get_knowledge_bases()],
      list2=[],
    )
    base = machine.create_knowledge_base(
      resource_param=(modules[0], None),
      preproc_params=((modules[1], None),),
      index_params=((modules[2], None),),
    )
    self.assertListEqual(
      list1=[b.id for b in machine.get_knowledge_bases()],
      list2=[base.id],
    )

    machine.goto_processing()
    self.assertIsNone(machine.pop_preproc_event())
    self.assertIsNone(machine.pop_handle_index_event())
    self.assertIsNone(machine.pop_removed_resource_event())

    machine.goto_scanning()
    self.assertEqual(machine.state, StateMachineState.SCANNING)
    resource1 = Resource(
      id="1",
      hash=b"HASH-1",
      base=base,
      content_type="TXT",
      meta=None,
      updated_at=0,
    )
    resource2 = Resource(
      id="2",
      hash=b"HASH-2",
      base=base,
      content_type="TXT",
      meta=None,
      updated_at=0,
    )
    machine.put_resource(0, resource1, Path("file1.txt"))
    machine.put_resource(1, resource2, Path("file2.txt"))

    machine.goto_processing()
    self.assertListEqual(
      list1=list(self._pop_all(machine.pop_handle_index_event)),
      list2=[],
    )
    self.assertListEqual(
      list1=list(self._pop_all(machine.pop_removed_resource_event)),
      list2=[],
    )
    preproc_events = list(self._pop_all(machine.pop_preproc_event))

    self.assertListEqual(
      list1=[(e.resource_hash, e.resource_path) for e in preproc_events],
      list2=[
        (b"HASH-1", Path("file1.txt")),
        (b"HASH-2", Path("file2.txt")),
      ],
    )
    machine.complete_preproc_task(
      event=preproc_events[1],
      document_descriptions=[
        DocumentDescription(
          hash=b"DOC-HASH-1",
          path="doc-1.json",
          meta=None,
        ),
        DocumentDescription(
          hash=b"DOC-HASH-2",
          path="doc-2.json",
          meta=None,
        ),
        DocumentDescription(
          hash=b"DOC-HASH-3",
          path="doc-3.json",
          meta=None,
        ),
      ],
    )
    self.assertListEqual(
      list1=list(self._pop_all(machine.pop_preproc_event)),
      list2=[],
    )
    self.assertListEqual(
      list1=list(self._pop_all(machine.pop_removed_resource_event)),
      list2=[],
    )
    self.assertListEqual(
      list1=[
        (e.document_hash, e.document_path)
        for e in self._pop_all(machine.pop_handle_index_event)
      ],
      list2=[
        (b"DOC-HASH-1", Path("doc-1.json")),
        (b"DOC-HASH-2", Path("doc-2.json")),
        (b"DOC-HASH-3", Path("doc-3.json")),
      ],
    )
    # to check that state machine can recover from database
    machine = StateMachine(db_path, modules)
    self.assertEqual(machine.state, StateMachineState.PROCESSING)
    handle_index_events = list(self._pop_all(machine.pop_handle_index_event))

    self.assertListEqual(
      list1=list(self._pop_all(machine.pop_removed_resource_event)),
      list2=[],
    )
    self.assertListEqual(
      list1=[(e.document_hash, e.document_path) for e in handle_index_events],
      list2=[
        (b"DOC-HASH-1", Path("doc-1.json")),
        (b"DOC-HASH-2", Path("doc-2.json")),
        (b"DOC-HASH-3", Path("doc-3.json")),
      ],
    )
    self.assertListEqual(
      list1=[
        (e.task_id, e.resource_hash, e.resource_path)
        for e in self._pop_all(machine.pop_preproc_event)
      ],
      list2=[(
        # preproc_events[0] is who wasn't the removed one
        preproc_events[0].task_id,
        preproc_events[0].resource_hash,
        preproc_events[0].resource_path,
      )],
    )
    machine.complete_preproc_task(
      event=preproc_events[0],
      document_descriptions=[],
    )
    for index_event in handle_index_events:
      machine.complete_index_task(index_event)

    self.assertListEqual(
      list1=list(self._pop_all(machine.pop_removed_resource_event)),
      list2=[],
    )
    machine.goto_scanning()
    machine.remove_resource(2, resource1)

    machine.goto_processing()
    self.assertListEqual(
      list1=list(self._pop_all(machine.pop_preproc_event)),
      list2=[],
    )
    self.assertListEqual(
      list1=list(self._pop_all(machine.pop_handle_index_event)),
      list2=[],
    )
    self.assertListEqual(
      list1=[
        (e.proto_event_id, e.hash)
        for e in self._pop_all(machine.pop_removed_resource_event)
      ],
      list2=[(2, b"HASH-1")],
    )

    machine.goto_scanning()
    machine.remove_resource(3, resource2)

    machine.goto_processing()
    self.assertListEqual(
      list1=list(self._pop_all(machine.pop_preproc_event)),
      list2=[],
    )
    self.assertListEqual(
      list1=[
        (e.document_hash, e.document_path)
        for e in self._pop_all(machine.pop_handle_index_event)
      ],
      list2=[
        (b"DOC-HASH-1", Path("doc-1.json")),
        (b"DOC-HASH-2", Path("doc-2.json")),
        (b"DOC-HASH-3", Path("doc-3.json")),
      ],
    )
    self.assertListEqual(
      list1=[
        (e.proto_event_id, e.hash)
        for e in self._pop_all(machine.pop_removed_resource_event)
      ],
      list2=[(3, b"HASH-2")],
    )

  def _pop_all(self, pop_fn: Callable[[], _T | None]) -> Generator[_T, None, None]:
    while True:
      item = pop_fn()
      if item is None:
        break
      yield item