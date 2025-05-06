from typing import Iterable, Generator, Callable
from os import PathLike
from pathlib import Path

from ..sqlite3_pool import ThreadPoolContext
from ..reporter import EventReporter
from ..state_machine import StateMachine, StateMachineState, DocumentDescription
from ..interruption import Interruption
from ..module import (
  T, R,
  Event,
  Module,
  KnowledgeBase,
  Resource,
  ResourceModule,
  PreprocessingModule,
  IndexModule,
)

from .scan_hub import ScanHub
from .process_hub import ProcessHub


class KnowledgeBasesHub:
  def __init__(
        self,
        db_path: PathLike,
        preprocess_path: PathLike,
        scan_workers: int,
        process_workers: int,
        modules: Iterable[Module],
        listener: Callable[[Event], None] | None = None,
      ) -> None:

    reporter = EventReporter(listener)
    self._interruption: Interruption = Interruption()
    self._machine: StateMachine = StateMachine(
      db_path=Path(db_path),
      modules=modules,
    )
    self._scan_hub: ScanHub = ScanHub(
      state_machine=self._machine,
      interruption=self._interruption,
      reporter=reporter,
    )
    self._process_hub: ProcessHub = ProcessHub(
      state_machine=self._machine,
      preprocess_dir_path=Path(preprocess_path),
      interruption=self._interruption,
      reporter=reporter,
    )
    self._scan_workers: int = scan_workers
    self._process_workers: int = process_workers

  def resource_module(self, id: str) -> ResourceModule:
    return self._machine.module_context.resource_module(id)

  def preproc_module(self, id: str) -> PreprocessingModule:
    return self._machine.module_context.preproc_module(id)

  def index_module(self, id: str) -> IndexModule:
    return self._machine.module_context.index_module(id)

  def interrupt(self) -> None:
    self._interruption.interrupt()

  def scan(self) -> None:
    with ThreadPoolContext():
      if self._machine.state == StateMachineState.PROCESSING:
        self._process_hub.start_loop(self._scan_workers)
      self._scan_hub.start_loop(self._scan_workers)
      self._process_hub.start_loop(self._process_workers)
      self._machine.goto_setting()

  def get_resources(self, base: KnowledgeBase[T, R], hash: bytes) -> Generator[Resource[T, R], None, None]:
    yield from self._machine.get_resources(base, hash)

  def get_document(
      self,
      base: KnowledgeBase[T, R],
      preproc_module: PreprocessingModule[T],
      hash: bytes) -> DocumentDescription | None:

    return self._machine.get_document(
      base=base,
      preproc_module=preproc_module,
      document_hash=hash,
    )

  def get_knowledge_base(self, id: int) -> KnowledgeBase:
    return self._machine.get_knowledge_base(id)

  def get_knowledge_bases(self) -> Generator[KnowledgeBase, None, None]:
    yield from self._machine.get_knowledge_bases()

  def create_knowledge_base(
        self,
        resource_module: ResourceModule[T, R],
        resource_param: T,
      ) -> KnowledgeBase[T, R]:
    return self._machine.create_knowledge_base(
      resource_param=(resource_module, resource_param),
    )

  def remove_knowledge_base(self, knbase: KnowledgeBase) -> None:
    with ThreadPoolContext():
      self._machine.clean_resources(-1, knbase)
      self._process_hub.start_loop(self._scan_workers)
      self._machine.goto_setting()
      self._machine.remove_knowledge_base(knbase)