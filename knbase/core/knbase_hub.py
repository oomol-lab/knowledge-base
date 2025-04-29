from typing import Iterable, Generator
from os import PathLike
from pathlib import Path

from ..sqlite3_pool import ThreadPoolContext
from ..module import T, R, Module, KnowledgeBase, ResourceModule
from ..state_machine import StateMachine, StateMachineState
from ..interruption import Interruption
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
      ) -> None:

    self._interruption: Interruption = Interruption()
    self._machine: StateMachine = StateMachine(
      db_path=Path(db_path),
      modules=modules,
    )
    self._scan_hub: ScanHub = ScanHub(
      state_machine=self._machine,
      interruption=self._interruption,
    )
    self._process_hub: ProcessHub = ProcessHub(
      state_machine=self._machine,
      preprocess_dir_path=Path(preprocess_path),
      interruption=self._interruption,
    )
    self._scan_workers: int = scan_workers
    self._process_workers: int = process_workers

  def interrupt(self) -> None:
    self._interruption.interrupt()

  def scan(self) -> None:
    with ThreadPoolContext():
      if self._machine.state == StateMachineState.PROCESSING:
        self._process_hub.start_loop(self._scan_workers)
      self._scan_hub.start_loop(self._scan_workers)
      self._process_hub.start_loop(self._process_workers)
      self._machine.goto_setting()

  def get_knowledge_base(self, id: int) -> KnowledgeBase:
    with ThreadPoolContext():
      return self._machine.get_knowledge_base(id)

  def get_knowledge_bases(self) -> Generator[KnowledgeBase, None, None]:
    with ThreadPoolContext():
      yield from self._machine.get_knowledge_bases()

  def create_knowledge_base(
        self,
        resource_module: ResourceModule[T, R],
        resource_param: T,
      ) -> KnowledgeBase[T, R]:

    with ThreadPoolContext():
      return self._machine.create_knowledge_base(
        resource_param=(resource_module, resource_param),
      )

  def remove_knowledge_base(self, knbase: KnowledgeBase) -> None:
    with ThreadPoolContext():
      self._machine.clean_resources(-1, knbase)
      self._process_hub.start_loop(self._scan_workers)
      self._machine.goto_setting()
      self._machine.remove_knowledge_base(knbase)