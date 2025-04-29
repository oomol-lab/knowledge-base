from typing import Any, Iterable, Generator
from os import PathLike
from pathlib import Path

from ..sqlite3_pool import ThreadPoolContext
from ..module import T, R, Module, KnowledgeBase, ResourceModule, PreprocessingModule, IndexModule
from ..state_machine import StateMachine, StateMachineState
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

    self._machine: StateMachine = StateMachine(
      db_path=Path(db_path),
      modules=modules,
    )
    self._scan_hub: ScanHub = ScanHub(
      state_machine=self._machine,
    )
    self._process_hub: ProcessHub = ProcessHub(
      state_machine=self._machine,
      preprocess_dir_path=Path(preprocess_path),
    )
    self._scan_workers: int = scan_workers
    self._process_workers: int = process_workers

  def scan(self) -> None:
    with ThreadPoolContext():
      if self._machine.state == StateMachineState.PROCESSING:
        self._process_hub.start_loop(self._scan_workers)
      self._scan_hub.start_loop(self._scan_workers)
      self._process_hub.start_loop(self._process_workers)
      self._machine.goto_setting()

  def get_knowledge_bases(self) -> Generator[KnowledgeBase, None, None]:
    yield from self._machine.get_knowledge_bases()

  def create_knowledge_base(
        self,
        resource_module: ResourceModule[T, R],
        resource_param: T,
        preproc_params: Iterable[tuple[PreprocessingModule, Any]] = (),
        index_params: Iterable[tuple[IndexModule, Any]] = (),
      ) -> KnowledgeBase[T, R]:

    return self._machine.create_knowledge_base(
      resource_param=(resource_module, resource_param),
      preproc_params=preproc_params,
      index_params=index_params,
    )