from typing import Iterable
from os import PathLike
from pathlib import Path

from ..sqlite3_pool import build_thread_pool, release_thread_pool
from ..module import Module
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
    build_thread_pool()
    try:
      if self._machine.state == StateMachineState.PROCESSING:
        self._process_hub.start_loop(self._scan_workers)
      self._scan_hub.start_loop(self._scan_workers)
      self._process_hub.start_loop(self._process_workers)
    finally:
      release_thread_pool()