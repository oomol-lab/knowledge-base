from typing import Iterable
from os import PathLike
from pathlib import Path

from ..module import Module, KnowledgeBase
from ..state_machine import StateMachine


class KnowledgeBasesHub:
  def __init__(self, db_path: PathLike, modules: Iterable[Module]):
    self._knbases: dict[int, KnowledgeBase] = {}
    self._machine: StateMachine = StateMachine(
      db_path=Path(db_path),
      modules=modules,
    )
    for base in self._machine.get_knowledge_bases():
      self._knbases[base.id] = base