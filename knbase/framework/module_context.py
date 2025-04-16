from enum import Enum
from sqlite3 import Cursor
from typing import Iterable

from .common import FRAMEWORK_DB, ConnSession
from ..sqlite3_pool import register_table_creators
from ..modules import Module, ResourceModule, PreprocessingModule, IndexModule


class _ModelStep(Enum):
  Resource = 0
  Preprocessing = 1
  Index = 2

class ModuleContext:
  def __init__(self, session: ConnSession, iter_modules: Iterable[Module]):
    modules, model2id = self._bind_modules(session, iter_modules)
    self._modules: dict[int, Module] = modules
    self._model2id: dict[str, int] = model2id

  def module(self, id: int) -> Module:
    return self._modules[id]

  def model_id(self, module: Module) -> int:
    return self._model2id[module.id]

  def _bind_modules(self, session: ConnSession, iter_modules: Iterable[Module]):
    cursor, conn = session
    modules: dict[int, Module] = {}
    model2id: dict[str, int] = {}

    for module in iter_modules:
      class_id = module.id
      cursor.execute(
        "SELECT id, step FROM modules WHERE class_id = ?",
        (class_id,),
      )
      id: int
      step: _ModelStep
      row = cursor.fetchone()

      if row is not None:
        id = row[0]
        step = _ModelStep(row[1])
        if step == _ModelStep.Resource:
          assert isinstance(module, ResourceModule), f"Expected ResourceModule for {class_id}"
        elif step == _ModelStep.Preprocessing:
          assert isinstance(module, PreprocessingModule), f"Expected PreprocessingModule for {class_id}"
        elif step == _ModelStep.Index:
          assert isinstance(module, IndexModule), f"Expected IndexModule for {class_id}"
      else:
        if isinstance(module, ResourceModule):
          step = _ModelStep.Resource
        elif isinstance(module, PreprocessingModule):
          step = _ModelStep.Preprocessing
        elif isinstance(module, IndexModule):
          step = _ModelStep.Index
        else:
          raise RuntimeError(f"Unknown module type: {type(module)}")
        cursor.execute(
          "INSERT INTO modules (step, class_id) VALUES (?, ?)",
          (step.value, class_id),
        )
        conn.commit()
        id = cursor.lastrowid

      modules[id] = module
      model2id[module.id] = id

    return modules, model2id

def _create_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE modules (
      id INTEGER PRIMARY KEY,
      step INTEGER NOT NULL,
      class_id TEXT NOT NULL,
    )
  """)

register_table_creators(FRAMEWORK_DB, _create_tables)