from enum import Enum
from typing import Iterable
from sqlite3 import Cursor

from .common import FRAMEWORK_DB
from ..sqlite3_pool import register_table_creators
from ..module import Module, ResourceModule, PreprocessingModule, IndexModule


class _ModelStep(Enum):
  Resource = 0
  Preprocessing = 1
  Index = 2

class ModuleContext:
  def __init__(self, cursor: Cursor, iter_modules: Iterable[Module]):
    modules, module2id = self._bind_modules(cursor, iter_modules)
    self._modules: dict[int, Module] = modules
    self._module2id: dict[str, int] = module2id

  def module(self, id: int) -> Module:
    return self._modules[id]

  def module_id(self, module: Module) -> int:
    return self._module2id[module.id]

  def resource_module(self, id: str) -> ResourceModule:
    module = self._str_id_2_module(id)
    if not isinstance(module, ResourceModule):
      raise TypeError(f"Module {id} is not a ResourceModule")
    return module

  def preproc_module(self, id: str) -> PreprocessingModule:
    module = self._str_id_2_module(id)
    if not isinstance(module, PreprocessingModule):
      raise TypeError(f"Module {id} is not a PreprocessingModule")
    return module

  def index_module(self, id: str) -> IndexModule:
    module = self._str_id_2_module(id)
    if not isinstance(module, IndexModule):
      raise TypeError(f"Module {id} is not an IndexModule")
    return module

  def _bind_modules(self, cursor: Cursor, iter_modules: Iterable[Module]):
    modules: dict[int, Module] = {}
    module2id: dict[str, int] = {}

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
        id = cursor.lastrowid

      modules[id] = module
      module2id[module.id] = id

    return modules, module2id

  def _str_id_2_module(self, id: str) -> Module:
    int_id = self._module2id.get(id, None)
    if int_id is None:
      raise ValueError(f"Module with id {id} not found")
    return self._modules[int_id]

def _create_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE modules (
      id INTEGER PRIMARY KEY,
      step INTEGER NOT NULL,
      class_id TEXT NOT NULL
    )
  """)

register_table_creators(FRAMEWORK_DB, _create_tables)