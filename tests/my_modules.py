from typing import Any, Generator, Iterable, Callable
from pathlib import Path

from knbase.module import (
  ResourceModule,
  PreprocessingModule,
  IndexModule,
  PreprocessingResult,
  ResourceEvent,
  KnowledgeBase,
)


class MyResourceModule(ResourceModule[Any, Any]):
  def __init__(self, modules: Iterable[PreprocessingModule[Any] | IndexModule[Any]]):
    super().__init__("my_res")
    self._preprocess_modules: list[PreprocessingModule[Any]] = []
    self._index_modules: list[IndexModule[Any]] = []
    for module in modules:
      if isinstance(module, PreprocessingModule):
        self._preprocess_modules.append(module)
      elif isinstance(module, IndexModule):
        self._index_modules.append(module)
      else:
        raise TypeError(f"Unknown module type: {type(module)}")

  def scan(self, base: KnowledgeBase[Any, Any]) -> Generator[ResourceEvent[Any, Any], None, None]:
    raise NotImplementedError()

  def complete_event(self, event: ResourceEvent[Any, Any]) -> None:
    raise NotImplementedError()

  def complete_scanning(self, base: KnowledgeBase[None, None]) -> None:
    raise NotImplementedError()

  def preprocess_module_ids(self, base: KnowledgeBase[Any, Any], content_type: str) -> list[str]:
    return [module.id for module in self._preprocess_modules]

  def index_module_ids(self, base: KnowledgeBase[Any, Any]) -> list[str]:
    return [module.id for module in self._index_modules]

class MyPreprocessingModule(PreprocessingModule[Any]):
  def __init__(self):
    super().__init__("my_preproc")

  def acceptant(
        self,
        base_id: int,
        resource_hash: bytes,
        resource_path: Path,
        resource_content_type: str,
      ) -> bool:
    raise NotImplementedError()

  def preprocess(
        self,
        workspace_path: Path,
        latest_cache_path: Path | None,
        base_id: int,
        resource_hash: bytes,
        resource_path: Path,
        resource_content_type: str,
        report_progress: Callable[[float], None],
      ) -> list[PreprocessingResult[Any]]:
    raise NotImplementedError()

class MyIndexModule(IndexModule[Any]):
  def __init__(self):
    super().__init__("my_index")

  def add(
        self,
        base: KnowledgeBase,
        preproc_module: PreprocessingModule,
        document_hash: bytes,
        document_path: Path,
        document_meta: Any,
        report_progress: Callable[[float], None],
      ) -> None:
    raise NotImplementedError()

  def remove(
        self,
        base: KnowledgeBase,
        preproc_module: PreprocessingModule,
        document_hash: bytes,
        report_progress: Callable[[float], None],
      ) -> None:
    raise NotImplementedError()
