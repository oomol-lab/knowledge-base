from typing import Any, Generator
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
  def __init__(self):
    super().__init__("my_res")

  def scan(self, base: KnowledgeBase[Any, Any]) -> Generator[ResourceEvent[Any, Any], None, None]:
    raise NotImplementedError()

  def complete_event(self, event: ResourceEvent[Any, Any]) -> None:
    raise NotImplementedError()

  def complete_scanning(self, base: KnowledgeBase[None, None]) -> None:
    raise NotImplementedError()

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
      ) -> list[PreprocessingResult[Any]]:
    raise NotImplementedError()

class MyIndexModule(IndexModule[Any]):
  def __init__(self):
    super().__init__("my_index")

  def add(
        self,
        base_id: int,
        document_hash: bytes,
        document_path: Path,
        document_meta: Any,
      ) -> None:
    raise NotImplementedError()

  def remove(
        self,
        base_id: int,
        document_hash: bytes,
      ) -> None:
    raise NotImplementedError()
