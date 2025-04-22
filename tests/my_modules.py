from io import BufferedReader
from typing import Any, Generator
from pathlib import Path

from knbase.module import (
  ResourceModule,
  PreprocessingModule,
  IndexModule,
  PreprocessingResult,
  Resource,
  ResourceEvent,
  KnowledgeBase,
)


class MyResourceModule(ResourceModule):
  def __init__(self):
    super().__init__("my_res")

  def scan(self, base: KnowledgeBase) -> Generator[ResourceEvent, None, None]:
    raise NotImplementedError()

  def open(self, resource: Resource) -> BufferedReader:
    raise NotImplementedError()

  def complete_event(self, event: ResourceEvent) -> None:
    raise NotImplementedError()

class MyPreprocessingModule(PreprocessingModule):
  def __init__(self):
    super().__init__("my_preproc")

  def preprocess(
        self,
        workspace_path: Path,
        latest_cache_path: Path | None,
        base_id: int,
        resource_hash: bytes,
        resource_path: Path,
        resource_content_type: str,
      ) -> list[PreprocessingResult]:
    raise NotImplementedError()

class MyIndexModule(IndexModule):
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
