from io import BufferedReader
from typing import Generator
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
      resource: Resource,
    ) -> list[PreprocessingResult]:
    raise NotImplementedError()

class MyIndexModule(IndexModule):
  def __init__(self):
    super().__init__("my_index")

  def create(self, id: int):
    raise NotImplementedError()

  def remove(self, id: int):
    raise NotImplementedError()
