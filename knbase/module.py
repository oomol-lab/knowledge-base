from __future__ import annotations

from io import BufferedReader
from os import PathLike
from typing import Any, Generator
from dataclasses import dataclass
from abc import abstractmethod, ABC
from enum import Enum
from pathlib import Path


class InterruptedException(Exception):
  pass

class Updating(Enum):
  CREATE = 0,
  UPDATE = 1,
  DELETE = 2,

class Module(ABC):
  def __init__(self, id: str) -> None:
    super().__init__()
    self._id: str = id

  @property
  def id(self) -> str:
    return self._id

@dataclass
class ProcessRecord:
  id: int
  module: PreprocessingModule | IndexModule
  params: Any

@dataclass
class KnowledgeBase:
  id: int
  resource_params: Any
  resource_module: ResourceModule
  process_records: list[ProcessRecord]

  @property
  def preproc_modules(self) -> list[PreprocessingModule]:
    return [
      record.module
      for record in self.process_records
      if isinstance(record.module, PreprocessingModule)
    ]

  @property
  def index_modules(self) -> list[IndexModule]:
    return [
      record.module
      for record in self.process_records
      if isinstance(record.module, IndexModule)
    ]

@dataclass
class Resource:
  id: int
  hash: bytes
  base: KnowledgeBase
  content_type: str
  meta: Any
  updated_at: int

  def open(self) -> BufferedReader:
    return self.base.resource_module.open(self)

@dataclass
class ResourceEvent:
  id: int
  resource: Resource
  updating: Updating

  def complete(self) -> None:
    self.resource.base.resource_module.complete_event(self)

class ResourceModule(Module):
  @abstractmethod
  def scan(self, base: KnowledgeBase) -> Generator[ResourceEvent, None, None]:
    raise NotImplementedError()

  @abstractmethod
  def open(self, resource: Resource) -> BufferedReader:
    raise NotImplementedError()

  @abstractmethod
  def complete_event(self, event: ResourceEvent) -> None:
    raise NotImplementedError()

@dataclass
class PreprocessingResult:
  hash: bytes
  path: PathLike
  meta: Any
  from_cache: bool = False

class PreprocessingModule(Module):
  @abstractmethod
  def preprocess(
      self,
      workspace_path: Path,
      latest_cache_path: Path | None,
      resource_hash: bytes,
      resource_path: Path,
      resource_content_type: str,
    ) -> list[PreprocessingResult]:
    raise NotImplementedError()

class IndexModule(Module):
  @abstractmethod
  def create(self, id: int):
    raise NotImplementedError()

  @abstractmethod
  def remove(self, id: int):
    raise NotImplementedError()