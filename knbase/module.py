from __future__ import annotations

from io import BufferedReader
from typing import Any, Generator, Iterable
from dataclasses import dataclass
from abc import abstractmethod, ABC
from enum import Enum
from pathlib import Path
from json import dump, load

class Updating(Enum):
  CREATE = 0,
  UPDATE = 1,
  DELETE = 2,

class Module(ABC):
  def __init__(
        self,
        id: str,
      ) -> None:
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
    pass

  @abstractmethod
  def open(self, resource: Resource) -> BufferedReader:
    pass

  @abstractmethod
  def complete_event(self, event: ResourceEvent) -> None:
    pass

@dataclass
class Document:
  resource_hash: bytes
  meta: Any
  body: list[Fragment]

@dataclass
class Fragment:
  text: str
  meta: Any

def save_document(path: Path, document: Document) -> None:
  with path.open("w", encoding="utf-8") as fp:
    obj = {
      **document,
      "resource_hash": document.resource_hash.hex(),
    }
    dump(obj, fp, ensure_ascii=False)

def load_document(path: Path) -> Document:
  with path.open("r", encoding="utf-8") as fp:
    obj = load(fp)
    obj["resource_hash"] = bytes.fromhex(obj["resource_hash"])
    return Document(
      **obj,
      body=[Fragment(**f) for f in obj["body"]],
    )

@dataclass
class PreprocessingFile:
  hash: bytes
  path: Path

PreprocessingResult = tuple[Path, Updating]

class PreprocessingModule(Module):
  @abstractmethod
  def create(
    self,
    context: Path,
    file: PreprocessingFile,
    resource: Resource,
    recover: bool,
  ) -> Iterable[PreprocessingResult]:
    pass

  @abstractmethod
  def update(
    self,
    context: Path,
    file: PreprocessingFile,
    prev_file: PreprocessingFile,
    prev_cache: Path | None,
    resource: Resource,
    recover: bool,
  ) -> Iterable[PreprocessingResult]:
    pass

class IndexModule(Module):
  @abstractmethod
  def create(self, id: int, document: Document):
    pass

  @abstractmethod
  def remove(self, id: int):
    pass