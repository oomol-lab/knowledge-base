from __future__ import annotations

from os import PathLike
from typing import Generator, TypeVar, Generic, Callable
from dataclasses import dataclass
from enum import Enum
from abc import abstractmethod, ABC
from pathlib import Path


T = TypeVar("T")
R = TypeVar("R")

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
class KnowledgeBase(Generic[T, R]):
  id: int
  resource_params: T
  resource_module: ResourceModule[T, R]

@dataclass
class Resource(Generic[T, R]):
  id: str
  hash: bytes
  base: KnowledgeBase[T, R]
  content_type: str
  meta: R
  updated_at: int

@dataclass
class ResourceEvent(Generic[T, R]):
  id: int
  resource: Resource[T, R]
  resource_path: PathLike
  updating: Updating

class ResourceModule(Module, Generic[T, R]):
  @abstractmethod
  def scan(self, base: KnowledgeBase[T, R]) -> Generator[ResourceEvent[T, R], None, None]:
    raise NotImplementedError()

  @abstractmethod
  def complete_event(self, event: ResourceEvent[T, R]) -> None:
    raise NotImplementedError()

  @abstractmethod
  def complete_scanning(self, base: KnowledgeBase[T, R]) -> None:
    raise NotImplementedError()

  @abstractmethod
  def preprocess_module_ids(self, base: KnowledgeBase[T, R], content_type: str) -> list[str]:
    raise NotImplementedError()

  @abstractmethod
  def index_module_ids(self, base: KnowledgeBase[T, R]) -> list[str]:
    raise NotImplementedError()

@dataclass
class PreprocessingResult(Generic[T]):
  hash: bytes
  path: PathLike
  meta: T
  from_cache: bool = False

class PreprocessingModule(Module, Generic[T]):
  @abstractmethod
  def acceptant(
        self,
        base_id: int,
        resource_hash: bytes,
        resource_path: Path,
        resource_content_type: str,
      ) -> bool:
    raise NotImplementedError()

  @abstractmethod
  def preprocess(
        self,
        workspace_path: Path,
        latest_cache_path: Path | None,
        base_id: int,
        resource_hash: bytes,
        resource_path: Path,
        resource_content_type: str,
        report_progress: Callable[[float], None],
      ) -> list[PreprocessingResult[T]]:
    raise NotImplementedError()

class IndexModule(Module, Generic[T]):
  @abstractmethod
  def add(
        self,
        base: KnowledgeBase,
        preproc_module: PreprocessingModule,
        document_hash: bytes,
        document_path: Path,
        document_meta: T,
        report_progress: Callable[[float], None],
      ) -> None:
    raise NotImplementedError()

  @abstractmethod
  def remove(
        self,
        base: KnowledgeBase,
        preproc_module: PreprocessingModule,
        document_hash: bytes,
        report_progress: Callable[[float], None],
      ) -> None:
    raise NotImplementedError()