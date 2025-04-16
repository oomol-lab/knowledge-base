from __future__ import annotations
from io import BufferedReader
from abc import abstractmethod
from typing import Any, Generator
from dataclasses import dataclass
from .module import Module
from .types import Updating


@dataclass
class Resource:
  id: int
  module: ResourceModule
  base: ResourceBase
  content_type: str
  meta: Any

  def open(self) -> BufferedReader:
    return self.module.open(self)

@dataclass
class ResourceBase:
  id: int
  module: ResourceModule
  meta: Any

  def scan(self) -> Generator[ResourceEvent, None, None]:
    return self.module.scan(self)

@dataclass
class ResourceEvent:
  id: int
  resource: Resource
  updating: Updating
  hash: bytes

  def complete(self) -> None:
    self.resource.module.complete_event(self)

class ResourceModule(Module):
  @abstractmethod
  def scan(self, base: ResourceBase) -> Generator[ResourceEvent, None, None]:
    pass

  @abstractmethod
  def open(self, resource: Resource) -> BufferedReader:
    pass

  @abstractmethod
  def complete_event(self, event: ResourceEvent) -> None:
    pass