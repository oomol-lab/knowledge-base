from __future__ import annotations
from abc import abstractmethod
from .module import Module
from .preprocessing import Document


class IndexModule(Module):
  @abstractmethod
  def create(self, id: int, document: Document):
    pass

  @abstractmethod
  def remove(self, id: int):
    pass