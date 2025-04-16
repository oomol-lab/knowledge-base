from __future__ import annotations
from abc import abstractmethod
from typing import Any, Iterable
from pathlib import Path
from json import dump, load
from dataclasses import dataclass
from .module import Module
from .resource import Resource
from .types import Updating


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