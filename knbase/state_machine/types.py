from typing import Any, TypeVar, Generic
from dataclasses import dataclass
from pathlib import Path

from ..module import KnowledgeBase, PreprocessingModule, IndexModule
from .task_model import IndexTaskOperation


M = TypeVar("M")

@dataclass
class DocumentDescription(Generic[M]):
  base: KnowledgeBase
  preproc_module: PreprocessingModule
  resource_hash: bytes
  document_hash: bytes
  path: Path
  meta: M

@dataclass
class PreprocessingEvent:
  proto_event_id: int
  task_id: int
  base: KnowledgeBase
  module: PreprocessingModule
  resource_hash: bytes
  from_resource_hash: bytes | None
  resource_path: Path
  resource_content_type: str
  created_at: int

@dataclass
class HandleIndexEvent:
  proto_event_id: int
  task_id: int
  base: KnowledgeBase
  preproc_module: PreprocessingModule
  index_module: IndexModule
  operation: IndexTaskOperation
  document_hash: bytes
  document_path: Path
  document_meta: Any
  created_at: int

@dataclass
class RemovedResourceEvent:
  proto_event_id: int
  hash: bytes
  base: KnowledgeBase