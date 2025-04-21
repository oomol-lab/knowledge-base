from typing import Any
from dataclasses import dataclass
from pathlib import Path

from ..module import KnowledgeBase, PreprocessingModule, IndexModule
from .task_model import IndexTaskOperation


@dataclass
class DocumentDescription:
  hash: bytes
  path: Path
  meta: Any

@dataclass
class PreprocessingEvent:
  proto_event_id: int
  task_id: int
  base: KnowledgeBase
  module: PreprocessingModule
  resource_hash: bytes
  from_resource_hash: bytes | None
  path: Path
  created_at: int

@dataclass
class HandleIndexEvent:
  proto_event_id: int
  task_id: int
  base: KnowledgeBase
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