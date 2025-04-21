from typing import Any
from dataclasses import dataclass
from pathlib import Path

from ..module import KnowledgeBase, PreprocessingModule, IndexModule
from .task_model import IndexTaskOperation


class PreprocessingEvent:
  proto_event_id: int
  task_id: int
  base: KnowledgeBase
  module: PreprocessingModule
  resource_hash: bytes
  from_resource_hash: bytes | None
  path: Path
  created_at: int

class HandleIndexEvent:
  proto_event_id: int
  task_id: int
  base: KnowledgeBase
  module: IndexModule
  operation: IndexTaskOperation
  created_at: int

@dataclass
class DocumentDescription:
  hash: bytes
  path: Path
  meta: Any