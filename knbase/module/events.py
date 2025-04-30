from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from .modules import KnowledgeBase, Updating, PreprocessingModule, IndexModule


@dataclass
class ScanBeginEvent:
  id: int
  base: KnowledgeBase

@dataclass
class ScanCompleteEvent:
  id: int
  base: KnowledgeBase

@dataclass
class ScanFailEvent:
  id: int
  base: KnowledgeBase
  error: Exception

@dataclass
class ScanResourceEvent:
  id: int
  base: KnowledgeBase
  path: Path
  hash: bytes
  content_type: str
  updating: Updating

@dataclass
class PreprocessingBeginEvent:
  id: int
  base: KnowledgeBase
  path: Path
  hash: bytes
  content_type: str
  module: PreprocessingModule

@dataclass
class PreprocessingProgressEvent:
  id: int
  base: KnowledgeBase
  path: Path
  hash: bytes
  content_type: str
  progress: float

@dataclass
class PreprocessingCompleteEvent:
  id: int
  base: KnowledgeBase
  path: Path
  hash: bytes
  content_type: str
  module: PreprocessingModule
  documents: list[DocumentInfo]

@dataclass
class DocumentInfo:
  hash: bytes

@dataclass
class PreprocessingFailEvent:
  id: int
  base: KnowledgeBase
  path: Path
  hash: bytes
  content_type: str
  module: PreprocessingModule
  error: Exception

@dataclass
class HandleIndexBeginEvent:
  id: int
  base: KnowledgeBase
  hash: bytes
  module: IndexModule
  updating: Updating

@dataclass
class HandleIndexProgressEvent:
  id: int
  base: KnowledgeBase
  hash: bytes
  module: IndexModule
  updating: Updating
  progress: float

@dataclass
class HandleIndexCompleteEvent:
  id: int
  base: KnowledgeBase
  hash: bytes
  module: IndexModule
  updating: Updating

@dataclass
class HandleIndexFailEvent:
  id: int
  base: KnowledgeBase
  hash: bytes
  module: IndexModule
  updating: Updating
  error: Exception

Event = (
  ScanBeginEvent |
  ScanCompleteEvent |
  ScanFailEvent |
  ScanResourceEvent |
  PreprocessingBeginEvent |
  PreprocessingProgressEvent |
  PreprocessingCompleteEvent |
  PreprocessingFailEvent |
  HandleIndexBeginEvent |
  HandleIndexProgressEvent |
  HandleIndexCompleteEvent |
  HandleIndexFailEvent
)