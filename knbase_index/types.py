from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from knbase import KnowledgeBase, PreprocessingModule


class IndexNodeMatching(Enum):
  Matched = "matched"
  MatchedPartial = "matched_partial"
  Similarity = "similarity"

@dataclass
class IndexRow:
  base: KnowledgeBase
  preproc_module: PreprocessingModule
  document_hash: bytes
  matching: IndexNodeMatching
  metadata: dict
  fts5_rank: float
  vector_distance: float
  segments: list[IndexSegment]

@dataclass
class IndexNode:
  id: str
  type: str
  matching: IndexNodeMatching
  metadata: dict
  fts5_rank: float
  vector_distance: float
  segments: list[IndexSegment]

@dataclass
class IndexSegment:
  start: int
  end: int
  fts5_rank: float
  vector_distance: float
  matched_tokens: list[str]