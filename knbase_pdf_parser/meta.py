from __future__ import annotations
from typing import TypedDict, Literal


class PDFMeta(TypedDict):
  kind: Literal["meta"]
  author: str | None
  modified_at: str | None
  producer: str | None

class PDFPage(TypedDict):
  kind: Literal["page"]
  page_index: int

class PDFAnnotation(TypedDict):
  kind: Literal["anno"]
  page_index: int
  anno: Annotation

class Annotation(TypedDict):
  type: str | None
  title: str
  uri: str
  created_at: str | None
  updated_at: str | None
  quad_points: list[float] | None
  extracted_text: str | None