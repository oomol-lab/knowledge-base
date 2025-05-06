from __future__ import annotations
from pathlib import Path
from typing import Generator
from dataclasses import dataclass
from knbase import KnowledgeBasesHub
from knbase_pdf_parser import PDFPage
from knbase_index import IndexDatabase


@dataclass
class QueryResult:
  items: list[QueryItem]
  keywords: list[str]

@dataclass
class PdfQueryItem:
  pdf_files: list[str]
  distance: float
  metadata: PdfMetadata

@dataclass
class PdfMetadata:
  author: str | None
  modified_at: str | None
  producer: str | None

@dataclass
class PageQueryItem:
  pdf_files: list[PagePDFFile]
  distance: float
  content: str
  segments: list[PageHighlightSegment]
  annotations: list[PageAnnoQueryItem]

@dataclass
class PagePDFFile:
  scope: str
  path: str
  device_path: str
  page_index: int

@dataclass
class PageHighlightSegment:
  start: int
  end: int
  main: bool
  highlights: list[tuple[int, int]]

@dataclass
class PageAnnoQueryItem:
  index: int
  distance: float
  content: str
  segments: list[PageHighlightSegment]

QueryItem = PdfQueryItem | PageQueryItem

class Query:
  def __init__(
        self,
        hub: KnowledgeBasesHub,
        index_db: IndexDatabase,
      ) -> None:

    self._hub: KnowledgeBasesHub = hub
    self._index_db: IndexDatabase = index_db

  def do(self, query: str, results_limit: int) -> QueryResult:
    return QueryResult(
      items=list(self._search_page_query_items(query, results_limit)),
      keywords=[query],
    )

  def _search_page_query_items(self, query: str, results_limit: int) -> Generator[PageQueryItem, None, None]:
    for row in self._index_db.query(query, results_limit):
      document = self._hub.get_document(
        base=row.base,
        preproc_module=row.preproc_module,
        hash=row.document_hash,
      )
      if not document:
        continue

      meta: dict = document.meta
      kind = meta["kind"]
      if kind != "page":
        continue

      pdf_page: PDFPage = meta

      yield PageQueryItem(
        distance=row.vector_distance,
        content="Test Text",
        segments=[],
        annotations=[],
        pdf_files=[
          PagePDFFile(
            scope=r.base.id,
            path="/foobar/",
            device_path="/foobar/",
            page_index=pdf_page["page_index"],
          )
          for r in self._hub.get_resources(row.base, document.resource_hash)
        ],
      )