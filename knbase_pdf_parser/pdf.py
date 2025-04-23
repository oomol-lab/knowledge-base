from __future__ import annotations

import os
import json
import shutil
import pikepdf

from typing import cast, Callable
from sqlite3 import Cursor, Connection
from dataclasses import dataclass
from knbase import assert_continue, InterruptedException
from knbase.sqlite3_pool import register_table_creators, SQLite3Pool

from .pdf_extractor import extract_metadata_with_pdf, PdfExtractor, Annotation
from .progress_events import PDFFileProgressEvent, PDFFileStep, ProgressEventListener
from .temp_folder import TempFolderHub
from .utils import get_sha256


@dataclass
class Pdf:
  hash: str
  metadata: PdfMetadata
  pages: list[PdfPage]

@dataclass
class PdfMetadata:
  author: str | None
  modified_at: str | None
  producer: str | None

class PdfPage:
  def __init__(
      self,
      pdf_id: int,
      index: int,
      hash: str,
      pages_path: str,
      extractor: PdfExtractor,
    ):
    self.index: int = index
    self.hash: str = hash
    self._pdf_id: int = pdf_id
    self._pages_path: str = pages_path
    self._extractor: PdfExtractor = extractor
    self._annotations: list[Annotation] | None = None
    self._snapshot: str | None = None

  @property
  def page_file_path(self) -> str:
    return os.path.join(self._pages_path, f"{self.hash}.pdf")

  @property
  def annotations(self) -> list[Annotation]:
    if self._annotations is None:
      extractor = cast(PdfExtractor, self._extractor)
      self._annotations = extractor.read_annotations(self.hash)
    return self._annotations

  @property
  def snapshot(self) -> str:
    if self._snapshot is None:
      extractor = cast(PdfExtractor, self._extractor)
      self._snapshot = extractor.read_snapshot(self.hash)
    return self._snapshot

# it's just for test unit now.
@dataclass
class PdfParserListeners:
  on_page_added: Callable[[str], None] = lambda _: None
  on_page_removed: Callable[[str], None] = lambda _: None

class PdfParser:
  def __init__(
    self,
    cache_dir_path: str,
    temp_dir_path: str,
    listeners: PdfParserListeners = PdfParserListeners(),
  ) -> None:
    db = SQLite3Pool(
      format_name="pdf",
      path=os.path.abspath(
        os.path.join(cache_dir_path, "pages.sqlite3"),
      ),
    )
    self._db: SQLite3Pool = db.assert_format("pdf")
    self._pages_path: str = os.path.abspath(
      os.path.join(cache_dir_path, "pages"),
    )
    self._temp_folders: TempFolderHub = TempFolderHub(temp_dir_path)
    self._extractor: PdfExtractor = PdfExtractor(self._pages_path)
    self._listeners: PdfParserListeners = listeners

    if not os.path.exists(self._pages_path):
      os.makedirs(self._pages_path, exist_ok=True)

  @property
  def name(self) -> str:
    return "pdf"

  @property
  def pages_path(self) -> str:
    return self._pages_path

  def page(self, page_hash: str) -> PdfPage | None:
    with self._db.connect() as (cursor, _):
      cursor.execute("SELECT pdf_id, idx FROM pages WHERE hash = ? LIMIT 1", (page_hash,))
      row = cursor.fetchone()
      if row is not None:
        pdf_id, index = row
        return PdfPage(
          pdf_id=pdf_id,
          index=index,
          hash=page_hash,
          pages_path=self._pages_path,
          extractor=self._extractor,
        )
      else:
        return None

  def pdf(self, hash: str, file_path: str, listener: ProgressEventListener) -> Pdf:
    with self._db.connect() as (cursor, conn):
      cursor.execute("SELECT id, meta FROM pdfs WHERE hash = ? LIMIT 1", (hash,))
      row = cursor.fetchone()

      if row is None:
        pdf_id, metadata = self._create_and_split_pdf(
          cursor, conn, hash, file_path, listener,
        )
      else:
        pdf_id, meta_json = row
        metadata = PdfMetadata(**json.loads(meta_json))

      return Pdf(
        hash=hash,
        metadata=metadata,
        pages=self._all_pages(cursor, pdf_id),
      )

  def pdf_or_none(self, hash: str) -> Pdf | None:
    with self._db.connect() as (cursor, _):
      pdf_id = self._pdf_id(cursor, hash)
      if pdf_id is None:
        return None

      cursor.execute("SELECT hash, meta FROM pdfs WHERE id = ? LIMIT 1", (pdf_id,))
      row = cursor.fetchone()
      if row is None:
        raise ValueError(f"pdf_id {pdf_id} not found")

      hash, meta_json = row
      metadata = PdfMetadata(**json.loads(meta_json))

      return Pdf(
        hash=hash,
        metadata=metadata,
        pages=self._all_pages(cursor, pdf_id),
      )

  def pdf_has_cached(self, hash: str) -> bool:
    with self._db.connect() as (cursor, _):
      pdf_id = self._pdf_id(cursor, hash)
      return pdf_id is not None

  def _all_pages(self, cursor: Cursor, pdf_id: int) -> list[PdfPage]:
    pdf_pages: list[PdfPage] = []
    rows = cursor.execute(
      "SELECT idx, hash FROM pages WHERE pdf_id = ? ORDER BY idx",
      (pdf_id,),
    )
    for row in rows.fetchall():
      index, page_hash = row
      pdf_page = PdfPage(
        pdf_id= pdf_id,
        index=index,
        hash=page_hash,
        pages_path=self._pages_path,
        extractor=self._extractor,
      )
      pdf_pages.append(pdf_page)

    return pdf_pages

  def _create_and_split_pdf(
      self,
      cursor: Cursor,
      conn: Connection,
      hash: str,
      file_path: str,
      listener: ProgressEventListener,
    ) -> tuple[int, PdfMetadata]:
    metadata = PdfMetadata(**extract_metadata_with_pdf(file_path))
    metadata_json = json.dumps(metadata.__dict__)
    added_page_hashes: list[str] = []
    try:
      cursor.execute("BEGIN TRANSACTION")
      cursor.execute("INSERT INTO pdfs (hash, meta) VALUES (?, ?)", (hash, metadata_json))
      pdf_id = cast(int, cursor.lastrowid)

      for index, page_hash in enumerate(self._extract_page_hashes(file_path)):
        cursor.execute(
          "INSERT INTO pages (pdf_id, hash, idx) VALUES (?, ?, ?)",
          (pdf_id, page_hash, index),
        )
        cursor.execute("SELECT COUNT(*) FROM pages WHERE hash = ?", (page_hash,))
        num_rows = cursor.fetchone()[0]
        if num_rows == 1:
          added_page_hashes.append(page_hash)

      for i, page_hash in enumerate(added_page_hashes):
        assert_continue()
        self._extractor.extract_page(page_hash)
        self._listeners.on_page_added(page_hash)
        listener(PDFFileProgressEvent(
          step=PDFFileStep.Parse,
          completed=i + 1,
          total=len(added_page_hashes),
        ))
      pages_count = len(added_page_hashes)
      listener(PDFFileProgressEvent(
        step=PDFFileStep.Parse,
        completed=pages_count,
        total=pages_count,
        ))
      conn.commit()
      return pdf_id, metadata

    except InterruptedException as e:
      for page_hash in added_page_hashes:
        self._extractor.remove_page(page_hash)
      conn.rollback()
      raise e

  # to clean useless cache files
  def fire_file_removed(self, hash: str):
    with self._db.connect() as (cursor, conn):
      pdf_id = self._pdf_id(cursor, hash)
      if pdf_id is None:
        return

      rows = cursor.execute(
        "SELECT hash FROM pages WHERE pdf_id = ? ORDER BY idx",
        (pdf_id,),
      )
      page_hashes: list[str] = [row[0] for row in rows]
      removed_page_hashes: list[str] = []

      try:
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("DELETE FROM pdfs WHERE id = ?", (pdf_id,))
        cursor.execute("DELETE FROM pages WHERE pdf_id = ?", (pdf_id,))
        conn.commit()
      except Exception as e:
        conn.rollback()
        raise e

      for page_hash in page_hashes:
        cursor.execute("SELECT id FROM pages WHERE hash = ? LIMIT 1", (page_hash,))
        if cursor.fetchone() is None:
          removed_page_hashes.append(page_hash)

      for page_hash in removed_page_hashes:
        self._extractor.remove_page(page_hash)
        self._listeners.on_page_removed(page_hash)

  def _extract_page_hashes(self, file_path: str) -> list[str]:
    page_hashes: list[str] = []

    with self._temp_folders.create() as folder:
      folder_path = folder.path
      pages_count: int = 0

      # https://pikepdf.readthedocs.io/en/latest/
      with pikepdf.Pdf.open(file_path) as pdf_file:
        for i, page in enumerate(pdf_file.pages):
          page_file = pikepdf.Pdf.new()
          page_file.pages.append(page)
          page_file_path = os.path.join(folder_path, f"{i}.pdf")
          page_file.save(
            page_file_path,
            # make sure hash of file never changes
            deterministic_id=True,
          )
        pages_count = len(pdf_file.pages)

      for i in range(pages_count):
        page_file_path = os.path.join(folder_path, f"{i}.pdf")
        page_hash = get_sha256(page_file_path)
        page_hashes.append(page_hash)
        target_page_path = os.path.join(self._pages_path, f"{page_hash}.pdf")

        if os.path.exists(target_page_path):
          if os.path.isdir(target_page_path):
            shutil.rmtree(target_page_path)
          else:
            os.remove(target_page_path)

        shutil.move(page_file_path, target_page_path)

    return page_hashes

  def _pdf_id(self, cursor: Cursor, hash: str) -> int | None:
    cursor.execute("SELECT id FROM pdfs WHERE hash = ? LIMIT 1", (hash,))
    row = cursor.fetchone()
    if row is None:
      return None
    else:
      return row[0]

def _create_tables(cursor: Cursor):
  cursor.execute("""
    CREATE TABLE pdfs (
      id INTEGER PRIMARY KEY,
      hash TEXT NOT NULL,
      meta TEXT NOT NULL
    )
  """)
  cursor.execute("""
    CREATE TABLE pages (
      id INTEGER PRIMARY KEY,
      pdf_id TEXT NOT NULL,
      hash TEXT NOT NULL,
      idx INTEGER NOT NULL
    )
  """)
  cursor.execute("""
    CREATE INDEX idx_pdfs ON pdfs (hash)
  """)
  cursor.execute("""
    CREATE INDEX idx_pdf_pages ON pages (pdf_id, idx)
  """)
  cursor.execute("""
    CREATE INDEX idx__hash_pages ON pages (hash)
  """)

register_table_creators("pdf", _create_tables)