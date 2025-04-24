import shutil
import pikepdf
import pdfplumber
import knbase

from typing import Generator
from pathlib import Path
from json import loads, dumps
from knbase import load_document, save_document

from .temp_folder import TempFolderHub
from .extractor import extract_snapshot, extract_annotations, extract_metadata_with_pdf
from .meta import PDFMeta, PDFPage, PDFAnnotation
from .utils import get_sha256


_PDF_EXT = ".pdf"
_SNAPSHOT_EXT = ".snapshot.yaml"
_ANNOTATION_EXT = ".annotation.yaml"

PreprocessingResult = knbase.PreprocessingResult[PDFMeta | PDFPage | PDFAnnotation]

class PDFParserModule(knbase.PreprocessingModule[PDFMeta | PDFPage | PDFAnnotation]):
  def __init__(self):
    super().__init__("pdf-parser")

  def acceptant(
        self,
        base_id: int,
        resource_hash: bytes,
        resource_path: Path,
        resource_content_type: str,
      ) -> bool:
    return resource_content_type == "application/pdf"

  def preprocess(
        self,
        workspace_path: Path,
        latest_cache_path: Path | None,
        base_id: int,
        resource_hash: bytes,
        resource_path: Path,
        resource_content_type: str,
      ) -> list[PreprocessingResult]:

    results: list[PreprocessingResult] = []
    temp_folder = TempFolderHub(
      base_path=workspace_path.joinpath("temp"),
    )
    with temp_folder.create() as folder:
      hash2origin_indexes: dict[str, int] = {}

      if latest_cache_path is not None:
        latest_json_path = latest_cache_path.joinpath("pages.json")
        with open(latest_json_path, "r", encoding="utf-8") as file:
          for i, hash in enumerate(loads(file.read())):
            hash2origin_indexes[hash] = i

      output_path = workspace_path.joinpath("output")
      output_path.mkdir(parents=True, exist_ok=True)

      page_hashes = self._split_pages(
        pdf_path=resource_path,
        pages_path=folder.path,
        json_path=workspace_path.joinpath("pages.json"),
      )
      last_output_path: Path | None = None

      for i, page_hash in enumerate(page_hashes):
        origin_index = hash2origin_indexes.get(page_hash, -1)
        if origin_index < 0:
          for doc_path, hash, meta in self._extract_documents(
            page_index=i,
            input_dir=folder.path,
            output_dir=output_path,
          ):
            results.append(PreprocessingResult(
              hash=hash,
              path=doc_path.relative_to(workspace_path),
              meta=meta,
              from_cache=False,
            ))
        else:
          if last_output_path is None:
            last_output_path = latest_cache_path.joinpath("output")
          for name, path, hash, meta in self._search_documents_files_and_meta(
            page_index=origin_index,
            search_dir=last_output_path,
          ):
            target_path: Path = path
            if i != origin_index:
              meta.page_index = i
              target_path = output_path.joinpath(name)
              shutil.copy(path, target_path)

            results.append(PreprocessingResult(
              hash=hash,
              path=target_path.relative_to(workspace_path),
              meta=meta,
              from_cache=(i == origin_index),
            ))

      with pdfplumber.open(resource_path) as pdf:
        pdf_meta = extract_metadata_with_pdf(pdf)
        doc_path = workspace_path.joinpath("meta.yaml")
        doc = save_document(
          file_path=doc_path,
          meta=pdf_meta,
        )
        results.append(PreprocessingResult(
          hash=doc.hash,
          path=doc_path.relative_to(workspace_path),
          meta=pdf_meta,
          from_cache=False,
        ))

    return results

  def _split_pages(self, pdf_path: Path, pages_path: Path, json_path: Path) -> list[str]:
    # https://pikepdf.readthedocs.io/en/latest/
    with pikepdf.Pdf.open(pdf_path) as pdf_file:
      for i, page in enumerate(pdf_file.pages):
        page_file = pikepdf.Pdf.new()
        page_file.pages.append(page)
        page_file_path = pages_path.joinpath(f"{i}.pdf")
        page_file.save(
          page_file_path,
          # make sure hash of file never changes
          deterministic_id=True,
        )
      pages_count: int = len(pdf_file.pages)

    page_hashes: list[str] = []
    for i in range(pages_count):
      page_file_path = pages_path.joinpath(f"{i}.pdf")
      page_hash = get_sha256(page_file_path)
      page_hashes.append(page_hash)

    with open(json_path, "w", encoding="utf-8") as file:
      file.write(dumps(
        obj=page_hashes,
        indent=2,
        ensure_ascii=False,
      ))
    return page_hashes

  def _extract_documents(
        self,
        page_index: int,
        input_dir: Path,
        output_dir: Path,
      ) -> Generator[tuple[Path, PDFPage | PDFAnnotation, bytes], None, None]:

    with pdfplumber.open(input_dir.joinpath(f"{page_index}{_PDF_EXT}")) as pdf:
      if len(pdf.pages) == 0:
        return
      page = pdf.pages[0]
      snapshot = extract_snapshot(page)
      file_path = output_dir.joinpath(f"{page_index}{_SNAPSHOT_EXT}")
      doc = save_document(
        file_path=file_path,
        content=snapshot,
        meta=None,
      )
      yield file_path, doc.hash, PDFPage(
        kind="page",
        page_index=page_index,
        meta=doc.meta,
      )

      for i, (content, meta) in enumerate(extract_annotations(page)):
        file_path = output_dir.joinpath(f"{page_index}-{i}{_ANNOTATION_EXT}")
        doc = save_document(
          file_path=file_path,
          content=content,
          meta=meta,
        )
        yield file_path, doc.hash, PDFAnnotation(
          kind="anno",
          page_index=page_index,
          anno=doc.meta,
        )

  def _search_documents_files_and_meta(
        self,
        page_index: int,
        search_dir: Path,
      ) -> Generator[tuple[str, Path, PDFPage | PDFAnnotation, bytes], None, None]:

    file_name = f"{page_index}{_SNAPSHOT_EXT}"
    file_path = search_dir.joinpath(file_name)
    if file_path.exists():
      doc = load_document(file_path)
      yield file_name, file_path, doc.hash, PDFPage(
        kind="page",
        page_index=page_index,
      )

    i: int = 0
    while True:
      file_name = f"{page_index}-{i}{_ANNOTATION_EXT}"
      file_path = search_dir.joinpath(file_name)
      if not file_path.exists():
        break
      doc = load_document(file_path)
      yield file_name, file_path, doc.hash, PDFAnnotation(
        kind="anno",
        page_index=page_index,
        anno=doc.meta,
      )
      i += 1
