import pikepdf
import knbase

from typing import TypedDict
from pathlib import Path
from json import loads, dumps

from .temp_folder import TempFolderHub
from .utils import get_sha256


class PDFDocumentMeta(TypedDict):
  pass

PreprocessingResult = knbase.PreprocessingResult[PDFDocumentMeta]

class PDFParserModule(knbase.PreprocessingModule[PDFDocumentMeta]):
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
      base_path=str(workspace_path.joinpath("temp")),
    )
    with temp_folder.create() as folder:
      hash2origin_indexes: dict[str, int] = {}
      last_output_path: Path = Path(".")
      if latest_cache_path is not None:
        latest_json_path = latest_cache_path.joinpath("pages.json")
        last_output_path = latest_cache_path.joinpath("output")
        with open(latest_json_path, "r", encoding="utf-8") as file:
          for i, hash in enumerate(loads(file.read())):
            hash2origin_indexes[hash] = i

      output_path = workspace_path.joinpath("output")
      output_path.mkdir(parents=True, exist_ok=True)

      page_hashes = self._extract_pages(
        pdf_path=resource_path,
        pages_path=folder.path,
        json_path=workspace_path.joinpath("pages.json"),
      )
      for i, page_hash in enumerate(page_hashes):
        origin_index = hash2origin_indexes.get(page_hash, -1)
        # TODO:
        if origin_index < 0:
          pass

  def _extract_pages(self, pdf_path: Path, pages_path: Path, json_path: Path) -> list[str]:
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