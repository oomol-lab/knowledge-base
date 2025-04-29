from os import PathLike
from pathlib import Path
from typing import Any

from knbase import KnowledgeBasesHub
from knbase_file_scanner import FileScannerModule
from knbase_pdf_parser import PDFParserModule
from knbase_index import IndexDatabase


class Service:
  def __init__(self, app_path: PathLike):
    app_path = Path(app_path)
    if not app_path.exists():
      app_path.mkdir(parents=True)
    elif not app_path.is_dir():
      raise NotADirectoryError(f"{app_path} is not a directory")

    pdf_parser_module = PDFParserModule()
    index_db = IndexDatabase(base_path=app_path)

    self._file_scanner_module: FileScannerModule = FileScannerModule(
      db_path=app_path.joinpath("file-scanner.sqlite3"),
      preprocess_modules_map={"*": pdf_parser_module},
      index_modules=[*index_db.modules],
    )
    self._hub: KnowledgeBasesHub = KnowledgeBasesHub(
      db_path=app_path.joinpath("main.sqlite3"),
      preprocess_path=app_path.joinpath("preprocess"),
      scan_workers=2,
      process_workers=2,
      modules=(
        self._file_scanner_module,
        pdf_parser_module,
        *index_db.modules,
      ),
    )

  def _create_hub(self, app_path: Path) -> KnowledgeBasesHub:
    if not app_path.exists():
      app_path.mkdir(parents=True)
    elif not app_path.is_dir():
      raise NotADirectoryError(f"{app_path} is not a directory")

    pdf_parser_module = PDFParserModule()
    index_db = IndexDatabase(base_path=app_path)
    file_scanner_module = FileScannerModule(
      db_path=app_path.joinpath("file-scanner.sqlite3"),
      preprocess_modules_map={"*": pdf_parser_module},
      index_modules=[*index_db.modules],
    )
    return KnowledgeBasesHub(
      db_path=app_path.joinpath("main.sqlite3"),
      preprocess_path=app_path.joinpath("preprocess"),
      scan_workers=2,
      process_workers=2,
      modules=(
        file_scanner_module,
        pdf_parser_module,
        *index_db.modules,
      ),
    )

  def bases(self) -> list[dict[str, Any]]:
    return [
      {
        "id": base.id,
        "name": base.resource_params["name"],
        "path": base.resource_params["path"],
      }
      for base in self._hub.get_knowledge_bases()
    ]

  def create_base(self, name: str | None, path: str) -> dict[str, Any]:
    path = path.rstrip(r"/\\")
    if any(
      path == base.resource_params["path"]
      for base in self._hub.get_knowledge_bases()
    ):
      raise ValueError(f"Base with path {path} already exists")

    if name is None:
      name = path.split(r"/")[-1].split(r"\\")[-1]

    base = self._hub.create_knowledge_base(
      resource_module=self._file_scanner_module,
      resource_param={
        "name": name,
        "path": path,
      },
    )
    return {
      "id": base.id,
      "name": name,
      "path": path,
    }

  def remove_base(self, id: int):
    base = self._hub.get_knowledge_base(id)
    self._hub.remove_knowledge_base(base)