from os import PathLike
from pathlib import Path
from knbase import KnowledgeBasesHub
from knbase_file_scanner import FileScannerModule
from knbase_pdf_parser import PDFParserModule
from knbase_index import IndexDatabase


class Service:
  def __init__(self, app_path: PathLike):
    self._hub: KnowledgeBasesHub = self._create_hub(
      app_path=Path(app_path),
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