import os
import shutil

from knbase import KnowledgeBasesHub
from knbase_file_scanner import FileScannerModule
from knbase_pdf_parser import PDFParserModule
from knbase_index import IndexDatabase


def main() -> None:
  temp_path = os.path.join(__file__, "..", "temp")
  temp_path = os.path.abspath(temp_path)
  knbase_path = "/Users/taozeyu/Downloads/TestPaper2/"

  if os.path.exists(temp_path):
    shutil.rmtree(temp_path)
  os.makedirs(temp_path)

  file_scanner_module = FileScannerModule(
    db_path=os.path.join(temp_path, "file-scanner.sqlite3"),
  )
  pdf_parser_module = PDFParserModule()
  index_db = IndexDatabase(base_path=temp_path)
  knbases_hub = KnowledgeBasesHub(
    db_path=os.path.join(temp_path, "main.sqlite3"),
    preprocess_path=os.path.join(temp_path, "preprocess"),
    scan_workers=2,
    process_workers=2,
    modules=(
      file_scanner_module,
      pdf_parser_module,
      *index_db.modules,
    ),
  )
  knbases_hub.create_knowledge_base(
    resource_module=file_scanner_module,
    resource_param={
      "path": knbase_path,
    },
    preproc_params=[(
      pdf_parser_module,
      None,
    )],
    index_params=[
      (module, None)
      for module in index_db.modules
    ],
  )
  knbases_hub.scan()

if __name__ == "__main__":
  main()