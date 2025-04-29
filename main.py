import os
import shutil
import signal

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

  pdf_parser_module = PDFParserModule()
  index_db = IndexDatabase(base_path=temp_path)
  file_scanner_module = FileScannerModule(
    db_path=os.path.join(temp_path, "file-scanner.sqlite3"),
    preprocess_modules_map={"*": pdf_parser_module},
    index_modules=[*index_db.modules],
  )
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
  signal.signal(
    signalnum=signal.SIGINT,
    handler=lambda _1, _2: knbases_hub.interrupt(),
  )
  knbases_hub.create_knowledge_base(
    resource_module=file_scanner_module,
    resource_param={
      "path": knbase_path,
    },
  )
  knbases_hub.scan()
  for row in index_db.query("一带一路", 5):
    print(row.matching.name, row.metadata)
    for segment in row.segments:
      content = " ".join(segment.matched_tokens)
      print(f"  [{segment.start}-{segment.end}] fts5={segment.fts5_rank} vector={segment.vector_distance}")
      print("    ", content)

if __name__ == "__main__":
  main()