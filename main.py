import os
import shutil

from knbase import KnowledgeBasesHub
from knbase_file_scanner import FileScannerModule


def main() -> None:
  temp_path = os.path.join(__file__, "..", "temp")
  temp_path = os.path.abspath(temp_path)

  if os.path.exists(temp_path):
    shutil.rmtree(temp_path)
    os.makedirs(temp_path)

  file_scanner_module = FileScannerModule(
    db_path=os.path.join(temp_path, "file-scanner.sqlite3"),
  )
  knbases_hub = KnowledgeBasesHub(
    db_path=os.path.join(temp_path, "main.sqlite3"),
    preprocess_path=os.path.join(temp_path, "preprocess"),
    scan_workers=2,
    process_workers=2,
    modules=(file_scanner_module,),
  )
  knbases_hub.scan()

if __name__ == "__main__":
  main()