import os
from pathlib import Path


def ensure_db_file_not_exist(file_name: str) -> Path:
  base_path = os.path.join(__file__, "..", "..", "tests_temp", "framework")
  base_path = os.path.abspath(base_path)
  os.makedirs(base_path, exist_ok=True)

  file_path = Path(base_path).joinpath(file_name)
  if file_path.exists():
    os.remove(file_path)

  return file_path