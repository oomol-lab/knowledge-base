import os
import uuid
import shutil

from pathlib import Path


class TempFolder:
  def __init__(self, base_path: Path) -> None:
    self._base_path: Path = base_path
    self._folder_name: str = ""

  @property
  def path(self) -> Path:
    return self._base_path.joinpath(self._folder_name)

  def __enter__(self):
    while True:
      self._folder_name = uuid.uuid4().hex
      if not self.path.exists():
        break
    os.makedirs(self.path)
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    shutil.rmtree(self.path)

class TempFolderHub:
  def __init__(self, base_path: Path) -> None:
    self._base_path: Path = base_path

  def create(self) -> TempFolder:
    return TempFolder(self._base_path)