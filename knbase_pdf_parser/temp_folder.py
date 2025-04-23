import os
import uuid
import shutil


class TempFolder:
  def __init__(self, base_path: str) -> None:
    self._base_path: str = base_path
    self._folder_name: str = ""

  @property
  def path(self) -> str:
    return os.path.join(self._base_path, self._folder_name)

  def __enter__(self):
    while True:
      self._folder_name = uuid.uuid4().hex
      if not os.path.exists(self.path):
        break
    os.makedirs(self.path)
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    shutil.rmtree(self.path)

class TempFolderHub:
  def __init__(self, base_path: str) -> None:
    self._base_path: str = base_path

  def create(self) -> TempFolder:
    return TempFolder(self._base_path)