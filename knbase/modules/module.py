from abc import ABC


class Module(ABC):
  def __init__(
        self,
        id: str,
      ) -> None:
    super().__init__()
    self._id: str = id

  @property
  def id(self) -> str:
    return self._id
