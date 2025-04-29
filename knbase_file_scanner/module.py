import os
import knbase

from os import PathLike
from typing import Generator, TypedDict
from pathlib import Path
from hashlib import sha256
from knbase import Updating, ResourceModule, PreprocessingModule, IndexModule

from .scanner import Scanner
from .event_parser import Event, EventKind
from .events import EventTarget
from .mime import get_content_type


class ResourceBaseMeta(TypedDict):
  name: str
  path: str

Resource = knbase.Resource[ResourceBaseMeta, None]
ResourceEvent = knbase.ResourceEvent[ResourceBaseMeta, None]
KnowledgeBase = knbase.KnowledgeBase[ResourceBaseMeta, None]

_SendedEventDict = dict[int, tuple[Event, ResourceEvent, bytes]]

class FileScannerModule(ResourceModule[ResourceBaseMeta, None]):
  def __init__(
        self,
        db_path: PathLike,
        preprocess_modules_map: dict[str, PreprocessingModule | list[PreprocessingModule]],
        index_modules: list[IndexModule],
      ) -> None:

    super().__init__("file-scanner")
    self._scanner = Scanner(Path(db_path))
    self._index_modules: list[IndexModule] = list(index_modules)
    self._preprocess_modules_map: dict[str, list[PreprocessingModule]] = {}
    self._base_sended_events: dict[int, _SendedEventDict] = {}

    for k, v in preprocess_modules_map.items():
      if isinstance(v, PreprocessingModule):
        self._preprocess_modules_map[k] = [v]
      elif isinstance(v, list):
        self._preprocess_modules_map[k] = v
      else:
        raise TypeError(f"Invalid type for preprocess module: {type(v)}")

  def scan(self, base: KnowledgeBase) -> Generator[ResourceEvent, None, None]:
    base_path = base.resource_params["path"]
    sended_events = self._sended_events(base.id)
    event_ids = list(self._scanner.scan(
      base_id=base.id,
      base_path=base_path,
    ))
    for event_id in event_ids:
      e = self._scanner.parse_event(event_id)
      if e.target == EventTarget.Directory:
        e.close()
      elif e.target == EventTarget.File:
        event = self._transform_event(e, base, base_path)
        sended_events[e.id] = (e, event, event.resource.hash)
        yield event

  def complete_event(self, event: ResourceEvent) -> None:
    base_id = event.resource.base.id
    sended = self._sended_events(base_id).pop(event.id, None)
    if sended is None:
      return

    e, origin_event, hash = sended
    if origin_event != event:
      return

    e.close(hash)

  def complete_scanning(self, base: KnowledgeBase) -> None:
    sended_events = self._base_sended_events.pop(base.id, None)
    if sended_events is not None:
      for e, _, hash in sended_events.values():
        e.close(hash)

  def preprocess_module_ids(self, base: KnowledgeBase, content_type: str) -> list[str]:
    preprocess_modules = self._preprocess_modules_map.get(content_type, None)
    if preprocess_modules is None:
      preprocess_modules = self._preprocess_modules_map.get("*", None)
      if preprocess_modules is None:
        return []
    return [module.id for module in preprocess_modules]

  def index_module_ids(self, base: KnowledgeBase) -> list[str]:
    return [module.id for module in self._index_modules]

  def _sended_events(self, base_id: int) -> _SendedEventDict:
    sended_events = self._base_sended_events.get(base_id, None)
    if sended_events is None:
      sended_events = {}
      self._base_sended_events[base_id] = sended_events
    return sended_events

  def _transform_event(self, event: Event, base: KnowledgeBase, base_path: str) -> ResourceEvent:
    resource_path = Path(os.path.join(base_path, f".{event.path}"))
    resource_hash: bytes

    if event.kind in (EventKind.Added, EventKind.Updated):
      resource_hash = self._sha256(resource_path)
    else:
      resource_hash = event.removed_hash or b""

    content_type = get_content_type(resource_path)
    resource = Resource(
      id=event.path,
      hash=resource_hash,
      base=base,
      content_type=content_type,
      meta=None,
      updated_at=event.mtime,
    )
    updating: Updating
    if event.kind == EventKind.Added:
      updating = Updating.CREATE
    elif event.kind == EventKind.Updated:
      updating = Updating.UPDATE
    elif event.kind == EventKind.Removed:
      updating = Updating.DELETE

    return ResourceEvent(
      id=event.id,
      resource=resource,
      resource_path=resource_path,
      updating=updating,
    )

  def _sha256(self, file_path: Path) -> bytes:
    hash = sha256()
    chunk_size = 8192
    with open(file_path, "rb") as f:
      for chunk in iter(lambda: f.read(chunk_size), b""):
        hash.update(chunk)
    return hash.digest()
