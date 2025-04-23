import os
import knbase

from os import PathLike
from typing import Generator, TypedDict
from pathlib import Path
from hashlib import sha256
from knbase import Updating, ResourceModule

from .scanner import Scanner
from .event_parser import Event, EventKind
from .events import EventTarget


class ResourceBaseMeta(TypedDict):
  name: str
  path: str

Resource = knbase.Resource[ResourceBaseMeta, None]
ResourceEvent = knbase.ResourceEvent[ResourceBaseMeta, None]
KnowledgeBase = knbase.KnowledgeBase[ResourceBaseMeta, None]

_SendedEventDict = dict[int, tuple[Event, ResourceEvent, bytes]]

class FileScannerModule(ResourceModule[ResourceBaseMeta, None]):
  def __init__(self, db_path: PathLike) -> None:
    super().__init__("file-scanner")
    self._scanner = Scanner(Path(db_path))
    self._base_sended_events: dict[int, _SendedEventDict] = {}

  def scan(self, base: KnowledgeBase) -> Generator[ResourceEvent, None, None]:
    base_path = base.resource_params["path"]
    event_ids = list(self._scanner.scan(
      base_id=base.id,
      base_path=base_path,
    ))
    for event_id in event_ids:
      event = self._scanner.parse_event(event_id)
      if event.target == EventTarget.File:
        event = self._transform_event(event, base, base_path)
        self._base_sended_events[event.id] = (event, event, event.resource.hash)
        yield event

  def complete_event(self, event: ResourceEvent) -> None:
    base_id = event.resource.base.id
    sended_events = self._base_sended_events.get(base_id, None)
    if sended_events is None:
      sended_events = {}
      self._base_sended_events[base_id] = sended_events

    sended = sended_events.pop(event.id, None)
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

  def _transform_event(self, event: Event, base: KnowledgeBase, base_path: str) -> ResourceEvent:
    resource_path = Path(os.path.join(base_path, event.path))
    resource_hash: bytes

    if event.kind in (EventKind.Added, EventKind.Updated):
      resource_hash = self._sha256(resource_path)
    else:
      resource_hash = event.removed_hash or b""

    resource = Resource(
      id=event.path,
      hash=resource_hash,
      base=base,
      content_type="TODO",
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
    chunk_size=65536
    with open(file_path, "rb") as f:
      for chunk in iter(lambda: f.read(chunk_size), b""):
        hash.update(chunk)
    return hash.digest()
