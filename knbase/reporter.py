from typing import Callable
from threading import Lock

from .state_machine import (
  PreprocessingEvent,
  HandleIndexEvent,
  DocumentDescription,
  IndexTaskOperation,
)

from .module import (
  Event,
  Updating,
  KnowledgeBase,
  DocumentInfo,
  ResourceEvent,
  ScanBeginEvent,
  ScanCompleteEvent,
  ScanResourceEvent,
  ScanFailEvent,
  PreprocessingBeginEvent,
  PreprocessingProgressEvent,
  PreprocessingCompleteEvent,
  PreprocessingFailEvent,
  HandleIndexBeginEvent,
  HandleIndexProgressEvent,
  HandleIndexCompleteEvent,
  HandleIndexFailEvent,
)


# thread safe
class EventReporter:
  def __init__(self, listener: Callable[[Event], None] | None) -> None:
    self._listener: Callable[[Event], None] = listener
    self._next_id_lock: Lock = Lock()
    self._next_id: int = 0

  def report_scan_begin(self, base: KnowledgeBase) -> int:
    if self._listener is None:
      return -1

    id = self._generate_id()
    self._listener(ScanBeginEvent(
      id=id,
      base=base,
    ))
    return id

  def report_scan_done(self, id: int, base: KnowledgeBase, error: Exception | None = None) -> None:
    if self._listener is None:
      return

    if error is None:
      self._listener(ScanCompleteEvent(
        id=id,
        base=base,
      ))
    else:
      self._listener(ScanFailEvent(
        id=id,
        base=base,
        error=error,
      ))

  def report_resource_event(self, event: ResourceEvent) -> None:
    if self._listener is None:
      return

    self._listener(ScanResourceEvent(
      id=self._generate_id(),
      base=event.resource.base,
      path=event.resource_path,
      hash=event.resource.hash,
      content_type=event.resource.content_type,
      updating=event.updating,
    ))

  def report_preproc_begin(self, event: PreprocessingEvent) -> int:
    if self._listener is None:
      return -1

    id = self._generate_id()
    self._listener(PreprocessingBeginEvent(
      id=id,
      base=event.base,
      path=event.resource_path,
      hash=event.resource_hash,
      content_type=event.resource_content_type,
      module=event.module,
    ))
    return id

  def report_preproc_progress(self, event: PreprocessingEvent, progress: float) -> None:
    if self._listener is None:
      return

    self._listener(PreprocessingProgressEvent(
      id=event.proto_event_id,
      base=event.base,
      path=event.resource_path,
      hash=event.resource_hash,
      content_type=event.resource_content_type,
      progress=progress,
    ))

  def report_preproc_done(
        self,
        id: int,
        event: PreprocessingEvent,
        target: Exception | list[DocumentDescription],
      ) -> None:

    if self._listener is None:
      return

    if isinstance(target, Exception):
      self._listener(PreprocessingFailEvent(
        id=id,
        base=event.base,
        path=event.resource_path,
        hash=event.resource_hash,
        content_type=event.resource_content_type,
        module=event.module,
        error=target,
      ))
    else:
      self._listener(PreprocessingCompleteEvent(
        id=id,
        base=event.base,
        path=event.resource_path,
        hash=event.resource_hash,
        content_type=event.resource_content_type,
        module=event.module,
        documents=[DocumentInfo(hash=doc.hash) for doc in target],
      ))

  def report_handle_index_begin(self, event: HandleIndexEvent) -> int:
    if self._listener is None:
      return -1

    id = self._generate_id()
    self._listener(HandleIndexBeginEvent(
      id=id,
      base=event.base,
      hash=event.document_hash,
      module=event.module,
      updating=self._operation_to_updating(event.operation),
    ))
    return id

  def report_handle_index_progress(self, event: HandleIndexEvent, progress: float) -> None:
    if self._listener is None:
      return

    self._listener(HandleIndexProgressEvent(
      id=event.proto_event_id,
      base=event.base,
      hash=event.document_hash,
      module=event.module,
      updating=self._operation_to_updating(event.operation),
      progress=progress,
    ))

  def report_handle_index_done(
        self,
        id: int,
        event: HandleIndexEvent,
        error: Exception | None,
      ) -> None:

    if self._listener is None:
      return

    if error is None:
      self._listener(HandleIndexCompleteEvent(
        id=id,
        base=event.base,
        hash=event.document_hash,
        module=event.module,
        updating=self._operation_to_updating(event.operation),
      ))
    else:
      self._listener(HandleIndexFailEvent(
        id=id,
        base=event.base,
        hash=event.document_hash,
        module=event.module,
        updating=self._operation_to_updating(event.operation),
        error=error,
      ))

  def _operation_to_updating(self, operation: IndexTaskOperation) -> Updating:
    if operation == IndexTaskOperation.CREATE:
      return Updating.CREATE
    elif operation == IndexTaskOperation.REMOVE:
      return Updating.DELETE
    else:
      raise ValueError(f"Unknown operation: {operation}")

  def _generate_id(self) -> int:
    with self._next_id_lock:
      id = self._next_id
      self._next_id += 1
      return id