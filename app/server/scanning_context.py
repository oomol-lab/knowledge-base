from threading import Lock
from pathlib import Path
from knbase import (
  Event,
  KnowledgeBase,
  Updating,
  ScanCompleteEvent,
  ScanFailEvent,
  ScanResourceEvent,
  PreprocessingBeginEvent,
  PreprocessingCompleteEvent,
  PreprocessingFailEvent,
  HandleIndexBeginEvent,
  HandleIndexCompleteEvent,
  HandleIndexFailEvent,
)

from .progress_events import ProgressEvents, HandleFileOperation


class ScanningContext:
  def __init__(self, progress_events: ProgressEvents) -> None:
    self._lock: Lock = Lock()
    self._progress_events = progress_events
    self._updated_files_count: int = 0
    self._last_resource_ref: tuple[KnowledgeBase, Path] | None = None
    self._preproc_tasks_count: int = 0
    self._index_tasks_count: int = 0

  def notify_start(self) -> None:
    with self._lock:
      self._updated_files_count = 0
      self._last_resource_ref = None
      self._progress_events.notify_scanning()

  def notify_complete(self) -> None:
    with self._lock:
      if self._last_resource_ref is not None:
        _, path = self._last_resource_ref
        self._last_resource_ref = None
        self._preproc_tasks_count = 0
        self._index_tasks_count = 0
        self._progress_events.notify_complete_handle_file(
          path=str(path),
        )
      self._progress_events.notify_complete()

  def notify_scanning_event(self, event: Event) -> None:
    with self._lock:
      if isinstance(event, ScanCompleteEvent | ScanFailEvent):
        if isinstance(event, ScanFailEvent):
          print(event.error)
        self._progress_events.notify_scan_completed(
          updated_files=self._updated_files_count,
        )
      elif isinstance(event, ScanResourceEvent):
        if self._last_resource_ref is not None:
          _, path = self._last_resource_ref
          self._preproc_tasks_count = 0
          self._index_tasks_count = 0
          self._progress_events.notify_complete_handle_file(
            path=str(path),
          )
        self._last_resource_ref = (event.base, event.path)
        self._progress_events.notify_start_handle_file(
          path=str(event.path),
          operation=self._to_operation(event.updating),
        )
      elif isinstance(event, PreprocessingBeginEvent):
        self._preproc_tasks_count += 1
        self._progress_events.notify_pdf_parse_progress(
          page_index=self._preproc_tasks_count,
          total_pages=self._preproc_tasks_count + 1,
        )
      elif isinstance(event, PreprocessingCompleteEvent):
        self._progress_events.notify_pdf_parse_progress(
          page_index=self._preproc_tasks_count + 1,
          total_pages=self._preproc_tasks_count + 1,
        )
      elif isinstance(event, PreprocessingFailEvent):
        print(event.error)

      elif isinstance(event, HandleIndexBeginEvent):
        self._index_tasks_count += 1
        self._progress_events.notify_pdf_index_progress(
          page_index=self._index_tasks_count,
          total_pages=self._index_tasks_count + 1,
        )
      elif isinstance(event, HandleIndexCompleteEvent):
        self._progress_events.notify_pdf_index_progress(
          page_index=self._index_tasks_count + 1,
          total_pages=self._index_tasks_count + 1,
        )
      elif isinstance(event, HandleIndexFailEvent):
        print(event.error)

  def _to_operation(self, updating: Updating) -> HandleFileOperation:
    if updating == Updating.CREATE:
      return HandleFileOperation.CREATE
    elif updating == Updating.UPDATE:
      return HandleFileOperation.UPDATE
    elif updating == Updating.DELETE:
      return HandleFileOperation.REMOVE
    else:
      raise ValueError(f"Unknown updating type: {updating}")