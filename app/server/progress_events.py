from dataclasses import dataclass
from typing import Generator
from enum import Enum, IntEnum
from threading import Lock
from queue import Queue, Empty


class ProgressPhase(IntEnum):
  READY = 0
  SCANNING = 1
  HANDING_FILES = 2
  COMPLETED = 3

class InterruptionStatus(IntEnum):
  NO = 0
  INTERRUPTING = 1
  INTERRUPTED = 2

class HandleFileOperation(Enum):
  CREATE = "create"
  UPDATE = "update"
  REMOVE = "remove"

@dataclass
class HandingFile:
  path: str
  operation: HandleFileOperation
  pdf_handing: tuple[int, int] | None = None
  pdf_indexing: tuple[int, int] | None = None

@dataclass
class File:
  path: str
  operation: HandleFileOperation

class ProgressEvents:
  def __init__(self):
    self._phase: ProgressPhase = ProgressPhase.READY
    self._status_lock: Lock = Lock()
    self._updated_files: int = 0
    self._handing_file: HandingFile | None = None
    self._error: str | None = None
    self._interruption_status: InterruptionStatus = InterruptionStatus.NO
    self._completed_files: list[File] = []
    self._fetcher_lock: Lock = Lock()
    self._fetcher_queues: list[Queue[dict]] = []

  def notify_scanning(self):
    with self._status_lock:
      if self._phase != ProgressPhase.READY:
        self._updated_files = 0
        self._handing_file = None
        self._error = None
        self._interruption_status = InterruptionStatus.NO
        self._completed_files.clear()
      self._phase = ProgressPhase.SCANNING

    self._emit_event({
      "kind": "scanning",
    })

  def _init_events(self) -> list[dict]:
    with self._status_lock:
      if self._phase == ProgressPhase.READY:
        return []

      if self._phase == ProgressPhase.SCANNING:
        return [{ "kind": "scanning" }]

      events: list[dict] = []
      events.append({
        "kind": "scanCompleted",
        "count": self._updated_files,
      })
      for file in self._completed_files:
        events.append({
          "kind": "completeHandingFile",
          "path": file.path,
          "operation": file.operation.value,
        })
      if self._phase == ProgressPhase.COMPLETED:
        events.append({ "kind": "completed" })

      elif self._phase == ProgressPhase.HANDING_FILES and \
           self._handing_file is not None:
        events.append({
          "kind": "startHandingFile",
          "path": self._handing_file.path,
          "operation": self._handing_file.operation.value,
        })
        if self._handing_file.pdf_handing is not None:
          index, total = self._handing_file.pdf_handing
          events.append({
            "kind": "completeParsePdfPage",
            "index": index,
            "total": total,
          })
        if self._handing_file.pdf_indexing is not None:
          index, total = self._handing_file.pdf_indexing
          events.append({
            "kind": "completeIndexPdfPage",
            "index": index,
            "total": total,
          })

      if self._error is not None:
        events.append({
          "kind": "failure",
          "error": self._error or "",
        })
      elif self._interruption_status == InterruptionStatus.INTERRUPTING:
        events.append({ "kind": "interrupting" })
      elif self._interruption_status == InterruptionStatus.INTERRUPTED:
        events.append({ "kind": "interrupted" })

      return events

  def notify_scan_completed(self, updated_files: int):
    with self._status_lock:
      self._phase = ProgressPhase.HANDING_FILES
      self._updated_files = updated_files

    self._emit_event({
      "kind": "scanCompleted",
      "count": updated_files,
    })

  def notify_start_handle_file(self, path: str, operation: HandleFileOperation):
    with self._status_lock:
      self._handing_file = HandingFile(
        path=path,
        operation=operation,
      )
    self._emit_event({
      "kind": "startHandingFile",
      "path": path,
      "operation": operation.value,
    })

  # TODO: 恢复之前的逻辑，让 scan 结束后就结束
  def notify_complete_handle_file(self, path: str):
    file: File | None = None
    with self._status_lock:
      if self._handing_file is not None and self._handing_file.path == path:
        file = File(
          path=path,
          operation=self._handing_file.operation,
        )
        self._completed_files.append(file)
        self._handing_file = None

    if file is not None:
      self._emit_event({
        "kind": "completeHandingFile",
        "path": file.path,
        "operation": file.operation.value,
      })

  def notify_pdf_parse_progress(self, page_index: int, total_pages: int):
    with self._status_lock:
      if self._handing_file is not None:
        self._handing_file.pdf_handing = (page_index, total_pages)

    self._emit_event({
      "kind": "completeParsePdfPage",
      "index": page_index,
      "total": total_pages,
    })

  def notify_pdf_index_progress(self, page_index: int, total_pages: int):
    with self._status_lock:
      if self._handing_file is not None:
        self._handing_file.pdf_indexing = (page_index, total_pages)

    self._emit_event({
      "kind": "completeIndexPdfPage",
      "index": page_index,
      "total": total_pages,
    })

  def notify_complete(self):
    with self._status_lock:
      self._phase = ProgressPhase.COMPLETED
      self._handing_file = None

    self._emit_event({
      "kind": "completed",
    })

  def set_interrupting(self):
    with self._status_lock:
      if self._interruption_status == InterruptionStatus.NO:
        self._interruption_status = InterruptionStatus.INTERRUPTING

    self._emit_event({ "kind": "interrupting" })

  def set_interrupted(self):
    with self._status_lock:
      self._interruption_status = InterruptionStatus.INTERRUPTED

    self._emit_event({ "kind": "interrupted" })

  def fail(self, error: str):
    with self._status_lock:
      self._error = error

    self._emit_event({
      "kind": "failure",
      "error": error,
    })

  def fetch_events(self) -> Generator[dict, None, None]:
    queue: Queue[dict] = Queue()
    with self._fetcher_lock:
      init_events = self._init_events()
      self._fetcher_queues.append(queue)
    try:
      for event in init_events:
        yield event
      init_events.clear()
      while True:
        try:
          yield queue.get(timeout=5.0)
        except Empty:
          yield { "kind": "heartbeat"}
    finally:
      with self._fetcher_lock:
        self._fetcher_queues.remove(queue)

  def _emit_event(self, event: dict) -> None:
    with self._fetcher_lock:
      for queue in self._fetcher_queues:
        queue.put(event)
