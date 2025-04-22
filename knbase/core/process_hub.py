from typing import Callable
from pathlib import Path
from queue import Queue, Empty

from .thread_pool import ThreadPool
from .waker import WakerDidStop

from ..module import InterruptedException
from ..state_machine import (
  StateMachine,
  DocumentDescription,
  PreprocessingEvent,
  HandleIndexEvent,
  RemovedResourceEvent,
)


class ProcessHub:
  def __init__(
        self,
        state_machine: StateMachine,
        thread_pool: ThreadPool,
        preprocess_dir_path: Path,
      ) -> None:

    self._machine: StateMachine = state_machine
    self._thread_pool: ThreadPool = thread_pool
    self._preprocess_dir_path: Path = preprocess_dir_path
    self._main_invokers_queue: Queue[Callable[[], None]] = Queue(maxsize=0)

  def start_loop(self, workers: int) -> None:
    assert workers > 0
    self._machine.goto_processing()
    self._thread_pool.set_workers(workers)
    try:
      while True:
        while True:
          try:
            self._main_invokers_queue.get_nowait()()
          except Empty:
            break
        while True:
          event = self._machine.pop_removed_resource_event()
          if event is None:
            break
          self._thread_pool.execute(
            func=lambda: self._handle_removed_resource_event(event),
          )
        while True:
          event = self._machine.pop_handle_index_event()
          if event is None:
            break
          self._thread_pool.execute(
            func=lambda: self._handle_index_event(event),
          )
        event = self._machine.pop_preproc_event()
        if event is not None:
          self._thread_pool.execute(
            func=lambda: self._handle_preproc_event(event),
          )
    except WakerDidStop:
      pass

    finally:
      self._thread_pool.set_workers(0)

  # running in background thread
  def _handle_preproc_event(self, event: PreprocessingEvent):
    latest_cache_path: Path | None = None
    workspace_path = self._preprocess_dir_path.joinpath(
      str(event.base.id),
      event.module.id,
      event.resource_hash.hex(),
    )
    workspace_path.mkdir(parents=True, exist_ok=True)

    if event.from_resource_hash is not None:
      latest_cache_path = self._preprocess_dir_path.joinpath(
        str(event.base.id),
        event.module.id,
        event.from_resource_hash.hex(),
      )
      if not latest_cache_path.exists():
        latest_cache_path = None

    try:
      results = event.module.preprocess(
        workspace_path=workspace_path,
        latest_cache_path=latest_cache_path,
        resource_hash=event.resource_hash,
        resource_path=event.resource_path,
        resource_content_type=event.resource_content_type,
      )
    except InterruptedException:
      return

    except Exception as e:
      print(e)

    documents: list[DocumentDescription] = []
    for i, result in enumerate(results):
      base_path: Path
      if not result.from_cache:
        base_path = workspace_path
      else:
        if latest_cache_path is None:
          raise ValueError(f"[{i}].from_cache is True but latest_cache_path is None")
        base_path = latest_cache_path

      path = Path(result.path)
      if path.is_absolute():
        raise ValueError(f"[{i}].path must be relative")

      path = base_path.joinpath(path)
      documents.append(DocumentDescription(
        hash=result.hash,
        path=path,
        meta=result.meta,
      ))

    self._main_invokers_queue.put(
      item=lambda: self._machine.complete_preproc_task(
        event=event,
        document_descriptions=documents
      )
    )

  # running in background thread
  def _handle_index_event(self, event: HandleIndexEvent):
    pass

  # running in background thread
  def _handle_removed_resource_event(self, event: RemovedResourceEvent):
    pass
