from typing import Callable
from pathlib import Path
from shutil import rmtree

from .thread_pool import ThreadPool, ExecuteSuccess, ExecuteFail, NoMoreExecutions
from .waker import WakerDidStop

from ..reporter import EventReporter
from ..interruption import Interruption, InterruptedException
from ..state_machine import (
  StateMachine,
  DocumentDescription,
  PreprocessingEvent,
  HandleIndexEvent,
  IndexTaskOperation,
  RemovedResourceEvent,
)


class ProcessHub:
  def __init__(
        self,
        state_machine: StateMachine,
        interruption: Interruption,
        preprocess_dir_path: Path,
        reporter: EventReporter,
      ) -> None:

    self._machine: StateMachine = state_machine
    self._interruption: Interruption = interruption
    self._preprocess_dir_path: Path = preprocess_dir_path
    self._reporter: EventReporter = reporter
    self._thread_pool: ThreadPool[None | Callable[[], None]] = ThreadPool(interruption)

  def start_loop(self, workers: int) -> None:
    assert workers > 0
    self._machine.goto_processing()
    self._thread_pool.set_workers(workers)

    try:
      is_clear1: bool = False
      is_clear2: bool = False
      while not is_clear1 or not is_clear2:
        try:
          is_clear1 = self._handle_events_from_machine()
        except InterruptedException as e:
          is_clear2 = self._handle_callback_events()
          raise e
        is_clear2 = self._handle_callback_events()

    except WakerDidStop:
      pass

    finally:
      self._thread_pool.set_workers(0)

  def _handle_events_from_machine(self) -> bool:
    is_clear = True

    while True:
      event = self._machine.pop_removed_resource_event()
      if event is None:
        break
      self._thread_pool.execute(
        func=lambda e=event: self._handle_removed_resource_event(e),
      )
      is_clear = False
      self._interruption.assert_continue()

    while True:
      event = self._machine.pop_handle_index_event()
      if event is None:
        break
      self._thread_pool.execute(
        func=lambda e=event: self._handle_index_event(e),
      )
      is_clear = False
      self._interruption.assert_continue()

    event = self._machine.pop_preproc_event()
    if event is not None:
      self._thread_pool.execute(
        func=lambda e=event: self._handle_preproc_event(e),
      )
      is_clear = False
      self._interruption.assert_continue()

    return is_clear

  def _handle_callback_events(self) -> bool:
    is_clear = True

    while True:
      result = self._thread_pool.pop_result()
      if isinstance(result, NoMoreExecutions):
        break

      elif isinstance(result, ExecuteFail):
        is_clear = False
        print(result.error)

      elif isinstance(result, ExecuteSuccess):
        is_clear = False
        callback_in_main = result.result
        if callback_in_main is not None:
          callback_in_main()

    return is_clear

  # running in background thread
  def _handle_removed_resource_event(self, event: RemovedResourceEvent) -> None:
    resource_dir_path = self._preprocess_dir_path.joinpath(
      str(event.base.id),
      event.hash.hex(),
    )
    rmtree(resource_dir_path, ignore_errors=True)

  # running in background thread
  def _handle_preproc_event(self, event: PreprocessingEvent) -> None | Callable[[], None]:
    if not event.module.acceptant(
      base_id=event.base.id,
      resource_hash=event.resource_hash,
      resource_path=event.resource_path,
      resource_content_type=event.resource_content_type,
    ):
      return None

    documents: list[DocumentDescription] = []
    event_id: int
    error: Exception | None = None
    latest_cache_path: Path | None = None
    workspace_path = self._preprocess_dir_path.joinpath(
      str(event.base.id),
      event.resource_hash.hex(),
      event.module.id,
    )
    workspace_path.mkdir(parents=True, exist_ok=True)

    if event.from_resource_hash is not None:
      latest_cache_path = self._preprocess_dir_path.joinpath(
        str(event.base.id),
        event.from_resource_hash.hex(),
        event.module.id,
      )
      if not latest_cache_path.exists():
        latest_cache_path = None

    try:
      event_id = self._reporter.report_preproc_begin(event)
      results = event.module.preprocess(
        workspace_path=workspace_path,
        latest_cache_path=latest_cache_path,
        base_id=event.base.id,
        resource_hash=event.resource_hash,
        resource_path=event.resource_path,
        resource_content_type=event.resource_content_type,
        report_progress=lambda progress: self._reporter.report_preproc_progress(
          event=event,
          progress=progress,
        ),
      )
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
          base=event.base,
          preproc_module=event.module,
          resource_hash=event.resource_hash,
          document_hash=result.hash,
          path=path,
          meta=result.meta,
        ))
    except InterruptedException:
      return None

    except Exception as e:
      error = e
      raise e

    finally:
      if error is None:
        self._reporter.report_preproc_done(event_id, event, documents)
      else:
        self._reporter.report_preproc_done(event_id, event, error)

    return lambda: self._machine.complete_preproc_task(
      event=event,
      document_descriptions=documents
    )

  # running in background thread
  def _handle_index_event(self, event: HandleIndexEvent) -> None | Callable[[], None]:
    event_id: int
    error: Exception | None = None
    try:
      event_id = self._reporter.report_handle_index_begin(event)
      if event.operation == IndexTaskOperation.CREATE:
        event.index_module.add(
          base=event.base,
          preproc_module=event.preproc_module,
          document_hash=event.document_hash,
          document_path=event.document_path,
          document_meta=event.document_meta,
          report_progress=lambda progress: self._reporter.report_handle_index_progress(
            event=event,
            progress=progress,
          ),
        )
      elif event.operation == IndexTaskOperation.REMOVE:
        event.index_module.remove(
          base_id=event.base,
          preproc_module=event.preproc_module,
          document_hash=event.document_hash,
          document_path=event.document_path,
          report_progress=lambda progress: self._reporter.report_handle_index_progress(
            event=event,
            progress=progress,
          ),
        )
      else:
        raise ValueError(f"Unknown operation: {event.operation}")

    except InterruptedException:
      return None

    except Exception as e:
      error = e
      raise e

    finally:
      self._reporter.report_handle_index_done(
        id=event_id,
        event=event,
        error=error,
      )

    return lambda: self._machine.complete_index_task(event)
