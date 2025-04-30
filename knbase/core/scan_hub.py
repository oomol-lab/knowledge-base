import traceback

from dataclasses import dataclass
from threading import Event
from pathlib import Path
from concurrent.futures import as_completed, Future, ThreadPoolExecutor

from ..sqlite3_pool import ThreadPoolContext
from ..module import KnowledgeBase, ResourceEvent
from ..reporter import EventReporter
from ..state_machine import StateMachine
from ..interruption import Interruption
from .waker import Waker, WakerDidStop


@dataclass
class _Task:
  event: ResourceEvent
  done: Event
  interrupted: bool

class _AllTasksDone:
  pass

class ScanHub:
  def __init__(
        self,
        state_machine: StateMachine,
        interruption: Interruption,
        reporter: EventReporter,
      ) -> None:

    self._machine: StateMachine = state_machine
    self._interruption: Interruption = interruption
    self._reporter: EventReporter = reporter
    self._waker: Waker[_Task | _AllTasksDone] = Waker()

  def start_loop(self, workers: int) -> None:
    self._machine.goto_scanning()
    bases = list(self._machine.get_knowledge_bases())

    if len(bases) > 0:
      with ThreadPoolExecutor(max_workers=min(workers, len(bases))) as executor:
        futures: list[Future] = []
        for base in bases:
          future = executor.submit(self._scan_in_background, base)
          futures.append(future)

        self._handle_resource_events(bases_count=len(bases))

        for future in as_completed(futures):
          try:
            future.result()
          except WakerDidStop:
            pass
          except Exception:
            traceback.print_exc()

  def _handle_resource_events(self, bases_count: int):
    working_count: int = bases_count
    while working_count > 0:
      task = self._waker.receive()
      if isinstance(task, _Task):
        try:
          self._machine.put_resource(
            event_id=task.event.id,
            resource=task.event.resource,
            path=Path(task.event.resource_path),
          )
        except Exception:
          task.interrupted = True
          traceback.print_exc()

        finally:
          task.done.set()

      elif isinstance(task, _AllTasksDone):
        working_count -= 1

  def _scan_in_background(self, base: KnowledgeBase):
    with ThreadPoolContext():
      with self._interruption.context():
        module = base.resource_module
        event_id = self._reporter.report_scan_begin(base)
        error: Exception | None = None
        try:
          for event in module.scan(base):
            self._reporter.report_resource_event(event)
            task = _Task(
              event=event,
              done=Event(),
              interrupted=False,
            )
            self._waker.push(task)
            task.done.wait()
            if task.interrupted:
              break
            module.complete_event(event)

        except Exception as e:
          error = e
          raise e

        finally:
          self._waker.push(_AllTasksDone())
          module.complete_scanning(base)
          self._reporter.report_scan_done(event_id, base, error)