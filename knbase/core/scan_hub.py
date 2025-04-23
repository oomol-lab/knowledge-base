import traceback

from dataclasses import dataclass
from threading import Event
from pathlib import Path
from concurrent.futures import as_completed, Future, ThreadPoolExecutor

from ..sqlite3_pool import build_thread_pool, release_thread_pool
from ..module import KnowledgeBase, ResourceEvent
from ..state_machine import StateMachine
from .waker import Waker, WakerDidStop


@dataclass
class _Task:
  event: ResourceEvent
  done: Event
  interrupted: bool

class _AllTasksDone:
  pass

class ScanHub:
  def __init__(self, state_machine: StateMachine):
    self._machine: StateMachine = state_machine
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

    self._machine.goto_setting()

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
    build_thread_pool()
    module = base.resource_module
    try:
      for event in module.scan(base):
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

    finally:
      self._waker.push(_AllTasksDone())
      module.complete_scanning(base)
      release_thread_pool()