from dataclasses import dataclass
from typing import Callable
from threading import Thread, Lock, Event

from .waker import Waker, WakerDidStop


@dataclass
class _Worker:
  thread: Thread
  is_working: bool
  did_removed: bool


class ThreadPool:
  def __init__(self) -> None:
    self._lock: Lock = Lock()
    self._workers: list[_Worker] = []
    self._waker: Waker[None | Callable[[], None]] = Waker()
    self._invoking_lock: Lock = Lock()
    self._invoking_count: int = 0
    self._no_invoking_event: Event = Event()
    self._no_invoking_event.set()

  @property
  def did_stop(self) -> bool:
    return self._waker.did_stop

  def wait_util_no_invoking(self) -> None:
    self._no_invoking_event.wait()

  def workers(self) -> int:
    return len(self._workers)

  def set_workers(self, workers: int) -> None:
    to_start_threads: list[Thread] | None = None
    to_join_threads: list[Thread] | None = None

    with self._lock:
      if self._waker.did_stop:
        return

      if len(self._workers) < workers:
        to_added_workers = workers - len(self._workers)
        to_start_threads = []

        for _ in range(to_added_workers):
          worker = _Worker(
            thread=None,
            is_working=False,
            did_removed=False,
          )
          self._workers.append(worker)
          thread=Thread(
            target=self._run_in_background,
            args=(worker,),
          )
          worker.thread = thread
          to_start_threads.append(thread)

      elif len(self._workers) > workers:
        to_removed_workers = workers - len(self._workers)
        removed_indexes: set[int] = set()
        for want_is_working in (False, True):
          for i, worker in enumerate(self._workers):
            if len(removed_indexes) >= to_removed_workers:
              break
            if worker.is_working == want_is_working:
              removed_indexes.add(i)

        to_join_threads = []
        removed_workers = [
          worker for i, worker in enumerate(self._workers)
          if i in removed_indexes
        ]
        self._workers = [
          worker for i, worker in enumerate(self._workers)
          if i not in removed_indexes
        ]
        for removed_worker in removed_workers:
          removed_worker.did_removed = True
          to_join_threads.append(removed_worker.thread)

        self._waker.broadcast(None)

    if to_start_threads is not None:
      for thread in to_start_threads:
        thread.start()

    if to_join_threads is not None:
      for thread in to_join_threads:
        thread.join()

  def stop(self) -> None:
    workers: list[_Worker]
    with self._lock:
      if self._waker.did_stop:
        return
      self._waker.stop()
      workers = self._workers
      self._workers = []

    for worker in workers:
      worker.thread.join()

  def execute(self, func: Callable[[], None]) -> None:
    self._waker.push(func)

  def _run_in_background(self, worker: _Worker):
    func: None | Callable[[], None] = None
    while True:
      if worker.did_removed:
        break
      try:
        func = self._waker.receive()
      except WakerDidStop:
        break
      if func is None:
        continue
      try:
        with self._invoking_lock:
          self._invoking_count += 1
          if self._invoking_count == 1:
            self._no_invoking_event.clear()
        worker.is_working = True
        func()
      except Exception as e:
        print(e)
      finally:
        worker.is_working = False
        with self._invoking_lock:
          self._invoking_count -= 1
          if self._invoking_count == 0:
            self._no_invoking_event.set()