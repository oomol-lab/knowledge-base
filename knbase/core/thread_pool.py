from typing import Callable
from threading import Thread

from .waker import Waker, WakerDidStop


class ThreadPool:
  def __init__(self, workers: int) -> None:
    self._threads: list[Thread] = [
      Thread(target=self._run_in_background)
      for _ in range(workers)
    ]
    self._waker: Waker[Callable[[], None]] = Waker()

  def start(self) -> None:
    for thread in self._threads:
      thread.start()

  def stop(self) -> None:
    if self._waker.did_stop:
      return
    self._waker.stop()
    for thread in self._threads:
      thread.join()

  def execute(self, func: Callable[[], None]) -> None:
    self._waker.push(func)

  def _run_in_background(self):
    while True:
      func: Callable[[], None]
      try:
        func = self._waker.receive()
      except WakerDidStop:
        break
      try:
        func()
      except Exception as e:
        print(e)