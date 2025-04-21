from ..state_machine import StateMachine
from .thread_pool import ThreadPool


class ProcessHub:
  def __init__(self, state_machine: StateMachine, thread_pool: ThreadPool):
    self._state_machine: StateMachine = state_machine
    self._thread_pool: ThreadPool = thread_pool

  def start_loop(self):
    self._state_machine.goto_processing()