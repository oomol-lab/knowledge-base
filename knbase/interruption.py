from threading import local, Event
from typing import cast


_interrupted_event_val = local()

class InterruptedException(Exception):
  def __init__(self):
    super().__init__("Interrupt")

def init_interrupted_event(event: Event):
  _interrupted_event_val.value = event

def assert_continue():
  if not hasattr(_interrupted_event_val, "value"):
    return
  event = cast(Event, _interrupted_event_val.value)
  if event.is_set():
    raise InterruptedException()