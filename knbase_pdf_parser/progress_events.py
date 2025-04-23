from __future__ import annotations
from dataclasses import dataclass
from typing import Union, Callable
from enum import Enum


@dataclass
class ScanCompletedEvent:
  updated_files: int

@dataclass
class StartHandleFileEvent:
  path: str
  format: FileFormat
  operation: HandleFileOperation

class FileFormat(Enum):
  PDF = "pdf"

class HandleFileOperation(Enum):
  Create = "create"
  Update = "update"
  Remove = "remove"

@dataclass
class CompleteHandleFileEvent:
  path: str

@dataclass
class PDFFileProgressEvent:
  step: PDFFileStep
  completed: int
  total: int

class PDFFileStep(Enum):
  Parse = "parse"
  Index = "index"

ProgressEvent = Union[
  ScanCompletedEvent,
  StartHandleFileEvent,
  CompleteHandleFileEvent,
  PDFFileProgressEvent,
]

ProgressEventListener = Callable[[ProgressEvent], None]