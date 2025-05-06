from __future__ import annotations

import os

from os import PathLike
from pathlib import Path
from enum import Enum
from typing import Generic, Callable
from knbase import load_document, T, KnowledgeBase, KnowledgeBasesHub, PreprocessingModule, IndexModule

from .types import IndexRow
from .fts5_db import FTS5DB
from .vector_db import VectorDB
from .query import Query
from .segmentation import Segment, Segmentation


class _ModuleKind(Enum):
  FTS5 = 0,
  VECTOR = 1,

class IndexDatabase(Generic[T]):
  def __init__(
        self,
        base_path: PathLike,
        embedding_model_id: str = "shibing624/text2vec-base-chinese",
      ) -> None:

    os.makedirs(base_path, exist_ok=True)

    self._base_path: Path = Path(base_path)
    self._vector_path: Path = self._base_path.joinpath("chroma")
    self._segmentation: Segmentation = Segmentation()
    self._hub: KnowledgeBasesHub = None
    self._fts5_db = FTS5DB(
      db_path=self._base_path.joinpath("fts5.sqlite3"),
    )
    self._vector_db: VectorDB = VectorDB(
      embedding_model_id=embedding_model_id,
      distance_space="l2",
      index_dir_path=str(self._vector_path),
    )
    self._query: Query = Query(
      fts5_db=self._fts5_db,
      vector_db=self._vector_db,
    )
    self._modules: tuple[IndexModule[T], ...] = tuple(
      _VectorIndexModule(
        id=id,
        kind=kind,
        add_document=self._add_document,
        remove_document=self._remove_document,
      )
      for id, kind in (
        ("fts-index", _ModuleKind.FTS5),
        ("vector-index", _ModuleKind.VECTOR),
      )
    )

  @property
  def modules(self) -> tuple[IndexModule[T], ...]:
    return self._modules

  def set_hub(self, hub: KnowledgeBasesHub) -> None:
    self._hub = hub

  def query(self, query: str, results_limit: int) -> list[IndexRow]:
    rows: list[IndexRow] = []
    for node in self._query.do(query, results_limit):
      base_id, preproc_module_id, document_hash = node.id.split("/", maxsplit=1)
      base_id = int(base_id)
      rows.append(IndexRow(
        base=self._hub.get_knowledge_base(base_id),
        preproc_module=self._hub.preproc_module(preproc_module_id),
        document_hash=bytes.fromhex(document_hash),
        matching=node.matching,
        metadata=node.metadata,
        fts5_rank=node.fts5_rank,
        vector_distance=node.vector_distance,
        segments=node.segments,
      ))
    return rows

  def _add_document(self, kind: _ModuleKind, id: str, path: Path, meta: T):
    document = load_document(path)
    segments: list[Segment] = [
      s for s in self._segmentation.split(document.content)
      if not self._is_empty_string(s.text)
    ]
    if len(segments) == 0:
      return

    meta = self._normal_meta(meta)
    if kind == _ModuleKind.FTS5:
      self._fts5_db.save(id, segments, meta)
    elif kind == _ModuleKind.VECTOR:
      self._vector_db.save(id, segments, meta)

  def _remove_document(self, kind: _ModuleKind, id: str):
    if kind == _ModuleKind.FTS5:
      self._fts5_db.remove(id)
    elif kind == _ModuleKind.VECTOR:
      self._vector_db.remove(id)

  def _normal_meta(self, d: dict | None) -> dict:
    if d is None:
      return {}
    else:
      return {k: v for k, v in d.items() if v is not None}

  def _is_empty_string(self, text: str) -> bool:
    for char in text:
      if not char.isspace():
        return False
    return True

class _VectorIndexModule(IndexModule[T]):
  def __init__(
        self,
        id: str,
        kind: _ModuleKind,
        add_document: Callable[[_ModuleKind, str, Path, T], None],
        remove_document: Callable[[_ModuleKind, str], None],
      ) -> None:

    super().__init__(id)
    self._kind: _ModuleKind = kind
    self._add_document: Callable[[_ModuleKind, str, Path, T], None] = add_document
    self._remove_document: Callable[[_ModuleKind, str], None] = remove_document

  def add(
        self,
        base: KnowledgeBase,
        preproc_module: PreprocessingModule,
        document_hash: bytes,
        document_path: Path,
        document_meta: T,
        report_progress: Callable[[float], None],
      ) -> None:

    id = self._to_id(base, preproc_module, document_hash)
    self._add_document(self._kind, id, document_path, document_meta)

  def remove(
        self,
        base: KnowledgeBase,
        preproc_module: PreprocessingModule,
        document_hash: bytes,
        report_progress: Callable[[float], None],
      ) -> None:

    id = self._to_id(base, preproc_module, document_hash)
    self._remove_document(self._kind, id)

  def _to_id(
        self,
        base: KnowledgeBase,
        preproc_module: PreprocessingModule,
        document_hash: bytes,
      ) -> str:
    return f"{base.id}/{preproc_module.id}/{document_hash.hex()}"
