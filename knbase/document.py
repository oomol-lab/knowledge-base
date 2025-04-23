import io

from typing import Any, TypeVar, Generic
from dataclasses import dataclass
from pathlib import Path
from yaml import safe_load, dump
from hashlib import sha256


M = TypeVar("M")
F = TypeVar("F")

@dataclass
class Fragment(Generic[M]):
  meta: M
  content: str

@dataclass
class Document(Generic[M, F]):
  meta: M
  hash: bytes
  fragments: list[Fragment[F]]

def load_document(file_path: Path) -> Document[Any, Any]:
  data, hash = _load_yaml(file_path)
  fragments: list[Fragment[Any]] = []
  meta = data["meta"]

  for fragment in data["fragments"]:
    fragments.append(Fragment(
      meta=fragment["meta"],
      content=fragment["content"]
    ))
  return Document(
    meta=meta,
    hash=hash,
    fragments=fragments
  )

def save_document(
    file_path: Path,
    meta: M,
    fragments: list[Fragment[F]]
  ) -> Document[M, F]:

  buffer = io.StringIO()
  dump(
    stream=buffer,
    encoding="utf-8",
    allow_unicode=True,
    data={
      "meta": meta,
      "fragments": [
        {
          "meta": f.meta,
          "content": f.content
        }
        for f in fragments
      ]
    },
  )
  bin_data = buffer.getvalue().encode("utf-8")
  with open(file_path, "wb") as file:
    file.write(bin_data)

  return Document(
    meta=meta,
    fragments=fragments,
    hash=sha256(bin_data).digest()
  )

def _load_yaml(file_path: Path) -> tuple[Any, bytes]:
  sha256_hash = sha256()
  with open(file_path, "rb") as file:
    chunk = file.read(8192)
    while chunk:
      sha256_hash.update(chunk)
      chunk = file.read(8192)

  with open(file_path, "r", encoding="utf-8") as file:
    return (
      safe_load(file),
      sha256_hash.digest(),
    )
