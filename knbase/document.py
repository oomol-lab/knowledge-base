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
  content: str
  fragments: list[Fragment[F]]

def load_document(file_path: Path) -> Document[Any, Any]:
  data, hash = _load_yaml(file_path)
  fragments: list[Fragment[Any]] = []
  meta = data["meta"]
  content: str = data.get("content", "")
  json_fragments = data.get("fragments", None)

  if json_fragments:
    for fragment in json_fragments:
      fragments.append(Fragment(
        meta=fragment["meta"],
        content=fragment["content"]
      ))
  return Document(
    meta=meta,
    hash=hash,
    content=content,
    fragments=fragments
  )

def save_document(
    file_path: Path,
    meta: M,
    content: str | None = None,
    fragments: list[Fragment[F]] | None = None,
  ) -> Document[M, F]:

  buffer = io.StringIO()
  json_data = {"meta": meta}

  if content:
    json_data["content"] = content
  else:
    content = ""

  if fragments:
    json_data["fragments"] = [
      {
        "meta": f.meta,
        "content": f.content
      }
      for f in fragments
    ]
  else:
    fragments = []

  dump(
    stream=buffer,
    encoding="utf-8",
    allow_unicode=True,
    data=json_data,
  )
  bin_data = buffer.getvalue().encode("utf-8")
  with open(file_path, "wb") as file:
    file.write(bin_data)

  return Document(
    meta=meta,
    content=content,
    fragments=fragments,
    hash=sha256(bin_data).digest()
  )

def _load_yaml(file_path: Path) -> tuple[dict[str, Any], bytes]:
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
