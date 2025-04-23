from hashlib import sha256
from base64 import urlsafe_b64encode


def is_empty_string(text: str) -> bool:
  for char in text:
    if not char.isspace():
      return False
  return True

def get_sha256(file_path) -> str:
  hash = sha256()
  chunk_size = 8192
  with open(file_path, "rb") as f:
    for chunk in iter(lambda: f.read(chunk_size), b""):
      hash.update(chunk)
  return urlsafe_b64encode(hash.digest())