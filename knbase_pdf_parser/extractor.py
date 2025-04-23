import re

from typing import Generator
from datetime import datetime, timedelta
from dataclasses import dataclass
from pdfplumber.page import Page


_SNAPSHOT_EXT = ".snapshot.txt"
_ANNOTATION_EXT = ".annotation.json"

@dataclass
class Annotation:
  type: str | None
  title: str | None
  content: str | None
  uri: str | None
  created_at: str | None
  updated_at: str | None
  quad_points: list[float] | None
  extracted_text: str | None

def extract_annotations(page: Page) -> Generator[Annotation, None, None]:
  for anno in page.annots:
    if anno.get("object_type", "") != "annot":
      continue
    annotation = Annotation(
      type=None,
      title=anno.get("title", None),
      content=anno.get("contents", None),
      uri=anno.get("uri", None),
      created_at=None,
      updated_at=None,
      quad_points=None,
      extracted_text=None,
    )
    data = anno.get("data", None)
    if data is not None:
      annotation.quad_points = data.get("QuadPoints", None)
      sub_type = data.get("Subtype", None)

      if sub_type is not None:
        annotation.type = sub_type.name

      creation_date = data.get("CreationDate", None)
      updated_date = data.get("M", None)

      if creation_date is not None:
        annotation.created_at = _convert_to_utc(creation_date.decode("utf-8"))
      if updated_date is not None:
        annotation.updated_at = _convert_to_utc(updated_date.decode("utf-8"))

    if annotation.title is not None or \
        annotation.content is not None or \
        annotation.uri is not None:
      yield annotation

def _convert_to_utc(timestamp: str):
  pattern = r"D:(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})([\+\-]\d{2})'(\d{2})"
  match = re.match(pattern, timestamp)
  if match:
    year, month, day, hour, minute, second, timezone_offset_hour, timezone_offset_minute = match.groups()
    dt = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
    utc_offset = timedelta(hours=int(timezone_offset_hour), minutes=int(timezone_offset_minute))
    dt_adjusted = dt - utc_offset
    return dt_adjusted.strftime("%Y-%m-%d %H:%M:%S")
  else:
    return None