import re
import io

from typing import Generator
from datetime import datetime, timedelta
from pdfplumber import PDF
from pdfplumber.page import Page
from .select_text import extract_selected_text
from .meta import PDFMeta, Annotation


def extract_metadata_with_pdf(pdf: PDF) -> PDFMeta:
  origin = pdf.metadata
  modified_at = origin.get("ModDate", None)
  if modified_at is not None:
    modified_at = _convert_to_utc(modified_at)

  return PDFMeta(
    author=origin.get("Author", None),
    modified_at=modified_at,
    producer=origin.get("Producer", None),
  )

def extract_snapshot(page: Page) -> str:
  snapshot = page.extract_text_simple()
  snapshot = _standardize_text(snapshot)
  return snapshot

def extract_annotations(page: Page) -> Generator[tuple[str, Annotation], None, None]:
  for anno in page.annots:
    if anno.get("object_type", "") != "annot":
      continue

    content = anno.get("contents", None)
    title = anno.get("title", None)
    uri = anno.get("uri", None)
    if content is None or title is None or uri is None:
      continue

    meta = Annotation(
      type=None,
      title=title,
      uri=uri,
      created_at=None,
      updated_at=None,
      quad_points=None,
      extracted_text=None,
    )
    data = anno.get("data", None)
    if data is not None:
      quad_points = data.get("QuadPoints", None)
      text = extract_selected_text(page, quad_points)
      meta.quad_points = quad_points

      if text is not None:
        meta.extracted_text = _standardize_text(text)

      sub_type = data.get("Subtype", None)
      if sub_type is not None:
        meta.type = sub_type.name

      creation_date = data.get("CreationDate", None)
      updated_date = data.get("M", None)

      if creation_date is not None:
        meta.created_at = _convert_to_utc(creation_date.decode("utf-8"))
      if updated_date is not None:
        meta.updated_at = _convert_to_utc(updated_date.decode("utf-8"))

    yield content, meta

def _standardize_text(input_str: str) -> str:
  buffer = io.StringIO()
  state: int = 0 # 0: words, 1: space, 2: newline
  for char in input_str:
    if char.isspace():
      if char == "\n":
        state = 2
      elif state == 0:
        state = 1
    else:
      if state == 2:
        buffer.write("\n")
      elif state == 1:
        buffer.write(" ")
      buffer.write(char)
      state = 0
  return buffer.getvalue()

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