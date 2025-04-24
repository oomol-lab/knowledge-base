from shapely.geometry import Polygon
from pdfplumber.page import Page


def extract_selected_text(page: Page, quad_points: list[float]) -> str | None:
  annotation_polygon = _AnnotationPolygon(quad_points)
  if not annotation_polygon.is_valid:
    return None

  line_tuples: list[tuple[float, str]] = []
  height = page.height

  for line in page.extract_text_lines(char=False):
    x0, y0, x1, y1 = line["x0"], line["top"], line["x1"], line["bottom"]
    # coordinate system is from bottom-left
    y0 = height - y0
    y1 = height - y1
    if not annotation_polygon.intersects(x0, y0, x1, y1):
      continue

    line_chars: list[str] = []
    for char in line["chars"]:
      x0, y0, x1, y1 = char["x0"], char["y0"], char["x1"], char["y1"]
      if annotation_polygon.contains(x0, y0, x1, y1):
        line_chars.append(char["text"])
    line_tuples.append((-y0, "".join(line_chars)))

  lines: list[str] = []
  for (_, line) in sorted(line_tuples, key=lambda x: x[0]):
    lines.append(line)

  if len(lines) == 0:
    return None
  else:
    return "\n".join(lines)

class _AnnotationPolygon:
  def __init__(self, quad_points: list[float]):
    self._polygons: list[Polygon] = []
    for i in range(int(len(quad_points) / 8)):
      x0 = float("inf")
      x1 = -float("inf")
      y0 = float("inf")
      y1 = -float("inf")
      for j in range(4):
        index = i*8 + j*2
        x = quad_points[index]
        y = quad_points[index + 1]
        x0 = min(x0, x)
        x1 = max(x1, x)
        y0 = min(y0, y)
        y1 = max(y1, y)
      polygon = Polygon(((x0, y0), (x1, y0), (x1, y1), (x0, y1)))
      if polygon.is_valid:
        self._polygons.append(polygon)

  @property
  def is_valid(self) -> bool:
    return len(self._polygons) > 0

  def intersects(self, x0: float, y0: float, x1: float, y1: float) -> bool:
    target_polygon = Polygon(((x0, y0), (x1, y0), (x1, y1), (x0, y1)))
    for polygon in self._polygons:
      if polygon.overlaps(target_polygon):
        return True
    return False

  def contains(self, x0: float, y0: float, x1: float, y1: float) -> bool:
    # make target smaller to be contained
    rate = 0.01
    center_x = (x0 + x1) / 2.0
    center_y = (y0 + y1) / 2.0
    x0 += (center_x - x0) * rate
    y0 += (center_y - y0) * rate
    x1 += (center_x - x1) * rate
    y1 += (center_y - y1) * rate
    target_polygon = Polygon([(x0, y0), (x1, y0), (x1, y1), (x0, y1)])
    for polygon in self._polygons:
      if polygon.contains(target_polygon):
        return True
    return False