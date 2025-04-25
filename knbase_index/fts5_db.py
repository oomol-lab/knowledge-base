import re
import json

from typing import Generator
from sqlite3 import Cursor
from .types import IndexNode, IndexSegment, IndexNodeMatching
from ..segmentation import Segment
from ..sqlite3_pool import register_table_creators, SQLite3Pool

_Segment = tuple[int, int, list[str]]
_INVALID_TOKENS = set(["", "NEAR", "AND", "OR", "NOT"])

class FTS5DB:
  def __init__(self, db_path: str):
    db = SQLite3Pool(
      format_name="fts5",
      path=db_path,
    )
    self._db: SQLite3Pool = db.assert_format("fts5")

  def query(
    self,
    query_text: str,
    matching: IndexNodeMatching = IndexNodeMatching.Matched,
    is_or_condition: bool = False,
  ) -> Generator[IndexNode, None, None]:

    query_tokens = self._split_tokens(query_text)
    query_tokens_set = set(query_tokens)

    if len(query_tokens) == 0:
      return

    with self._db.connect() as (cursor, _):
      query_with_and = " AND ".join(query_tokens)
      if is_or_condition:
        query_with_or = " OR ".join(query_tokens)
        query = f"({query_with_or}) NOT ({query_with_and})"
      else:
        query = query_with_and

      query = f"\"content\": {query}"
      fields = "N.node_id, C.content, N.metadata, N.segments"
      sql = f"SELECT {fields} from contents C INNER JOIN nodes N ON C.rowid = N.content_id WHERE C.content MATCH ?"
      cursor.execute(sql, (query,))

      while True:
        rows = cursor.fetchmany(size=25)
        if len(rows) == 0:
          break
        for row in rows:
          node_id, content, metadata_json, encoded_segments = row
          metadata: dict = json.loads(metadata_json)
          type = metadata.get("type", "undefined")
          segments, rank = self._analysis_segments(
            query_tokens_set=query_tokens_set,
            segments=self._decode_segment(content, encoded_segments),
          )
          # fts5 database maybe matches no keywords
          if len(segments) > 0:
            node = IndexNode(
              id=node_id,
              type=type,
              matching=matching,
              metadata=metadata,
              fts5_rank=rank,
              vector_distance=0.0,
              segments=segments,
            )
            yield node

  def save(self, node_id: str, segments: list[Segment], metadata: dict):
    encoded_segments, tokens = self._encode_segments(segments)
    if len(encoded_segments) == 0:
      return

    with self._db.connect() as (cursor, conn):
      try:
        document = " ".join(tokens)
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("INSERT INTO contents (content) VALUES (?)", (document,))
        content_id = cursor.lastrowid
        type = metadata.get("type", None)
        metadata_json = json.dumps(metadata)
        cursor.execute(
          "INSERT INTO nodes (node_id, type, metadata, segments, content_id) VALUES (?, ?, ?, ?, ?)",
          (node_id, type, metadata_json, encoded_segments, content_id),
        )
        conn.commit()

      except Exception as e:
        conn.rollback()
        raise e

  def remove(self, node_id: str):
    with self._db.connect() as (cursor, conn):
      try:
        cursor.execute("SELECT content_id FROM nodes WHERE node_id = ?", (node_id,))
        row = cursor.fetchone()
        if row is not None:
          cursor.execute("BEGIN TRANSACTION")
          content_id = row[0]
          cursor.execute("DELETE FROM contents WHERE rowid = ?", (content_id,))
          cursor.execute("DELETE FROM nodes WHERE node_id = ?", (node_id,))
          conn.commit()

      except Exception as e:
        conn.rollback()
        raise e

  def _analysis_segments(
      self,
      query_tokens_set: set[str],
      segments: list[_Segment]) -> tuple[list[IndexSegment], float]:

    query_tokens_len = len(query_tokens_set)
    target_segments: list[IndexSegment] = []
    match_count_list: list[bool] = [False for _ in range(query_tokens_len)]

    for start, end, tokens in segments:
      matched_tokens_set = set()
      for token in tokens:
        if token in query_tokens_set:
          matched_tokens_set.add(token)
      matched_tokens = list(matched_tokens_set)
      matched_tokens.sort()

      if len(matched_tokens) > 0:
        match_count_list[query_tokens_len - len(matched_tokens)] = True
        target_segments.append(IndexSegment(
          start=start,
          end=end,
          fts5_rank=-len(matched_tokens),
          vector_distance=0.0,
          matched_tokens=matched_tokens
        ))

    sum_rank = 0.0
    rank = 1.0

    for is_matched in match_count_list:
      if is_matched:
        sum_rank += rank
      rank *= 0.35

    return target_segments, rank

  def _encode_segments(self, segments: list[Segment]) -> tuple[str, list[str]]:
    encoded: list[str] = []
    tokens: list[str] = []

    for s in segments:
      segment_tokens = self._split_tokens(s.text.lower())
      if len(segment_tokens) == 0:
        continue
      encoded.append(f"{len(segment_tokens)}:{s.start}-{s.end}")
      for k in segment_tokens:
        tokens.append(k)

    return ",".join(encoded), tokens

  def _decode_segment(self, content: str, segments: str) -> list[_Segment]:
    decoded: list[tuple[int, int, list[str]]] = []
    tokens = content.split(" ")
    offset: int = 0

    for segment in segments.split(","):
      parts = segment.split(":", 1)
      token_count, position = parts
      token_count = int(token_count)
      start, end = position.split("-")
      start = int(start)
      end = int(end)
      segment_tokens = tokens[offset:offset + token_count]
      offset += token_count
      decoded.append((start, end, segment_tokens))

    return decoded

  def _split_tokens(self, text: str) -> list[str]:
    text = re.sub(r"[-+:!\"'\{\},\.]", " ", text)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f ]+", " ", text)
    tokens: list[str] = []

    for token in text.split(" "):
      token = token.lower()
      if not token in _INVALID_TOKENS:
        tokens.append(token)

    return tokens

  def _weights(self, length: int, attenuation: float, mean_is_1: bool) -> list[float]:
    weights: list[float] = []
    weight: float = 1.0
    rate = 1.0 - attenuation
    for _ in range(1, length):
      weights.append(weight)
      weight *= rate
    if mean_is_1:
      sum_weight = sum(weights)
      for i in range(len(weights)):
        weights[i] /= sum_weight
    return weights

def _create_tables(cursor: Cursor):
  # unicode61 remove_diacritics 2 means: diacritics are correctly removed from all Latin characters.
  # to see: https://www.sqlite.org/fts5.html
  cursor.execute("""
    CREATE VIRTUAL TABLE contents USING fts5(
      content,
      tokenize = "unicode61 remove_diacritics 2"
    );
  """)
  cursor.execute("""
    CREATE TABLE nodes (
      node_id TEXT PRIMARY KEY,
      type TEXT,
      metadata TEXT NOT NULL,
      segments TEXT NOT NULL,
      content_id INTEGER NOT NULL
    )
  """)
  cursor.execute("""
    CREATE INDEX idx_nodes ON nodes (content_id)
  """)

register_table_creators("fts5", _create_tables)