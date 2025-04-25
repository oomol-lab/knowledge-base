from .types import IndexNodeMatching, IndexNode
from .fts5_db import FTS5DB
from .vector_db import VectorDB, Embedding


class Query:
  def __init__(self, fts5_db: FTS5DB, vector_db: VectorDB) -> None:
    self._fts5_db: FTS5DB = fts5_db
    self._vector_db: VectorDB = vector_db

  def do(self, query: str, results_limit: int) -> list[IndexNode]:
    matched_node_ids: set[str] = set()
    matched_nodes: list[IndexNode] = []

    query_embedding = self._vector_db.encode_embedding(query)
    generator = self._fts5_db.query(
      query,
      matching=IndexNodeMatching.Matched,
      is_or_condition=False,
    )
    for node in generator:
      matched_node_ids.add(node.id)
      matched_nodes.append(node)
      if len(matched_nodes) >= results_limit:
        return self._do_closing_of_matched_nodes(query_embedding, matched_nodes)

    self._do_closing_of_matched_nodes(query_embedding, matched_nodes)
    part_matched_nodes: list[IndexNode] = []
    generator = self._fts5_db.query(
      query,
      matching=IndexNodeMatching.MatchedPartial,
      is_or_condition=True,
    )
    for node in generator:
      matched_node_ids.add(node.id)
      part_matched_nodes.append(node)
      if len(matched_nodes) + len(part_matched_nodes) >= results_limit:
        return (
          matched_nodes +
          self._do_closing_of_matched_nodes(query_embedding, part_matched_nodes)
        )
    self._do_closing_of_matched_nodes(query_embedding, part_matched_nodes)
    similarity_nodes: list[IndexNode] = []
    nodes = self._vector_db.query(
      query_embedding=query_embedding,
      matching=IndexNodeMatching.Similarity,
      results_limit=results_limit,
    )
    for node in nodes:
      if node.id not in matched_node_ids:
        similarity_nodes.append(node)
    similarity_nodes.sort(key=self._sort_key)

    return (
      matched_nodes +
      part_matched_nodes +
      similarity_nodes
    )

  def _do_closing_of_matched_nodes(self, query_embedding: Embedding, nodes: list[IndexNode]) -> list[IndexNode]:
    for node in nodes:
      segments: list[tuple[str, int]] = []
      for i, _ in enumerate(node.segments):
        segments.append((node.id, i))

      min_distance = float("inf")
      for distance in self._vector_db.distances(query_embedding, segments):
        min_distance = min(min_distance, distance)
      node.vector_distance = min_distance

    nodes.sort(key=self._sort_key)
    return nodes

  def _sort_key(self, node: IndexNode) -> tuple[float, float]:
    return (-node.fts5_rank, node.vector_distance)