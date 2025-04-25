import re
import torch

from typing import cast, Callable, Literal
from numpy import ndarray, array
from sentence_transformers import SentenceTransformer
from chromadb import PersistentClient
from chromadb.api import ClientAPI
from chromadb.api.types import ID, EmbeddingFunction, IncludeEnum, Documents, Embedding, Embeddings, Document, Metadata
from chromadb.utils import distance_functions

from ..segmentation import Segment
from .types import IndexNode, IndexSegment, IndexNodeMatching

_DistanceFunction = Callable[[distance_functions.Vector, distance_functions.Vector], float]
DistanceSpace = Literal["l2", "ip", "cosine"]

class VectorDB:
  def __init__(
    self,
    index_dir_path: str,
    embedding_model_id: str,
    distance_space: DistanceSpace,
  ):
    if distance_space == "l2":
      self._distance_fn: _DistanceFunction = distance_functions.l2
    elif distance_space == "ip":
      self._distance_fn: _DistanceFunction = distance_functions.ip
    elif distance_space == "cosine":
      self._distance_fn: _DistanceFunction = distance_functions.cosine
    else:
      raise ValueError(f"Invalid distance space: {distance_space}")

    chromadb: ClientAPI = PersistentClient(path=index_dir_path)
    self._embedding_encode: _EmbeddingFunction = _EmbeddingFunction(embedding_model_id)
    self._db = chromadb.get_or_create_collection(
      name="nodes",
      embedding_function=self._embedding_encode,
      metadata={"hnsw:space": distance_space},
    )

  def encode_embedding(self, text: str) -> Embedding:
    return self._embedding_encode([text])[0]

  # segment is a tuple of (node_id, index)
  def distances(self, query_embedding: Embedding, segments: list[tuple[str, int]]) -> list[float]:
    ids: list[ID] = []
    for node_id, index in segments:
      ids.append(f"{node_id}/{index}")

    query_np_array = array(query_embedding)
    result = self._db.get(ids=ids, include=[IncludeEnum.embeddings])
    distances: list[float] = []

    for embedding in cast(list[Embedding], result["embeddings"]):
      distance = self._distance_fn(query_np_array, array(embedding))
      distances.append(distance)

    return distances

  def query(
    self,
    query_embedding: Embedding,
    results_limit: int,
    matching: IndexNodeMatching = IndexNodeMatching.Similarity,
  ) -> list[IndexNode]:
    result = self._db.query(
      query_embeddings=query_embedding,
      n_results=results_limit,
      include=[IncludeEnum.metadatas, IncludeEnum.distances],
    )
    ids = cast(list[list[ID]], result["ids"])[0]
    metadatas = cast(list[list[dict]], result["metadatas"])[0]
    distances = cast(list[list[float]], result["distances"])[0]
    node2segments: dict[str, list[tuple[float, int, int, dict]]] = {}

    for i in range(len(ids)):
      matches = re.match(r"(.*)/([^/]*)$", ids[i])
      if matches is None:
        raise ValueError(f"Invalid ID: {ids[i]}")
      node_id = matches.group(1)
      metadata: dict[str, any] = metadatas[i]
      distance = distances[i]
      start = metadata.pop("seg_start")
      end = metadata.pop("seg_end")
      segments = node2segments.get(node_id, None)
      if segments is None:
        node2segments[node_id] = segments = []
      segments.append((distance, start, end, metadata))

    nodes: list[IndexNode] = []
    for node_id, segments in node2segments.items():
      node_segments: list[IndexSegment] = []
      node_metadata: dict | None = None
      min_distance: float = float("inf")
      for distance, start, end, metadata in segments:
        node_segments.append(IndexSegment(
          start=start,
          end=end,
          fts5_rank=0.0,
          vector_distance=distance,
          matched_tokens=[],
        ))
        if node_metadata is None:
          node_metadata = metadata
        if distance < min_distance:
          min_distance = distance
      if node_metadata is None:
        continue
      type = node_metadata.get("type", "undefined")
      nodes.append(IndexNode(
        id=node_id,
        type=type,
        matching=matching,
        metadata=node_metadata,
        fts5_rank=0.0,
        vector_distance=min_distance,
        segments=node_segments,
      ))
    nodes.sort(key=lambda node: node.vector_distance)

    return nodes

  def save(self, node_id: str, segments: list[Segment], metadata: dict):
    ids: list[ID] = []
    documents: list[Document] = []
    metadatas: list[Metadata] = []

    for i, segment in enumerate(segments):
      segment_metadata = metadata.copy()
      segment_metadata["seg_start"] = segment.start
      segment_metadata["seg_end"] = segment.end
      if i == 0:
        segment_metadata["seg_len"] = len(segments)

      ids.append(f"{node_id}/{i}")
      documents.append(segment.text)
      metadatas.append(segment_metadata)

    self._db.add(
      ids=ids,
      documents=documents,
      metadatas=metadatas,
    )

  def remove(self, node_id: str):
    result = self._db.get(
      ids=f"{node_id}/0",
      include=[IncludeEnum.metadatas],
    )
    metadatas = result.get("metadatas")
    if metadatas is None or len(metadatas) == 0:
      # segments may be empty, but user maybe don't know it.
      return

    segments_len = cast(int, metadatas[0].get("seg_len", 1))
    group_size: int = 45

    for offset in range(0, segments_len, group_size):
      ids_len = min(group_size, segments_len - offset)
      ids = [f"{node_id}/{offset + i}" for i in range(ids_len)]
      self._db.delete(ids=ids)

class _EmbeddingFunction(EmbeddingFunction):
  def __init__(self, model_id: str):
    self._model_id: str = model_id
    self._model: SentenceTransformer | None = None

  def __call__(self, input: Documents) -> Embeddings:
    if self._model is None:
      self._model = SentenceTransformer(
        model_name_or_path=self._model_id,
        device="cuda" if torch.cuda.is_available() else "cpu",
      )
    result = self._model.encode(input)
    if not isinstance(result, ndarray):
      raise ValueError("Model output is not a numpy array")
    return result.tolist()