import spacy
import langid
import threading

from dataclasses import dataclass
from spacy.language import Language
from spacy.tokens import Doc

_Sentence = tuple[Doc, int, int]

@dataclass
class Segment:
  start: int
  end: int
  text: str

# Thread safety
class Segmentation:

  # https://spacy.io/
  def __init__(self) -> None:
    self._lock: threading.Lock = threading.Lock()
    self._nlp_dict: dict[str, Language] = {}
    self._lan2model: dict = {
      "en": "en_core_web_sm",
      "zh": "zh_core_web_sm",
    }

  def split(self, text: str) -> list[Segment]:
    lan, _ = langid.classify(text)
    nlp = self._nlp(lan)
    doc = nlp(text)
    sentences = self._to_sentences(nlp, doc)

    if len(sentences) == 0:
      segments = []
    else:
      segments = self._group_sentences(
        # this article say: We’ll use a value of 0.8
        # to see: https://blandthony.medium.com/methods-for-semantic-text-segmentation-prior-to-generating-text-embeddings-vectorization-6442afdb086
        threshold=0.8,
        sentences=sentences,
      )
    return segments

  def to_keywords(self, text: str) -> list[str]:
    lan, _ = langid.classify(text)
    nlp = self._nlp(lan)
    keywords: list[str] = []
    for chunk in nlp(text):
      if not chunk.is_stop:
        keywords.append(chunk.text)
    return keywords

  def _nlp(self, lan: str) -> Language:
    with self._lock:
      nlp = self._nlp_dict.get(lan, None)
      if nlp is None:
        model_id = self._lan2model.get(lan, None)
        if model_id is None:
          model_id = self._lan2model.get("en", None)
          if model_id is None:
            raise ValueError("no model found for input text.")
        nlp = spacy.load(model_id)
        self._nlp_dict[lan] = nlp
      return nlp

  def _to_sentences(self, nlp: Language, doc: Doc) -> list[_Sentence]:
    sentences: list[_Sentence] = []
    for sent in doc.sents:
      sentences.append((
        nlp(sent.text),
        sent.start_char,
        sent.end_char,
      ))
    return sentences

  def _group_sentences(self, sentences: list[_Sentence], threshold: float) -> list[Segment]:
    segments: list[Segment] = []
    start_idx: int = 0
    end_idx: int = 1
    current: list[_Sentence] = [sentences[start_idx]]

    while end_idx < len(sentences):
      start_sentence = sentences[start_idx]
      end_sentence = sentences[end_idx]
      start_doc = start_sentence[0]
      end_doc = end_sentence[0]

      if start_doc.similarity(end_doc) >= threshold:
        current.append(end_sentence)
      else:
        segments.append(self._merge_to_segment(current))
        start_idx = end_idx
        current = [end_sentence]

      end_idx += 1

    if len(current) > 0:
      segments.append(self._merge_to_segment(current))

    return segments

  def _merge_to_segment(self, sentences: list[_Sentence]) -> Segment:
    start_sentence = sentences[0]
    end_sentence = sentences[-1]
    texts: list[str] = []

    for sentence in sentences:
      text_list: list[str] = []
      for chunk in sentence[0]:
        if not chunk.is_stop:
          text_list.append(chunk.text)
      text = " ".join(text_list)
      texts.append(text)

    return Segment(
      start=start_sentence[1],
      end=end_sentence[2],
      text=" ".join(texts),
    )