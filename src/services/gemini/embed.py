from __future__ import annotations

import os
from collections import deque
from typing import TYPE_CHECKING

from google.cloud import aiplatform
from vertexai.preview.language_models import (
    TextEmbedding,
    TextEmbeddingInput,
    TextEmbeddingModel,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator

    from pydantic import BaseModel


def _init_client(
    location: str = "us-east1",
) -> None:
    aiplatform.init(
        project=os.getenv("GCP_PROJECT_ID"),
        location=location,
    )


def _embed(
    embedding_model: TextEmbeddingModel,
    base_models: Iterable[BaseModel],
) -> list[dict]:
    # Use deque for more efficient appending when building collections
    texts_to_embed_deque = deque()
    base_models_list = list(base_models)  # Convert to list once for reuse

    for base_model in base_models_list:
        texts_to_embed_deque.append(base_model.gemini_get_column_to_embed())

    texts_to_embed: list[str | TextEmbeddingInput] = list(texts_to_embed_deque)

    embeddings: list[TextEmbedding] = embedding_model.get_embeddings(
        texts=texts_to_embed,
    )

    # Use deque for building embedding vectors
    embedding_vectors_deque = deque()
    for embedding in embeddings:
        embedding_vectors_deque.append(embedding.values)

    embedding_vectors: list[list[float]] = list(embedding_vectors_deque)

    def add_embedding(
        base_model: BaseModel,
        embedding: list[float],
    ) -> dict:
        data: dict = base_model.model_dump(
            mode="python",
            exclude_unset=True,
        )
        data["embedding"] = embedding
        return data

    return list(map(add_embedding, base_models_list, embedding_vectors))


def embed_with_gemini(
    base_models_to_embed: Iterator[BaseModel],
    embed_batch_size: int,
) -> Generator[list[dict], None, None]:
    _init_client()
    model: TextEmbeddingModel = TextEmbeddingModel.from_pretrained(
        model_name="text-embedding-005",
    )
    batch: list[BaseModel] = []
    for item in base_models_to_embed:
        batch.append(item)
        if len(batch) >= embed_batch_size:
            embedded_data = _embed(
                embedding_model=model,
                base_models=batch,
            )
            yield embedded_data
            batch = []

    # Handle any remaining items in the last batch
    if batch:
        embedded_data = _embed(
            embedding_model=model,
            base_models=batch,
        )
        yield embedded_data
