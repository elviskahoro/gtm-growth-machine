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
) -> None:
    texts_to_embed: list[str | TextEmbeddingInput] = [
        base_model.gemini_get_column_to_embed() for base_model in base_models
    ]
    embeddings: list[TextEmbedding] = embedding_model.get_embeddings(
        texts=texts_to_embed,
    )
    embedding_vectors: list[list[float]] = [
        embedding.values for embedding in embeddings
    ]

    def add_embedding(
        base_model: BaseModel,
        embedding: list[float],
    ) -> None:
        base_model.add_embedding(embedding)

    deque(map(add_embedding, base_models, embedding_vectors))


def embed_with_gemini(
    base_models_to_embed: Iterator[BaseModel],
    batch_size: int,
) -> Generator[list[BaseModel], None, None]:
    _init_client()
    model: TextEmbeddingModel = TextEmbeddingModel.from_pretrained(
        model_name="text-embedding-005",
    )
    batch: list[BaseModel] = []
    for item in base_models_to_embed:
        batch.append(item)
        if len(batch) >= batch_size:
            _embed(
                embedding_model=model,
                base_models=batch,
            )
            yield batch
            batch = []

    # Handle any remaining items in the last batch
    if batch:
        _embed(
            embedding_model=model,
            base_models=batch,
        )
        yield batch
