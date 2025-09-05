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


def init_client(
    location: str = "us-east1",
) -> None:
    aiplatform.init(
        project=os.getenv("GCP_PROJECT_ID"),
        location=location,
    )


def _embed(
    embedding_model: TextEmbeddingModel,
    base_models: Iterable[BaseModel],
) -> list[dict[str, object]]:
    # Use deque for more efficient appending when building collections
    texts_to_embed_deque: deque[str | TextEmbeddingInput] = deque()
    base_models_list: list[BaseModel] = list(
        base_models,
    )  # Convert to list once for reuse

    for base_model in base_models_list:
        texts_to_embed_deque.append(base_model.gemini_get_column_to_embed())

    texts_to_embed: list[str | TextEmbeddingInput] = list(texts_to_embed_deque)

    embeddings: list[TextEmbedding] = embedding_model.get_embeddings(
        texts=texts_to_embed,
    )

    # Use deque for building embedding vectors
    embedding_vectors_deque: deque[list[float]] = deque()
    for embedding in embeddings:
        embedding_vectors_deque.append(embedding.values)

    embedding_vectors: list[list[float]] = list(embedding_vectors_deque)

    def add_embedding(
        base_model: BaseModel,
        embedding: list[float],
    ) -> dict[str, object]:
        data: dict[str, object] = base_model.model_dump(
            mode="python",
            exclude_unset=True,
        )
        data["embedding"] = embedding
        return data

    return list(map(add_embedding, base_models_list, embedding_vectors))


def embed_with_gemini(
    base_models_to_embed: Iterator[BaseModel],
    embed_batch_size: int,
) -> Generator[list[dict[str, object]], None, None]:
    max_api_batch_size: int = 250  # text-embedding-005 limit
    if embed_batch_size > max_api_batch_size:
        print(
            f"Warning: Batch size {embed_batch_size} exceeds API limit. Using {max_api_batch_size} instead.",
        )
        embed_batch_size = max_api_batch_size - 1

    model: TextEmbeddingModel = TextEmbeddingModel.from_pretrained(
        model_name="text-embedding-005",
    )

    batch: list[BaseModel] = []
    for item in base_models_to_embed:
        batch.append(item)
        if len(batch) >= embed_batch_size:
            embedded_data: list[dict[str, object]] = _embed(
                embedding_model=model,
                base_models=batch,
            )
            yield embedded_data
            batch = []

    # Handle any remaining items in the last batch
    if batch:
        embedded_data: list[dict[str, object]] = _embed(
            embedding_model=model,
            base_models=batch,
        )
        yield embedded_data


# trunk-ignore-begin(ruff/PLR2004,ruff/S101)
def test_init_client_with_default_location() -> None:
    """Test init_client with default location parameter."""
    from unittest.mock import patch

    with (
        patch.dict(os.environ, {"GCP_PROJECT_ID": "test-project"}),
        patch("src.services.gemini.embed.aiplatform.init") as mock_init,
    ):
        init_client()
        mock_init.assert_called_once_with(
            project="test-project",
            location="us-east1",
        )


def test_init_client_with_custom_location() -> None:
    """Test init_client with custom location parameter."""
    from unittest.mock import patch

    with (
        patch.dict(os.environ, {"GCP_PROJECT_ID": "test-project"}),
        patch("src.services.gemini.embed.aiplatform.init") as mock_init,
    ):
        init_client(location="us-west1")
        mock_init.assert_called_once_with(
            project="test-project",
            location="us-west1",
        )


def test_init_client_missing_project_id() -> None:
    """Test init_client when GCP_PROJECT_ID is not set."""
    from unittest.mock import patch

    with (
        patch.dict(os.environ, {}, clear=True),
        patch("src.services.gemini.embed.aiplatform.init") as mock_init,
    ):
        init_client()
        mock_init.assert_called_once_with(
            project=None,
            location="us-east1",
        )


def test_embed_with_gemini_normal_batch_size() -> None:
    """Test embed_with_gemini with normal batch size."""
    from unittest.mock import patch

    from pydantic import BaseModel

    class TestModel(BaseModel):
        text: str

        def gemini_get_column_to_embed(self) -> str:
            return self.text

    test_models: list[TestModel] = [TestModel(text=f"text {i}") for i in range(3)]

    with patch("src.services.gemini.embed.TextEmbeddingModel.from_pretrained"):
        # Directly patch the function in the module's globals
        original_embed = globals()["_embed"]

        def mock_embed(
            *,
            embedding_model: TextEmbeddingModel,
            base_models: Iterable[BaseModel],
        ) -> list[dict[str, object]]:
            del embedding_model, base_models  # Mark as intentionally unused
            return [{"text": "test", "embedding": [0.1, 0.2, 0.3]}]

        globals()["_embed"] = mock_embed

        try:
            result: list[list[dict[str, object]]] = list(
                embed_with_gemini(
                    base_models_to_embed=iter(test_models),
                    embed_batch_size=2,
                ),
            )
            assert len(result) == 2  # Two batches: [0,1] and [2]

        finally:
            globals()["_embed"] = original_embed


def test_embed_with_gemini_oversized_batch() -> None:
    """Test embed_with_gemini with batch size exceeding API limit."""
    from unittest.mock import patch

    from pydantic import BaseModel

    class TestModel(BaseModel):
        text: str

        def gemini_get_column_to_embed(self) -> str:
            return self.text

    test_models: list[TestModel] = [TestModel(text="test")]

    with (
        patch("src.services.gemini.embed._embed") as mock_embed,
        patch("src.services.gemini.embed.TextEmbeddingModel.from_pretrained"),
        patch("builtins.print") as mock_print,
    ):
        mock_embed.return_value = [{"text": "test", "embedding": [0.1]}]

        list(
            embed_with_gemini(
                base_models_to_embed=iter(test_models),
                embed_batch_size=300,  # Exceeds limit of 250
            ),
        )

        mock_print.assert_called_once()
        assert "Warning: Batch size 300 exceeds API limit" in str(mock_print.call_args)


def test_embed_with_gemini_empty_iterator() -> None:
    """Test embed_with_gemini with empty iterator."""
    from unittest.mock import patch

    with (
        patch("src.services.gemini.embed._embed") as mock_embed,
        patch("src.services.gemini.embed.TextEmbeddingModel.from_pretrained"),
    ):
        result: list[list[dict[str, object]]] = list(
            embed_with_gemini(
                base_models_to_embed=iter([]),
                embed_batch_size=5,
            ),
        )

        assert result == []
        mock_embed.assert_not_called()


def test_embed_with_gemini_exact_batch_size() -> None:
    """Test embed_with_gemini when items exactly fill batches."""
    from unittest.mock import patch

    from pydantic import BaseModel

    class TestModel(BaseModel):
        text: str

        def gemini_get_column_to_embed(self) -> str:
            return self.text

    test_models: list[TestModel] = [TestModel(text=f"text {i}") for i in range(4)]

    with patch("src.services.gemini.embed.TextEmbeddingModel.from_pretrained"):
        # Directly patch the function in the module's globals
        original_embed = globals()["_embed"]

        call_count = 0

        def mock_embed(
            *,
            embedding_model: TextEmbeddingModel,
            base_models: Iterable[BaseModel],
        ) -> list[dict[str, object]]:
            del embedding_model, base_models  # Mark as intentionally unused
            nonlocal call_count
            call_count += 1
            return [{"text": "test", "embedding": [0.1]}]

        globals()["_embed"] = mock_embed

        try:
            result: list[list[dict[str, object]]] = list(
                embed_with_gemini(
                    base_models_to_embed=iter(test_models),
                    embed_batch_size=2,
                ),
            )

            assert len(result) == 2  # Exactly 2 batches
            assert call_count == 2

        finally:
            globals()["_embed"] = original_embed


def test_embed_model_dump_behavior() -> None:
    """Test that _embed correctly uses model_dump with proper parameters."""
    from unittest.mock import Mock

    from pydantic import BaseModel

    class TestModel(BaseModel):
        text: str
        optional_field: str | None = None

        def gemini_get_column_to_embed(self) -> str:
            return self.text

    mock_embedding_model: Mock = Mock()
    mock_embedding: Mock = Mock()
    mock_embedding.values = [0.1, 0.2, 0.3]
    mock_embedding_model.get_embeddings.return_value = [mock_embedding]

    test_model: TestModel = TestModel(text="test text")  # optional_field is unset
    result: list[dict[str, object]] = _embed(
        embedding_model=mock_embedding_model,
        base_models=[test_model],
    )

    # Should exclude unset fields
    assert "optional_field" not in result[0] or result[0]["optional_field"] is None
    assert result[0]["text"] == "test text"
    assert result[0]["embedding"] == [0.1, 0.2, 0.3]


# trunk-ignore-end(ruff/PLR2004,ruff/S101)
