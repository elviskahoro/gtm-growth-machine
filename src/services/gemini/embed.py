from __future__ import annotations

import os
from collections import deque
from typing import TYPE_CHECKING

from google.api_core.exceptions import BadRequest
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


MAX_VALUE_LENGTH: int = 50  # Maximum length for displaying field values in identifiers


def _get_model_identifier(
    base_model: BaseModel,
) -> str:
    """Extract an identifier from a base model for error reporting.

    Tries to get a primary key or other identifying field from the model.
    """
    model_dict: dict[str, object] = base_model.model_dump()

    # Try common primary key field names
    for key_field in ["id", "primary_key", "pk", "key", "uuid"]:
        if key_field in model_dict:
            return f"{key_field}={model_dict[key_field]}"

    # If no primary key found, return the first field with a value
    for key, value in model_dict.items():
        if value is not None:
            value_str: str = str(value)
            if len(value_str) > MAX_VALUE_LENGTH:
                value_str = value_str[:MAX_VALUE_LENGTH] + "..."
            return f"{key}={value_str}"

    return "unknown_model"


def _embed_single(
    embedding_model: TextEmbeddingModel,
    base_model: BaseModel,
) -> dict[str, object] | None:
    """Embed a single model and return the result, or None if it fails.

    Returns:
        Dictionary with model data and embedding, or None if embedding failed.
    """
    try:
        text_to_embed: str | TextEmbeddingInput = (
            base_model.gemini_get_column_to_embed()
        )
        embeddings: list[TextEmbedding] = embedding_model.get_embeddings(
            texts=[text_to_embed],
        )

    except BadRequest as e:
        error_msg: str = str(e)
        model_id: str = _get_model_identifier(base_model=base_model)

        # Handle token limit errors
        if "token count" in error_msg.lower() or "input token" in error_msg.lower():
            text_content: str = base_model.gemini_get_column_to_embed()
            text_length: int = len(text_content) if isinstance(text_content, str) else 0

            print(f"FAILED: Record {model_id} - text length: {text_length} chars")
            print(f"  Reason: Text exceeds token limit (max 20,000 tokens)")
            print(f"  Error: {error_msg}")
            return None

        # Handle empty text errors
        if "text content is empty" in error_msg.lower() or "empty" in error_msg.lower():
            text_content: str = base_model.gemini_get_column_to_embed()
            text_length: int = len(text_content) if isinstance(text_content, str) else 0

            print(f"FAILED: Record {model_id} - text is empty or whitespace only")
            print(f"  Text length: {text_length} chars")
            print(f"  Text repr: {text_content[:100]!r}")
            print(f"  Full record: {base_model.model_dump_json(indent=2)[:500]}...")
            return None

        # Re-raise if it's a different type of error
        raise
    else:
        data: dict[str, object] = base_model.model_dump(
            mode="python",
            exclude_unset=True,
        )
        data["embedding"] = embeddings[0].values
        return data


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

    try:
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

    except BadRequest as e:
        error_msg: str = str(e)
        is_token_error: bool = (
            "token count" in error_msg.lower() or "input token" in error_msg.lower()
        )
        is_empty_error: bool = (
            "text content is empty" in error_msg.lower() or "empty" in error_msg.lower()
        )

        if is_token_error or is_empty_error:
            if is_token_error:
                print(f"Batch embedding failed due to token limit: {error_msg}")
            elif is_empty_error:
                print(f"Batch embedding failed due to empty text: {error_msg}")

            print(
                f"Falling back to individual processing for {len(base_models_list)} records...",
            )

            # Process each model individually to identify which ones fail
            results: list[dict[str, object]] = []
            failed_count: int = 0

            for idx, base_model in enumerate(base_models_list, start=1):
                result: dict[str, object] | None = _embed_single(
                    embedding_model=embedding_model,
                    base_model=base_model,
                )
                if result is not None:
                    results.append(result)
                else:
                    failed_count += 1

                # Print progress every 50 records
                if idx % 50 == 0 or idx == len(base_models_list):
                    print(
                        f"  Processed {idx}/{len(base_models_list)} records ({failed_count} failed)",
                    )

            if failed_count > 0:
                print(
                    f"WARNING: {failed_count} record(s) failed to embed and were skipped",
                )

            return results

        # Re-raise if it's a different error
        raise


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
def test_get_model_identifier_with_id() -> None:
    """Test _get_model_identifier when model has an id field."""
    from pydantic import BaseModel

    class TestModel(BaseModel):
        id: str
        name: str

    model: TestModel = TestModel(id="test-123", name="Test Name")
    result: str = _get_model_identifier(base_model=model)
    assert result == "id=test-123"


def test_get_model_identifier_without_id() -> None:
    """Test _get_model_identifier when model has no id field."""
    from pydantic import BaseModel

    class TestModel(BaseModel):
        name: str
        value: int

    model: TestModel = TestModel(name="Test Name", value=42)
    result: str = _get_model_identifier(base_model=model)
    assert result in {"name=Test Name", "value=42"}


def test_get_model_identifier_with_long_value() -> None:
    """Test _get_model_identifier truncates long values."""
    from pydantic import BaseModel

    class TestModel(BaseModel):
        description: str

    long_text: str = "a" * 100
    model: TestModel = TestModel(description=long_text)
    result: str = _get_model_identifier(base_model=model)
    assert len(result) < len(long_text) + 20  # Should be truncated
    assert "..." in result


def test_embed_single_success() -> None:
    """Test _embed_single successfully embeds a single record."""
    from unittest.mock import Mock

    from pydantic import BaseModel

    class TestModel(BaseModel):
        text: str

        def gemini_get_column_to_embed(self) -> str:
            return self.text

    mock_model: Mock = Mock()
    mock_embedding: Mock = Mock()
    mock_embedding.values = [0.1, 0.2, 0.3]
    mock_model.get_embeddings.return_value = [mock_embedding]

    test_model: TestModel = TestModel(text="test")
    result: dict[str, object] | None = _embed_single(
        embedding_model=mock_model,
        base_model=test_model,
    )

    assert result is not None
    assert result["text"] == "test"
    assert result["embedding"] == [0.1, 0.2, 0.3]


def test_embed_single_token_limit_failure() -> None:
    """Test _embed_single returns None when token limit is exceeded."""
    from unittest.mock import Mock, patch

    from pydantic import BaseModel

    class TestModel(BaseModel):
        id: str
        text: str

        def gemini_get_column_to_embed(self) -> str:
            return self.text

    mock_model: Mock = Mock()
    mock_model.get_embeddings.side_effect = BadRequest(
        "Unable to submit request because the input token count is 25000 but the model supports up to 20000",
    )

    test_model: TestModel = TestModel(id="test-123", text="very long text" * 1000)

    with patch("builtins.print"):
        result: dict[str, object] | None = _embed_single(
            embedding_model=mock_model,
            base_model=test_model,
        )

    assert result is None


def test_embed_batch_fallback_to_individual() -> None:
    """Test _embed falls back to individual processing on batch token limit error."""
    from unittest.mock import Mock, patch

    from pydantic import BaseModel

    class TestModel(BaseModel):
        id: str
        text: str

        def gemini_get_column_to_embed(self) -> str:
            return self.text

    mock_model: Mock = Mock()

    # First call (batch) fails, individual calls succeed
    batch_error: BadRequest = BadRequest(
        "Unable to submit request because the input token count is 75000",
    )
    mock_embedding: Mock = Mock()
    mock_embedding.values = [0.1, 0.2, 0.3]

    # Set up side effects: batch fails, then individual calls succeed
    mock_model.get_embeddings.side_effect = [
        batch_error,
        [mock_embedding],
        [mock_embedding],
    ]

    test_models: list[TestModel] = [
        TestModel(id="1", text="text1"),
        TestModel(id="2", text="text2"),
    ]

    with patch("builtins.print"):
        result: list[dict[str, object]] = _embed(
            embedding_model=mock_model,
            base_models=test_models,
        )

    assert len(result) == 2
    assert result[0]["id"] == "1"
    assert result[1]["id"] == "2"


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
