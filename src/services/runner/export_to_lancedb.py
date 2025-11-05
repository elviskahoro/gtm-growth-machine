# trunk-ignore-all(ruff/PGH003,trunk/ignore-does-nothing)
from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

import modal
from modal import Image

from src.services.gemini.embed import (
    embed_with_gemini,
    init_client as gemini_init_client,
)
from src.services.lance.setup import init_client as lance_init_client
from src.services.lance.upload import upload_to_lance
from src.services.local.filesystem import SourceFileData

if TYPE_CHECKING:
    from collections.abc import Iterator

    from lancedb.db import DBConnection
    from pydantic import BaseModel

# trunk-ignore-begin(ruff/F401,ruff/I001,pyright/reportUnusedImport)
from src.services.chalk_demo.fraud_transactions.webhook import (
    Webhook as FraudTransactionWebhook,  # noqa: F401
)
from src.services.chalk_demo.marketplace_product.webhook import (
    Webhook as MarketplaceProductWebhook,  # noqa: F401
)
from src.services.chalk_demo.marketplace_product_description.webhook import (
    Webhook as MarketplaceProductDescriptionWebhook,  # noqa: F401
)
from src.services.fathom.etl.message import (
    Webhook as FathomMessageWebhook,  # noqa: F401
)
from src.services.octolens.etl import Webhook as OctolensWebhook  # noqa: F401

# trunk-ignore-end(ruff/F401,ruff/I001,pyright/reportUnusedImport)


class WebhookModel(MarketplaceProductWebhook):  # type: ignore # trunk-ignore(ruff/F821)
    pass


WebhookModel.model_rebuild()

BUCKET_NAME: str = WebhookModel.etl_get_bucket_name()

GEMINI_EMBED_BATCH_SIZE: int = 35  # Maximum allowed by Vertex AI text-embedding-005
UPLOAD_DELAY_SECONDS: float = 0.1  # Delay between uploads to prevent rate limiting
MAX_RETRY_ATTEMPTS: int = 3  # Maximum retry attempts for rate limited uploads


image: Image = modal.Image.debian_slim().uv_pip_install(
    "fastapi[standard]",
    "google-cloud-aiplatform",
    "lancedb",
    "pyarrow",
)
image = image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name=WebhookModel.lance_get_project_name(),
    image=image,
)

VOLUME: modal.Volume = modal.Volume.from_name(
    BUCKET_NAME,
    create_if_missing=True,
)


@app.function(
    volumes={
        f"/{BUCKET_NAME}": VOLUME,
    },
)
def _get_data_from_storage_remote() -> str:
    path: Path = Path(f"/{BUCKET_NAME}/storage.json")
    if not path.exists():
        error: str = "File not found in the volume"
        raise FileNotFoundError(error)

    return path.read_text()


def _get_storage_source_file_data(
    local_storage_path: str | None,
) -> SourceFileData | None:
    if local_storage_path is not None:
        return SourceFileData.from_local_storage_path(
            local_storage_path=local_storage_path,
            base_model_type=WebhookModel.storage_get_base_model_type(),
        )

    if WebhookModel.etl_expects_storage_file():
        return SourceFileData.from_json_data(
            json_data=_get_data_from_storage_remote.remote(),  # trunk-ignore(pyright/reportFunctionMemberAccess)
            base_model_type=WebhookModel.storage_get_base_model_type(),
        )

    print("Assuming storage file is not expected for the webhook model")
    return None


def _get_existing_primary_keys(
    db: DBConnection,
    table_name: str,
    primary_key: str,
) -> set[str] | None:
    """Get existing primary keys from LanceDB table.

    Returns:
        Set of existing primary keys, or None if table doesn't exist.
    """
    try:
        tbl = db.open_table(name=table_name)

        # LanceDB Cloud has a 10,000 record limit per query, so we need to paginate
        existing_keys: set[str] = set()
        page_size: int = 10000
        offset: int = 0

        print("Fetching existing primary keys from LanceDB...")

        while True:
            try:
                # Fetch a page of primary keys
                result = (
                    tbl.search()
                    .select([primary_key])
                    .limit(page_size)
                    .offset(offset)
                    .to_arrow()
                )

                # If no results, we've fetched all records
                if len(result) == 0:
                    break

                # Add keys from this page
                page_keys: list[str] = result[primary_key].to_pylist()
                existing_keys.update(page_keys)

                print(
                    f"Fetched {len(page_keys):07d} keys (total: {len(existing_keys):07d})",
                )

                # If we got fewer records than page_size, we've reached the end
                if len(result) < page_size:
                    break

                offset += page_size

            except Exception as e:
                error_msg: str = str(e)
                if "cannot be bigger than" in error_msg or "limit" in error_msg.lower():
                    print(
                        f"Warning: Cannot fetch existing keys due to pagination limits: {error_msg}",
                    )
                    return None
                raise

    except ValueError as e:
        # Table doesn't exist yet
        if "was not found" in str(e):
            return None
        raise
    else:
        return existing_keys


def _filter_new_base_models(
    base_models_to_embed: list[BaseModel],
    existing_keys: set[str],
    primary_key: str,
) -> list[BaseModel]:
    """Filter out base models that already exist in the database.

    Args:
        base_models_to_embed: List of base models to check
        existing_keys: Set of primary keys that already exist in DB
        primary_key: Name of the primary key field

    Returns:
        List of base models that don't exist in the database yet
    """
    new_models: list[BaseModel] = []
    for model in base_models_to_embed:
        # Get the primary key value from the model
        model_dict: dict[str, str] = model.model_dump()
        model_primary_key: str | None = model_dict.get(primary_key)

        if model_primary_key is not None and model_primary_key not in existing_keys:
            new_models.append(model)

    return new_models


def _validate_text_content(
    base_model: BaseModel,
) -> bool:
    """Validate that a base model has non-empty text content to embed.

    Args:
        base_model: The base model to validate

    Returns:
        True if the model has valid text content, False otherwise
    """
    try:
        text_content: str = base_model.gemini_get_column_to_embed()
        return not (not text_content or not text_content.strip())
    except (AttributeError, ValueError):
        return False


MAX_LOG_VALUE_LENGTH: int = 50  # Maximum length for displaying field values in logs


def _get_model_identifier_for_logging(
    base_model: BaseModel,
) -> str:
    """Get a human-readable identifier for a model for logging purposes.

    Args:
        base_model: The base model

    Returns:
        String identifier for the model
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
            if len(value_str) > MAX_LOG_VALUE_LENGTH:
                value_str = value_str[:MAX_LOG_VALUE_LENGTH] + "..."
            return f"{key}={value_str}"

    return "unknown_model"


# trunk-ignore-begin(ruff/PLR0912,ruff/PLR0915)
def embed_with_gemini_and_upload_to_lance(
    source_file_data: Iterator[SourceFileData],
    embed_batch_size: int,
    storage: BaseModel | None,
    db: DBConnection,
    upload_delay: float = UPLOAD_DELAY_SECONDS,
) -> str:
    base_model_type: type[BaseModel] = WebhookModel.lance_get_base_model_type()
    table_name: str = WebhookModel.lance_get_table_name()
    primary_key: str = WebhookModel.lance_get_primary_key()
    primary_key_index_type: str = WebhookModel.lance_get_primary_key_index_type()

    gemini_init_client()

    # Get existing primary keys to avoid re-uploading
    existing_keys: set[str] | None = _get_existing_primary_keys(
        db=db,
        table_name=table_name,
        primary_key=primary_key,
    )

    # Collect all base models with file path information for better error reporting
    all_base_models_with_paths: list[tuple[BaseModel, str | None]] = []
    for source_data in source_file_data:
        file_path_str: str | None = str(source_data.path) if source_data.path else None
        for base_model in source_data.base_model.etl_get_base_models(storage=storage):
            all_base_models_with_paths.append((base_model, file_path_str))

    print(f"Batch size {embed_batch_size:04d}")
    print(f"Total models to process: {len(all_base_models_with_paths):07d}")

    # Validate text content and filter out invalid records
    validated_models: list[BaseModel] = []
    invalid_text_count: int = 0

    for base_model, file_path in all_base_models_with_paths:
        if not _validate_text_content(base_model=base_model):
            invalid_text_count += 1
            model_id: str = _get_model_identifier_for_logging(base_model=base_model)
            text_content: str = base_model.gemini_get_column_to_embed()
            print(f"SKIPPING: Record {model_id} - empty or whitespace-only text")
            print(f"  File: {file_path or 'webhook/direct input'}")
            print(
                f"  Text length: {len(text_content)} chars, stripped: {len(text_content.strip())} chars",
            )
            print(f"  Full record: {base_model.model_dump_json(indent=2)[:300]}...")
        else:
            validated_models.append(base_model)

    if invalid_text_count > 0:
        print(f"Skipped {invalid_text_count:07d} records with empty text content")

    all_base_models: list[BaseModel] = validated_models

    if existing_keys:
        print(f"Found {len(existing_keys):07d} existing records in LanceDB")

    else:
        print("No existing records found - plan on creating a new table")

    count: int = 0
    skipped: int = 0

    # Filter out models that already exist if we have existing keys
    filtered_models: list[BaseModel] = all_base_models
    if existing_keys:
        original_count: int = len(all_base_models)
        filtered_models = _filter_new_base_models(
            base_models_to_embed=all_base_models,
            existing_keys=existing_keys,
            primary_key=primary_key,
        )

        skipped_total: int = original_count - len(filtered_models)
        skipped += skipped_total

        if skipped_total > 0:
            print(f"{skipped_total:07d} records already exist - skipping")

    if not filtered_models:
        print("No new records to process")
        return f"Successfully processed {count:07d} batches and uploaded to LanceDB. Skipped {skipped:07d} existing records."

    print(f"{len(filtered_models):07d} new records to embed with Gemini")
    for data_to_upload in embed_with_gemini(
        base_models_to_embed=iter(filtered_models),
        embed_batch_size=embed_batch_size,
    ):
        upload_to_lance(
            data_to_upload=data_to_upload,
            base_model_type=base_model_type,
            db=db,
            table_name=table_name,
            primary_key=primary_key,
            primary_key_index_type=primary_key_index_type,
        )
        count += 1
        print(f"{count:07d} Uploaded to LanceDB")

        # Add a configurable delay between uploads to prevent rate limiting
        if upload_delay > 0:
            time.sleep(upload_delay)

    final_message: str = (
        f"Successfully processed {count:07d} batches and uploaded to LanceDB."
    )
    if skipped > 0:
        final_message += f" Skipped {skipped:07d} existing records."

    return final_message


# trunk-ignore-end(ruff/PLR0912,ruff/PLR0915)


@app.function(
    secrets=[
        modal.Secret.from_name(
            name=name,
        )
        for name in WebhookModel.modal_get_secret_collection_names()
    ],
    enable_memory_snapshot=False,
)
@modal.fastapi_endpoint(
    method="POST",
    docs=True,
)
@modal.concurrent(
    max_inputs=1,
)
def web(
    webhook: WebhookModel,
) -> str:
    if not webhook.etl_is_valid_webhook():
        return webhook.etl_get_invalid_webhook_error_msg()

    source_file_data: Iterator[SourceFileData] = iter(
        [
            SourceFileData(
                path=None,
                base_model=webhook,
            ),
        ],
    )
    storage_file_data: SourceFileData | None = _get_storage_source_file_data(
        local_storage_path=None,
    )
    return embed_with_gemini_and_upload_to_lance(
        source_file_data=source_file_data,
        storage=storage_file_data.base_model if storage_file_data else None,
        embed_batch_size=GEMINI_EMBED_BATCH_SIZE,
        db=lance_init_client(
            project_name=WebhookModel.lance_get_project_name(),
        ),
        upload_delay=UPLOAD_DELAY_SECONDS,
    )


@app.local_entrypoint()
def local(
    input_path: str,
    embed_batch_size: int = GEMINI_EMBED_BATCH_SIZE,
    upload_delay: float = UPLOAD_DELAY_SECONDS,
) -> None:
    input_path_obj: Path = Path(input_path)
    match input_path_obj:
        case path if path.is_dir():
            source_file_data: Iterator[SourceFileData] = (
                SourceFileData.from_input_folder(
                    input_folder=input_path,
                    base_model_type=WebhookModel,
                    extension=[
                        ".json",
                        ".jsonl",
                    ],
                )
            )

        case path if path.is_file():
            source_file_data: Iterator[SourceFileData] = SourceFileData.from_jsonl_file(
                jsonl_path=input_path,
                base_model_type=WebhookModel,
            )

        case _:
            error_msg: str = (
                f"Input path {input_path} is neither a file nor a directory"
            )
            raise ValueError(error_msg)

    storage_file_data: SourceFileData | None = _get_storage_source_file_data(
        local_storage_path=None,
    )
    response: str = embed_with_gemini_and_upload_to_lance(
        source_file_data=source_file_data,
        storage=storage_file_data.base_model if storage_file_data else None,
        embed_batch_size=embed_batch_size,
        db=lance_init_client(
            project_name=WebhookModel.lance_get_project_name(),
        ),
        upload_delay=upload_delay,
    )
    print(response)
