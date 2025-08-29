from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from openai import AsyncOpenAI

from src.services.chalk_example_fraud.transactions.models import (
    TransactionReceipt,
)


async def generate_receipt_body_from_description(
    description: str,
    client: AsyncOpenAI,
) -> str:
    """Generate a transaction receipt body using OpenAI based on the transaction description."""
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a receipt generator. Given a transaction description, create a realistic transaction receipt body with merchant details, transaction details, and relevant information. Keep it concise but informative.",
                },
                {
                    "role": "user",
                    "content": "Generate a transaction receipt body for this description: "
                    + description,
                },
            ],
            max_tokens=10000,
            temperature=0.9,
        )
        content = response.choices[0].message.content
        return content.strip() if content else f"Transaction: {description}"

    except (OSError, ValueError, KeyError) as e:
        print(f"Failed to generate receipt body for description '{description}': {e}")
        return f"Transaction: {description}"


# ----------------------------------------------------------------------------
async def create_transaction_receipt_dict(
    row: dict[str, Any],
    client: AsyncOpenAI,
) -> dict[str, Any]:
    """Create a TransactionReceipt dictionary from a transaction row using OpenAI to generate the body.
    Uses the same transaction ID to match the original transaction.
    This dictionary is validated to match the TransactionReceipt model schema.
    """
    # Parse datetime from CSV
    transaction_at: datetime = (
        datetime.fromisoformat(row["at"]) if isinstance(row["at"], str) else row["at"]
    )

    # Generate receipt body using OpenAI based on the description
    receipt_body: str = await generate_receipt_body_from_description(
        row["description"],
        client,
    )
    receipt_data: dict[str, Any] = {
        "id": int(row["id"]),  # Use same ID as the original transaction
        "at": transaction_at,
        "user_id": int(row["user_id"]),
        "body": receipt_body,
    }

    # Validate the receipt data by attempting to create a TransactionReceipt instance
    # This ensures data quality and type safety
    try:
        TransactionReceipt.dict_to_transaction_receipt(receipt_data)

    except (ValueError, KeyError, TypeError) as e:
        msg = f"Invalid receipt data: {e}"
        raise ValueError(msg) from e

    return receipt_data


# ----------------------------------------------------------------------------
async def generate_receipts_from_transactions(
    transactions: list[dict[str, Any]],
    client: AsyncOpenAI,
) -> AsyncGenerator[dict[str, Any], None]:
    """Generate receipt dictionaries from transaction row data using OpenAI."""
    processed_count: int = 0
    for row in transactions:
        print(f"Processing transaction {row['id']}: {row['description']}")
        try:
            receipt_dict = await create_transaction_receipt_dict(row, client)
            yield receipt_dict
            processed_count += 1

        except (OSError, ValueError, KeyError, TypeError) as e:
            print(f"Failed to process transaction {row['id']}: {e}")
            continue

    print(f"Successfully processed {processed_count} receipts total!")
