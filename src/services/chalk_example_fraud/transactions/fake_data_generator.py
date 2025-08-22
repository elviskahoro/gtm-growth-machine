# trunk-ignore-all(ruff/S311)
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from openai import AsyncOpenAI

# Day of week activity multipliers (Monday=0, Sunday=6)
DAY_OF_WEEK_ACTIVITY: dict[int, float] = {
    0: 1.2,  # Monday - higher activity
    1: 1.1,  # Tuesday
    2: 1.0,  # Wednesday - baseline
    3: 1.0,  # Thursday - baseline
    4: 1.3,  # Friday - highest activity
    5: 0.9,  # Saturday - lower activity
    6: 0.8,  # Sunday - lowest activity
}

# Static constants for weighted choices
TRANSACTION_TYPES = [
    ("purchase", 0.6),
    ("refund", 0.1),
    # ("transfer", 0.2),
    ("deposit", 0.1),
]

MERCHANTS = [
    ("Amazon", 0.3),
    ("Walmart", 0.2),
    ("Target", 0.15),
    ("Starbucks", 0.1),
    ("McDonald's", 0.08),
    ("Shell", 0.07),
    ("Home Depot", 0.05),
    ("Best Buy", 0.03),
    ("Costco", 0.02),
]

CATEGORIES = [
    ("retail", 0.4),
    ("food", 0.25),
    ("gas", 0.15),
    ("entertainment", 0.1),
    ("utilities", 0.05),
    ("healthcare", 0.03),
    ("travel", 0.02),
]

LOCATIONS = [
    ("New York, NY", 0.2),
    ("Los Angeles, CA", 0.15),
    ("Chicago, IL", 0.12),
    ("Houston, TX", 0.1),
    ("Phoenix, AZ", 0.08),
    ("Philadelphia, PA", 0.07),
    ("San Antonio, TX", 0.06),
    ("San Diego, CA", 0.05),
    ("Dallas, TX", 0.05),
    ("San Jose, CA", 0.04),
    ("Austin, TX", 0.04),
    ("Jacksonville, FL", 0.04),
]

# Fraud rate constant
FRAUD_RATE: float = 0.02  # 2% fraud rate


def get_weighted_choice(
    choices: list[tuple[str, float]],
) -> str:
    """Select a random choice based on weighted probabilities.

    Args:
        choices: List of tuples containing (item, weight) pairs.

    Returns:
        The selected item as a string.
    """
    items: tuple[str, ...]
    weights: tuple[float, ...]
    items, weights = zip(*choices)
    return random.choices(items, weights=weights)[0]


def get_time_period(
    start_date: datetime,
    end_date: datetime,
) -> datetime:
    """Generate a random datetime within the specified time period.

    Args:
        start_date: The earliest possible datetime.
        end_date: The latest possible datetime.

    Returns:
        A random datetime between start_date and end_date.
    """
    time_between: timedelta = end_date - start_date
    days_between: int = time_between.days

    # Handle same-day case
    if days_between == 0:
        # Generate random seconds within the same day
        total_seconds: int = int(time_between.total_seconds())
        random_seconds: int = random.randrange(max(1, total_seconds))
        return start_date + timedelta(seconds=random_seconds)

    # Handle multi-day case
    random_days: int = random.randrange(days_between)
    random_seconds: int = random.randrange(24 * 60 * 60)

    return start_date + timedelta(days=random_days, seconds=random_seconds)


def generate_random_transaction(
    transaction_id: int,
    start_date: datetime,
    end_date: datetime,
) -> dict[str, Any]:
    """Generate a single random transaction with realistic data.

    Args:
        transaction_id: Unique identifier for the transaction.
        start_date: Earliest possible transaction date.
        end_date: Latest possible transaction date.

    Returns:
        Dictionary containing transaction data.
    """
    transaction_type: str = get_weighted_choice(TRANSACTION_TYPES)
    merchant: str = get_weighted_choice(MERCHANTS)
    category: str = get_weighted_choice(CATEGORIES)
    location: str = get_weighted_choice(LOCATIONS)

    # Generate amount based on transaction type
    amount: float
    match transaction_type:
        case "deposit":
            amount = round(random.uniform(1, 120), 2)

        case "refund":
            amount = round(random.uniform(5, 200), 2)

        case "transfer":
            amount = round(random.uniform(50, 1000), 2)

        case _:  # purchase
            amount = round(random.uniform(1, 500), 2)

    # Generate user ID (simulating multiple users)
    user_id: int = random.randint(1000, 9999)

    # Generate transaction timestamp
    timestamp: datetime = get_time_period(start_date, end_date)

    return {
        "transaction_id": transaction_id,
        "user_id": user_id,
        "amount": amount,
        "transaction_type": transaction_type,
        "merchant": merchant,
        "category": category,
        "location": location,
        "timestamp": timestamp,
        "is_fraud": random.random() < FRAUD_RATE,
    }


def create_transaction_dict_from_fake_data(
    transaction_id: int,
    user_id: int,
    base_date: datetime,
) -> dict[str, Any]:
    """Create a transaction dictionary using the fake_data_generator with adapted parameters.

    Args:
        transaction_id: Unique identifier for the transaction.
        user_id: User ID for the transaction.
        base_date: Base date for the transaction.

    Returns:
        Dictionary with transaction data ready for database insertion.
    """
    # Calculate start and end dates for the same day
    start_date: datetime = base_date.replace(hour=6, minute=0, second=0, microsecond=0)
    end_date: datetime = base_date.replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=999999,
    )

    # Generate fake transaction data
    fake_data: dict[str, Any] = generate_random_transaction(
        transaction_id,
        start_date,
        end_date,
    )

    # Convert to transaction dictionary with our specific user_id
    amount_cents: int = int(fake_data["amount"] * 100)  # Convert dollars to cents
    memo: str = (
        f"{fake_data['transaction_type'].title()} at {fake_data['merchant']} - {fake_data['category']}"
    )

    return {
        "id": transaction_id,
        "user_id": user_id,  # Use the provided user_id instead of the random one
        "at": fake_data["timestamp"],
        "memo": memo,
        "amount": amount_cents,
    }


async def generate_receipt_body_from_memo(
    memo: str,
    client: AsyncOpenAI,
) -> str:
    """Generate a transaction receipt body using OpenAI based on the transaction memo."""
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a receipt generator. Given a transaction memo, create a realistic transaction receipt body with merchant details, transaction details, and relevant information. Keep it concise but informative.",
                },
                {
                    "role": "user",
                    "content": "Generate a transaction receipt body for this memo: "
                    + memo,
                },
            ],
            max_tokens=10000,
            temperature=0.9,
        )
        return response.choices[0].message.content.strip()

    except (OSError, ValueError, KeyError) as e:
        print(f"Failed to generate receipt body for memo '{memo}': {e}")
        return f"Transaction: {memo}"


# ----------------------------------------------------------------------------
async def create_transaction_receipt_dict(
    row: dict[str, Any],
    client: AsyncOpenAI,
) -> dict[str, Any]:
    """Create a TransactionReceipt dictionary from a transaction row using OpenAI to generate the body.
    Uses the same transaction ID to match the original transaction.
    """
    # Parse datetime from CSV
    transaction_at: datetime = (
        datetime.fromisoformat(row["at"]) if isinstance(row["at"], str) else row["at"]
    )

    # Generate receipt body using OpenAI based on the memo
    receipt_body: str = await generate_receipt_body_from_memo(row["memo"], client)
    return {
        "id": int(row["id"]),  # Use same ID as the original transaction
        "at": transaction_at,
        "user_id": int(row["user_id"]),
        "body": receipt_body,
    }


# ----------------------------------------------------------------------------
async def process_single_transaction_dict(
    row: dict[str, Any],
    client: AsyncOpenAI,
) -> dict[str, Any] | None:
    """Process a single transaction and return a receipt dictionary."""
    print(f"Processing transaction {row['id']}: {row['memo']}")
    try:
        return await create_transaction_receipt_dict(row, client)

    except (OSError, ValueError, KeyError, TypeError) as e:
        print(f"Failed to process transaction {row['id']}: {e}")
        return None


async def generate_receipts_from_transaction_rows(
    transaction_rows: list[dict[str, Any]],
    client: AsyncOpenAI,
    max_receipts: int = 2000,
) -> AsyncGenerator[dict[str, Any], None]:
    """Generate receipt dictionaries from transaction row data using OpenAI."""
    print(
        f"Processing up to {max_receipts} receipts from {len(transaction_rows)} transactions...",
    )

    processed_count: int = 0
    for row in transaction_rows:
        if processed_count >= max_receipts:
            print(
                f"Reached maximum limit of {max_receipts} receipts, stopping processing",
            )
            break

        receipt_dict: dict[str, Any] | None = await process_single_transaction_dict(
            row,
            client,
        )
        if receipt_dict:
            yield receipt_dict
            processed_count += 1

    print(f"Successfully processed {processed_count} receipts total!")


def calculate_daily_transaction_count(
    target_date: datetime,
    min_daily_transactions: int = 2,
    max_daily_transactions: int = 11,
) -> int:
    """Calculate number of transactions for the day based on date patterns."""
    day_of_week: int = target_date.weekday()
    activity_multiplier: float = DAY_OF_WEEK_ACTIVITY.get(day_of_week, 1.0)

    # Base count with some randomness
    date_seed: int = target_date.day + target_date.month * 31
    base_count: int = min_daily_transactions + (
        date_seed % (max_daily_transactions - min_daily_transactions)
    )

    # Apply day-of-week multiplier
    adjusted_count: int = int(base_count * activity_multiplier)

    # Ensure within bounds
    return max(min_daily_transactions, min(max_daily_transactions, adjusted_count))


async def generate_receipt_dict_for_transaction(
    transaction_dict: dict[str, Any],
    client: AsyncOpenAI,
) -> dict[str, Any]:
    """Generate a receipt dictionary for a specific transaction dictionary."""
    receipt_body: str = await generate_receipt_body_from_memo(
        transaction_dict["memo"],
        client,
    )
    return {
        "id": transaction_dict["id"],  # Use same ID as the transaction
        "at": transaction_dict["at"],
        "user_id": transaction_dict["user_id"],
        "body": receipt_body,
    }


def generate_daily_transaction_dicts(
    target_date: datetime | None = None,
    user_id: int = 1,
    starting_id: int = 1,
    min_daily_transactions: int = 2,
    max_daily_transactions: int = 11,
) -> Generator[dict[str, Any], None, None]:
    """Generate realistic daily transaction dictionaries for a given date."""
    if target_date is None:
        # Use today's date in UTC
        target_date = datetime.now(tz=timezone.utc).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    # Calculate dynamic transaction count based on date patterns
    transaction_count: int = calculate_daily_transaction_count(
        target_date,
        min_daily_transactions,
        max_daily_transactions,
    )
    day_name: str = target_date.strftime("%A")

    print(
        f"Generating {transaction_count} transactions for {target_date.date()} ({day_name})...",
    )

    # Generate transactions
    for i in range(transaction_count):
        transaction_dict: dict[str, Any] = create_transaction_dict_from_fake_data(
            transaction_id=starting_id + i,
            user_id=user_id,
            base_date=target_date,
        )
        yield transaction_dict

    print(
        f"Successfully generated {transaction_count} transactions for {target_date.date()}!",
    )


async def generate_daily_receipt_dicts(
    transaction_dicts: list[dict[str, Any]],
    client: AsyncOpenAI,
) -> AsyncGenerator[dict[str, Any], None]:
    """Generate receipt dictionaries for a list of transaction dictionaries."""
    print(f"Generating receipts for {len(transaction_dicts)} transactions...")

    for transaction_dict in transaction_dicts:
        print(
            f"Generating receipt for transaction {transaction_dict['id']}: {transaction_dict['memo']}",
        )
        receipt_dict: dict[str, Any] = await generate_receipt_dict_for_transaction(
            transaction_dict,
            client,
        )
        yield receipt_dict

    print(f"Successfully generated {len(transaction_dicts)} receipts!")
