from __future__ import annotations

import asyncio
import random
import secrets
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import modal
from modal import Image
from openai import AsyncOpenAI
from sqlalchemy import extract, func

from src.services.chalk_example_fraud.transactions.fake_data_generator import (
    generate_receipts_from_transactions,
)
from src.services.chalk_example_fraud.transactions.models import (
    Base,
    Transaction,
    TransactionReceipt,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session, sessionmaker

APP_NAME: str = "chalk-fraud-transactions-data-generator"

image: Image = modal.Image.debian_slim().uv_pip_install(
    "anyio",
    "openai",
    "polars",
    "psycopg2-binary",
    "sqlalchemy",
)
image = image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name=APP_NAME,
    image=image,
)


async def generate_daily_transactions(
    sql_session: Session,
    target_date: datetime | None = None,
    user_id: int = 1,
) -> AsyncGenerator[Transaction, None]:
    """Generate new transactions for today based on historical transactions matching today's weekday."""
    if target_date is None:
        target_date = datetime.now(tz=timezone.utc)

    # Get today's weekday (0=Monday, 6=Sunday)
    today_weekday: int = target_date.weekday()

    # Query database for all transactions that match today's weekday
    try:
        matching_transactions: list[Transaction] = (
            sql_session.query(Transaction)
            .filter(
                extract("dow", Transaction.at) == (today_weekday + 1) % 7,
            )
            .all()
        )
        if not matching_transactions:
            print(f"No historical transactions found for weekday {today_weekday}")
            return

    except (OSError, ValueError, TypeError) as e:
        print(f"Error querying database: {e}")
        return

    # Get the current max transaction ID from database
    try:
        max_id_result: tuple[int] | None = sql_session.query(
            func.max(Transaction.id),
        ).first()
        next_id: int = (
            (max_id_result[0] + 1) if max_id_result and max_id_result[0] else 1
        )

    except (OSError, ValueError, TypeError):
        # If table doesn't exist or is empty, start from 1
        next_id = 1

    print(
        f"Found {len(matching_transactions)} transactions matching weekday {today_weekday}",
    )

    # Randomly select between 1 and 7 transactions from the matching results
    num_transactions: int = min(secrets.randbelow(7) + 1, len(matching_transactions))
    selected_transactions: list[Transaction] = random.sample(
        matching_transactions, num_transactions,
    )
    print(f"Randomly selected {num_transactions} transactions to generate")

    # Create new transactions for today based on historical patterns
    for historical_transaction in selected_transactions:
        new_at: datetime = target_date.replace(
            hour=historical_transaction.at.hour,
            minute=historical_transaction.at.minute,
            second=historical_transaction.at.second,
            microsecond=historical_transaction.at.microsecond,
        )
        new_transaction: Transaction = Transaction(
            id=next_id,
            amount=historical_transaction.amount,
            at=new_at,
            description=historical_transaction.description,
            user_id=user_id,
            payer_id=historical_transaction.payer_id,
            payee_id=historical_transaction.payee_id,
        )
        yield new_transaction

        next_id += 1

    print(
        f"Generated {num_transactions} new transactions for {target_date.date()}",
    )


async def generate_receipts_for_transactions(
    transactions: list[dict[str, Any]],
    client: AsyncOpenAI,
) -> AsyncGenerator[TransactionReceipt, None]:
    async for receipt_dict in generate_receipts_from_transactions(
        transactions,
        client,
    ):
        receipt: TransactionReceipt = TransactionReceipt.dict_to_transaction_receipt(
            receipt_dict,
        )
        yield receipt

    print("Receipt generation complete!")


async def generate_daily_transactions_with_receipts(
    sql_session: Session,
    client: AsyncOpenAI,
    target_date: datetime | None = None,
    user_id: int = 1,
) -> AsyncGenerator[Transaction | TransactionReceipt, None]:
    """Generate realistic daily transactions and their receipts for a given date."""
    # Collect transactions first
    transactions: list[Transaction] = []
    async for transaction in generate_daily_transactions(
        sql_session=sql_session,
        target_date=target_date,
        user_id=user_id,
    ):
        transactions.append(transaction)
        yield transaction

    # Convert transactions to dictionaries for receipt generation
    transaction_dicts: list[dict[str, Any]] = [
        {
            "id": t.id,
            "amount": t.amount,
            "at": t.at,
            "description": t.description,
            "user_id": t.user_id,
            "payer_id": t.payer_id,
            "payee_id": t.payee_id,
        }
        for t in transactions
    ]
    print(f"Generating receipts for {len(transaction_dicts)} transactions...")
    async for receipt_dict in generate_receipts_for_transactions(
        transaction_dicts,
        client,
    ):
        receipt: TransactionReceipt = TransactionReceipt.dict_to_transaction_receipt(
            receipt_dict,
        )
        yield receipt

    print(f"Daily data generation complete!")


def main(
    generate_receipts: bool = False,  # trunk-ignore(ruff/FBT001,ruff/FBT002)
    target_date: str | None = None,
    user_id: int = 1,
) -> None:
    def create_openai_client() -> AsyncOpenAI:
        """Create OpenAI client using Modal secrets."""
        import os  # trunk-ignore(ruff/PLC0415)

        openai_api_token: str = os.environ["OPENAI_API_KEY"]
        return AsyncOpenAI(api_key=openai_api_token)

    def connect_and_create_tables(
        engine: Engine,
        *ts: type[Base],
    ) -> None:
        for t in ts:
            print(f"Creating or connecting to database table: {t.__tablename__}")
            t.__table__.create(engine, checkfirst=True)

    if target_date is None:
        print("No target_date provided. Generating data with today's date.")
        target_date = datetime.now(tz=timezone.utc).date().isoformat()

    else:
        print("Parameters validated, proceeding with data generation.")

    engine: Engine
    session_maker: sessionmaker[Session]
    engine, session_maker = Base.create_database_connection()
    client: AsyncOpenAI = create_openai_client()
    with session_maker() as sql_session:
        # Create both transaction and receipt tables

        async def collect_and_commit_objects() -> (
            list[Transaction | TransactionReceipt]
        ):
            """Collect all generated objects and commit them in a single transaction."""
            all_objects: list[Transaction | TransactionReceipt] = []

            if target_date:
                target_date_parsed: datetime
                try:
                    target_date_parsed = datetime.fromisoformat(target_date)

                except ValueError:
                    print(
                        f"Invalid date format: {target_date}. Target date must be in ISO format YYYY-MM-DD.",
                    )
                    raise

                if generate_receipts:
                    async for obj in generate_daily_transactions_with_receipts(
                        sql_session=sql_session,
                        client=client,
                        target_date=target_date_parsed,
                        user_id=user_id,
                    ):
                        all_objects.append(obj)

                    return all_objects

                async for obj in generate_daily_transactions(
                    sql_session=sql_session,
                    target_date=target_date_parsed,
                    user_id=user_id,
                ):
                    all_objects.append(obj)

                return all_objects

            msg = "target_date must be provided."
            raise ValueError(msg)

        connect_and_create_tables(
            engine,
            Transaction,
            TransactionReceipt,
        )
        all_objects: list[Transaction | TransactionReceipt] = (
            asyncio.run(
                collect_and_commit_objects(),
            )
            or []
        )
        if not all_objects:
            print("No objects generated to commit.")
            return

        sql_session.add_all(all_objects)
        sql_session.commit()


@app.function(
    schedule=modal.Cron(
        "0 9 * * *",
        timezone="UTC",
    ),
    secrets=[
        modal.Secret.from_name(
            name=APP_NAME + "-elvis",
        ),
    ],
    region="us-east-1",
    enable_memory_snapshot=False,
)
def web() -> None:
    """Core runner function with default parameters for no CLI arguments."""
    main(
        generate_receipts=False,
        target_date=datetime.now(tz=timezone.utc).date().isoformat(),
        user_id=1,
    )


@app.local_entrypoint()
def local(
    *,
    generate_receipts: bool = False,
    target_date: str | None = None,
    user_id: int = 1,
) -> None:
    """CLI entry point for generating transactions based on weekday patterns."""
    main(
        generate_receipts=generate_receipts,
        target_date=target_date,
        user_id=user_id,
    )
