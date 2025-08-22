from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session

import pandas as pd
from anyio import Semaphore
from dotenv import load_dotenv
from openai import AsyncOpenAI
from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import declarative_base, sessionmaker

NROWS: int | None = None
SKIPROWS: int | None = None
MAX_RECEIPTS_TO_GENERATE: int = (
    10000  # Global variable to control how many receipts to generate
)
BATCH_SIZE: int = 10  # Number of receipts to commit in each batch

logger: logging.Logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
)

load_dotenv()
DATABASE_USER: str = os.environ["DB_USER"]
DATABASE_PASSWORD: str = os.environ["DB_PASSWORD"]
DATABASE_HOST: str = os.environ["DB_HOST"]
DATABASE_NAME: str = os.environ["DB_NAME"]
OPENAI_API_TOKEN: str = os.environ["OPENAI_API_TOKEN"]

url: URL = URL.create(
    drivername="postgresql",
    username=DATABASE_USER,
    password=DATABASE_PASSWORD,
    host=DATABASE_HOST,
    port=5432,
    database=DATABASE_NAME,
)

engine: Engine = create_engine(
    url=url,
    pool_size=20,
    future=True,
)

SessionMaker: sessionmaker[Session] = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    future=True,
)

# Initialize OpenAI client
client: AsyncOpenAI = AsyncOpenAI(api_key=OPENAI_API_TOKEN)

Base: Any = declarative_base()


class TransactionReceipt(Base):
    __tablename__ = "txn_receipts"

    id = Column(Integer, primary_key=True, index=True)
    at = Column(DateTime, nullable=False)
    user_id = Column(Integer, nullable=False)
    body = Column(String, nullable=False)


# ----------------------------------------------------------------------------
def get_transactions_from_disk(
    nrows: int | None,
    skiprows: int | None,
) -> pd.DataFrame:
    path: Path = Path.cwd() / "data/fraud/txns.csv"
    skiprows_array: list[int] | None = None
    if skiprows:
        # noinspection PyTypeChecker
        skiprows_array = list(
            range(
                1,
                skiprows,
            ),
        )

    return pd.read_csv(
        filepath_or_buffer=path,
        header=0,
        on_bad_lines="error",
        nrows=nrows,
        skiprows=skiprows_array,
    )


async def generate_receipt_body_from_memo(memo: str) -> str:
    """Generate a transaction receipt body using OpenAI based on the transaction memo."""
    try:
        response: Any = await client.chat.completions.create(
            model="gpt-4.1-nano",
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

    except Exception:
        logger.exception("Failed to generate receipt body for memo '%s'", memo)
        return f"Transaction: {memo}"


# ----------------------------------------------------------------------------
async def create_transaction_receipt(row: pd.Series) -> TransactionReceipt:
    """Create a TransactionReceipt from a transaction row using OpenAI to generate the body.
    Uses the same transaction ID to match the original transaction.
    """
    # Parse datetime from CSV
    transaction_at: Any = pd.to_datetime(row["at"])

    # Generate receipt body using OpenAI based on the memo
    receipt_body: str = await generate_receipt_body_from_memo(row["memo"])
    return TransactionReceipt(
        id=int(row["id"]),  # Use same ID as the original transaction
        at=transaction_at,
        user_id=int(row["user_id"]),
        body=receipt_body,
    )


async def commit_receipt_batch(
    sql_session: Session,
    receipts: list[TransactionReceipt],
) -> None:
    """Commit a batch of receipts to the database."""
    if not receipts:
        return

    logger.info("Saving %d receipts to database...", len(receipts))
    sql_session.add_all(receipts)
    sql_session.commit()
    logger.info("Successfully saved %d receipts!", len(receipts))


# ----------------------------------------------------------------------------
async def process_single_transaction(
    row: pd.Series,
    semaphore: Semaphore,
) -> TransactionReceipt | None:
    """Process a single transaction with semaphore limiting concurrency."""
    async with semaphore:
        logger.info("Processing transaction %s: %s", row["id"], row["memo"])
        try:
            return await create_transaction_receipt(row)
        except Exception:
            logger.exception("Failed to process transaction %s", row["id"])
            return None


async def process_transactions_and_generate_receipts(sql_session: Session) -> None:
    """Process transactions from CSV and generate receipts using OpenAI with parallel processing."""
    transactions: pd.DataFrame = get_transactions_from_disk(
        nrows=NROWS,
        skiprows=SKIPROWS,
    )
    logger.info(
        "Processing up to %d receipts from %d transactions...",
        MAX_RECEIPTS_TO_GENERATE,
        len(transactions),
    )

    # Create semaphore to limit to 5 parallel workers
    semaphore: Semaphore = Semaphore(5)

    receipts_batch: list[TransactionReceipt] = []
    tasks: list[Any] = []
    processed_count: int = 0

    for processed_count, (_index, row) in enumerate(transactions.iterrows()):
        if processed_count >= MAX_RECEIPTS_TO_GENERATE:
            logger.info(
                "Reached maximum limit of %d receipts, stopping processing",
                MAX_RECEIPTS_TO_GENERATE,
            )
            break

        # Create task for this transaction
        task: Any = process_single_transaction(row, semaphore)
        tasks.append(task)

    # Process all tasks and collect results
    logger.info("Starting parallel processing of %d transactions...", len(tasks))
    results: list[Any] = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter successful receipts and batch commit every 10
    for receipt in results:
        if isinstance(receipt, TransactionReceipt):
            receipts_batch.append(receipt)

            # Commit when batch reaches configured size
            if len(receipts_batch) >= BATCH_SIZE:
                await commit_receipt_batch(sql_session, receipts_batch)
                receipts_batch = []

    # Commit any remaining receipts
    if receipts_batch:
        await commit_receipt_batch(sql_session, receipts_batch)

    total_successful: int = sum(1 for r in results if isinstance(r, TransactionReceipt))
    logger.info("Successfully processed %d receipts total!", total_successful)


def replace_database_table(
    *ts,  # trunk-ignore(ruff/ANN002)
) -> None:
    for t in ts:
        t.__table__.drop(engine, checkfirst=True)
        t.__table__.create(engine, checkfirst=True)


async def main() -> None:
    with SessionMaker() as sql_session:
        # Create tables if they don't exist
        replace_database_table(TransactionReceipt)

        # Process transactions and generate receipts
        await process_transactions_and_generate_receipts(sql_session)


if __name__ == "__main__":
    asyncio.run(main())
