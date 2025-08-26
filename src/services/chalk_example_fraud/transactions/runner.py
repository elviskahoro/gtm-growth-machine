from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import modal
import polars as pl
from modal import Image
from openai import AsyncOpenAI
from sqlalchemy import Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from src.services.chalk_example_fraud.transactions.fake_data_generator import (
    generate_daily_receipt_dicts,
    generate_daily_transaction_dicts,
    generate_receipts_from_transaction_rows,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session

NROWS: int | None = None
SKIPROWS: int | None = None
MAX_RECEIPTS_TO_GENERATE: int = (
    2000  # Global variable to control how many receipts to generate
)
BATCH_SIZE: int = 10  # Number of receipts to commit in each batch
MIN_DAILY_TRANSACTIONS: int = 2  # Minimum daily transactions
MAX_DAILY_TRANSACTIONS: int = 11  # Maximum daily transactions

# Modal configuration
image: Image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
    "polars",
    "sqlalchemy",
    "psycopg2-binary",
    "openai",
    "anyio",
    "python-dotenv",
)
image = image.add_local_python_source(
    *[
        "src",
    ],
)
app = modal.App(
    name=__name__.replace(".", "-"),
    image=image,
)


class Base(DeclarativeBase):

    @staticmethod
    def create_database_connection() -> tuple[Engine, sessionmaker[Session]]:
        """Create database connection using Modal secrets."""
        import os  # trunk-ignore(ruff/PLC0415)

        database_user: str = os.environ["DB_USER"]
        database_password: str = os.environ["DB_PASSWORD"]
        database_host: str = os.environ["DB_HOST"]
        database_name: str = os.environ["DB_NAME"]

        url: URL = URL.create(
            drivername="postgresql",
            username=database_user,
            password=database_password,
            host=database_host,
            port=5432,
            database=database_name,
        )
        engine: Engine = create_engine(
            url=url,
            pool_size=20,
            future=True,
        )
        session_maker: sessionmaker[Session] = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine,
            future=True,
        )
        return engine, session_maker


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    at = Column(DateTime(timezone=True), nullable=False)
    memo = Column(String(500), nullable=False)
    amount = Column(Integer, nullable=False)  # Store as cents

    @classmethod
    def dict_to_transaction(cls, transaction_dict: dict[str, Any]) -> Transaction:
        """Convert a transaction dictionary to a Transaction model object."""
        return cls(
            id=transaction_dict["id"],
            user_id=transaction_dict["user_id"],
            at=transaction_dict["at"],
            memo=transaction_dict["memo"],
            amount=transaction_dict["amount"],
        )

    @staticmethod
    def get_transactions_from_disk(
        input_path: str,
        nrows: int | None,
        skiprows: int | None,
    ) -> pl.DataFrame:
        path: Path = Path.cwd() / input_path
        skip_rows: int = skiprows or 0
        return pl.read_csv(
            source=path,
            has_header=True,
            skip_rows=skip_rows,
            n_rows=nrows,
        )


class TransactionReceipt(Base):
    __tablename__ = "transaction_receipts"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    at = Column(DateTime(timezone=True), nullable=False)
    body = Column(Text, nullable=False)

    @classmethod
    def dict_to_transaction_receipt(
        cls,
        receipt_dict: dict[str, Any],
    ) -> TransactionReceipt:
        """Convert a receipt dictionary to a TransactionReceipt model object."""
        return cls(
            id=receipt_dict["id"],
            user_id=receipt_dict["user_id"],
            at=receipt_dict["at"],
            body=receipt_dict["body"],
        )


async def generate_daily_transactions(
    sql_session: Session,
    target_date: datetime | None = None,
    user_id: int = 1,
) -> AsyncGenerator[Transaction, None]:
    """Generate realistic daily transactions for a given date."""
    # Get the current max transaction ID from database
    try:
        max_id_result: tuple[int] | None = (
            sql_session.query(Transaction.id).order_by(Transaction.id.desc()).first()
        )
        next_id: int = (max_id_result[0] + 1) if max_id_result else 1

    except (OSError, ValueError, TypeError):
        # If table doesn't exist or is empty, start from 1
        next_id = 1

    # Generate transaction dictionaries
    transaction_dicts: list[dict[str, Any]] = list(
        generate_daily_transaction_dicts(
            target_date=target_date,
            user_id=user_id,
            starting_id=next_id,
            min_daily_transactions=MIN_DAILY_TRANSACTIONS,
            max_daily_transactions=MAX_DAILY_TRANSACTIONS,
        ),
    )
    for td in transaction_dicts:
        transaction: Transaction = Transaction.dict_to_transaction(td)
        yield transaction

    print(f"Daily transaction generation complete!")


async def generate_daily_transactions_with_receipts(
    sql_session: Session,
    client: AsyncOpenAI,
    target_date: datetime | None = None,
    user_id: int = 1,
) -> AsyncGenerator[Transaction | TransactionReceipt, None]:
    """Generate realistic daily transactions and their receipts for a given date."""
    # Get the current max transaction ID from database
    try:
        max_id_result: tuple[int] | None = (
            sql_session.query(Transaction.id).order_by(Transaction.id.desc()).first()
        )
        next_id: int = (max_id_result[0] + 1) if max_id_result else 1

    except (OSError, ValueError, TypeError):
        # If table doesn't exist or is empty, start from 1
        next_id = 1

    # Generate transaction dictionaries
    transaction_dicts: list[dict[str, Any]] = list(
        generate_daily_transaction_dicts(
            target_date=target_date,
            user_id=user_id,
            starting_id=next_id,
            min_daily_transactions=MIN_DAILY_TRANSACTIONS,
            max_daily_transactions=MAX_DAILY_TRANSACTIONS,
        ),
    )

    # Yield transaction objects
    for td in transaction_dicts:
        transaction: Transaction = Transaction.dict_to_transaction(td)
        yield transaction

    # Generate and yield receipts
    print(f"Generating receipts for {len(transaction_dicts)} transactions...")
    async for receipt_dict in generate_daily_receipt_dicts(
        transaction_dicts,
        client,
    ):
        receipt: TransactionReceipt = TransactionReceipt.dict_to_transaction_receipt(
            receipt_dict,
        )
        yield receipt

    print(f"Daily data generation complete!")


async def generate_receipts_for_transactions(
    input_path: str,
    client: AsyncOpenAI,
) -> AsyncGenerator[TransactionReceipt, None]:
    """Process transactions from CSV and generate receipts using OpenAI with parallel processing."""
    transactions: pl.DataFrame = Transaction.get_transactions_from_disk(
        input_path=input_path,
        nrows=NROWS,
        skiprows=SKIPROWS,
    )
    transaction_rows: list[dict[str, Any]] = list(transactions.iter_rows(named=True))
    print(
        f"Processing up to {MAX_RECEIPTS_TO_GENERATE} receipts from {len(transaction_rows)} transactions...",
    )
    async for receipt_dict in generate_receipts_from_transaction_rows(
        transaction_rows,
        client,
        MAX_RECEIPTS_TO_GENERATE,
    ):
        receipt: TransactionReceipt = TransactionReceipt.dict_to_transaction_receipt(
            receipt_dict,
        )
        yield receipt

    print("Receipt generation complete!")


class TableOperation(Enum):
    REPLACE = "REPLACE"
    APPEND = "APPEND"


@app.local_entrypoint()
def local(  # trunk-ignore(ruff/PLR0915)
    input_path_transactions: str | None = None,
    replace_or_append: str = "APPEND",
    *,
    generate_receipts: bool = False,
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
        replace_or_append_to_table: TableOperation,
    ) -> None:
        for t in ts:
            match replace_or_append_to_table:
                case TableOperation.REPLACE:
                    print("Dropping existing database table")
                    t.__table__.drop(engine, checkfirst=True)

                case TableOperation.APPEND:
                    pass

                case _:
                    msg = "Invalid table operation"
                    raise ValueError(msg)

            print(f"Creating or connecting to database table: {t.__tablename__}")
            t.__table__.create(engine, checkfirst=True)

    match (input_path_transactions, target_date):
        case (None, None):
            print(
                "Neither input_path_transactions or target_date was provided.\n"
                "Generating data with today's date.",
            )
            target_date = datetime.now(tz=timezone.utc).date().isoformat()

        case (str(), str()):
            print(
                "Both input_path_transactions and target_date were provided.\n"
                "Terminating to avoid ambiguity.",
            )
            return

        case (str(), None) | (None, str()):
            print("Parameters validated, proceeding with data generation.")

        case _:
            error_msg: str = "Invalid parameters provided."
            raise ValueError(error_msg)

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
            if input_path_transactions:
                async for obj in generate_receipts_for_transactions(
                    input_path=input_path_transactions,
                    client=client,
                ):
                    all_objects.append(obj)

                return all_objects

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

            msg = "Either input_path_transactions or target_date must be provided."
            raise ValueError(msg)

        connect_and_create_tables(
            engine,
            Transaction,
            TransactionReceipt,
            replace_or_append_to_table=TableOperation(replace_or_append),
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
