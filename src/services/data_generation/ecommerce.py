# trunk-ignore-all(ruff/PLW0603,ruff/DTZ007)
from __future__ import annotations

import datetime
import logging
import os
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import uuid6
from dotenv import load_dotenv
from faker import Faker
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import declarative_base, sessionmaker

if TYPE_CHECKING:
    from collections.abc import Generator

PROJECT_PATH: str = "src/ecommerce/datagen/workspace"
DATABASE_NAME = "ecommerce"


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
)

load_dotenv()
DATABASE_USER = os.environ["DB_USER"]
DATABASE_PASSWORD = os.environ["DB_PASSWORD"]
DATABASE_HOST = os.environ["DB_HOST"]

url = URL.create(
    drivername="postgresql",
    username=DATABASE_USER,
    password=DATABASE_PASSWORD,
    host=DATABASE_HOST,
    port=5432,
    database=DATABASE_NAME,
)

engine = create_engine(
    url=url,
    pool_size=20,
    future=True,
)

SessionMaker = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    future=True,
)

f = Faker()
Faker.seed(0)
fake = Faker()

Base = declarative_base()


class UserSession(Base):
    __tablename__ = "sessions"
    id = Column(UUID, primary_key=True)
    created_at = Column(DateTime)
    session_duration = Column(Integer)
    user_id = Column(String)


class ProductPrice(Base):
    __tablename__ = "product_prices"
    id = Column(String, primary_key=True)
    price = Column(Float)
    created_at = Column(DateTime)
    product_id = Column(String)


class TransactionStatus(Enum):
    PENDING = "pending"
    CLEARED = "cleared"
    FAILED = "failed"


class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(String, primary_key=True)
    created_at = Column(DateTime)
    approved_at = Column(DateTime)
    transaction_status = Column(String)
    session_id = Column(String)
    user_id = Column(String)


class TransactionDetail(Base):
    __tablename__ = "transaction_details"
    id = Column(String, primary_key=True)
    quantity = Column(Integer)
    transaction_id = Column(String)
    product_id = Column(String)


# ----------------------------------------------------------------------------
def get_users_from_disk() -> pd.DataFrame:
    user_ids_path: Path = Path.cwd() / PROJECT_PATH / "users/users.csv"
    return pd.read_csv(
        filepath_or_buffer=user_ids_path,
        header=0,
        sep=",",
        dtype=str,  # Load "id" as string since UUIDs are strings
        index_col=None,
    )


def get_transactions_from_disk() -> pd.DataFrame:
    transaction_details_path: Path = (
        Path.cwd() / PROJECT_PATH / "transactions/transactions.csv"
    )
    return pd.read_csv(
        filepath_or_buffer=transaction_details_path,
        header=0,
        sep=",",
    )


def get_product_prices_from_disk() -> pd.DataFrame:
    product_items_path: Path = Path.cwd() / PROJECT_PATH / "products/product_prices.csv"
    return pd.read_csv(
        filepath_or_buffer=product_items_path,
        header=0,
        sep=",",
    )


# ----------------------------------------------------------------------------
def generate_sessions_from_missing_transactions(
    session_id_raw: str,
    transaction_created_at: datetime.datetime,
    user_id: str,
    random_number_generator_seed: int = 5,
) -> Generator:
    random_number_generator: np.random.Generator = np.random.default_rng(
        seed=random_number_generator_seed,
    )
    session_duration: int = int(
        np.ceil(
            np.abs(  # Ensure duration is positive
                random_number_generator.normal(
                    loc=600,  # Mean = 600 seconds
                    scale=42,  # Std Dev = 42 seconds
                    size=None,
                ),
            ),
        ).astype(int),
    )
    session_created_at: datetime.datetime = transaction_created_at - datetime.timedelta(
        seconds=session_duration,
    )
    yield UserSession(
        id=UUID(session_id_raw),
        created_at=session_created_at,
        session_duration=session_duration,
        user_id=user_id,
    )


# ----------------------------------------------------------------------------
def generate_user_session_new(
    user_id: str,
    random_number_generator_seed: int = 5,
) -> Generator:
    random_number_generator: np.random.Generator = np.random.default_rng(
        seed=random_number_generator_seed,
    )
    for _ in range(
        random_number_generator.integers(
            low=1,
            high=4,
        ),
    ):
        session_id: str = str(uuid6.uuid6())
        session_created_at: datetime.datetime = fake.date_time_between(
            start_date="-60d",
            end_date="now",
        )
        session_duration: int = int(
            np.ceil(
                np.abs(  # Ensure duration is positive
                    random_number_generator.normal(
                        loc=600,  # Mean = 600 seconds
                        scale=42,  # Std Dev = 42 seconds
                        size=None,
                    ),
                ),
            ).astype(int),
        )
        yield UserSession(
            id=session_id,
            created_at=session_created_at,
            session_duration=session_duration,
            user_id=user_id,
        )

        transaction_number_min: int = 1
        transaction_number_max: int = 4
        transaction_number = random_number_generator.integers(
            low=transaction_number_min,
            high=transaction_number_max,
        )
        transaction_number_threshold: int = 2
        if transaction_number > transaction_number_threshold:
            transaction_status: TransactionStatus = random_number_generator.choice(
                list(TransactionStatus),
                size=None,
            )
            transaction_created_at = session_created_at + datetime.timedelta(
                seconds=session_duration,
            )
            transaction_states_eligible_for_approved_at_field: list[
                TransactionStatus
            ] = [TransactionStatus.FAILED, TransactionStatus.CLEARED]
            yield Transaction(
                id=str(uuid6.uuid6()),
                created_at=transaction_created_at,
                approved_at=(
                    (
                        transaction_created_at
                        + datetime.timedelta(
                            seconds=random_number_generator.integers(
                                low=360,
                                high=86400,
                            ),
                        )
                    )
                    if transaction_status
                    in transaction_states_eligible_for_approved_at_field
                    else None
                ),
                transaction_status=transaction_status.value,
                session_id=session_id,
                user_id=user_id,
            )


def generate_transaction_details(
    transaction_id: str,
    product_ids: list[str],
    random_number_generator_seed: int = 5,
) -> Generator:
    random_number_generator: np.random.Generator = np.random.default_rng(
        seed=random_number_generator_seed,
    )
    for _ in range(
        random_number_generator.integers(
            low=1,
            high=4,
        ),
    ):
        product_id: str = random_number_generator.choice(
            product_ids,
            size=None,
        )
        yield TransactionDetail(
            id=str(uuid6.uuid6()),
            quantity=random_number_generator.integers(
                low=1,
                high=6,
            ),
            transaction_id=transaction_id,
            product_id=product_id,
        )


def generate_product_price(
    product_id: str,
    current_price: float,
    created_at: datetime.datetime,
    random_number_generator_seed: int = 5,
) -> Generator:
    random_number_generator: np.random.Generator = np.random.default_rng(
        seed=random_number_generator_seed,
    )
    for _ in range(
        random_number_generator.integers(
            low=3,
            high=20,
        ),
    ):
        time_delta = datetime.timedelta(
            days=random_number_generator.integers(
                low=57,
                high=83,
            ),
        )
        yield ProductPrice(
            id=str(uuid6.uuid6()),
            product_id=product_id,
            price=round(
                float(
                    random_number_generator.normal(
                        loc=current_price,
                        scale=current_price * 0.05,
                        size=None,
                    ),
                ),  # Convert to float
                2,  # Round to 2 decimal places
            ),
            created_at=created_at - time_delta,
        )


# ----------------------------------------------------------------------------
def add_generator(
    g: Generator,
    sql_session,  # trunk-ignore(ruff/ANN001)
) -> None:
    sql_session.add_all(list(g))
    sql_session.commit()


def make_tables(
    *ts,  # trunk-ignore(ruff/ANN002)
) -> None:
    for t in ts:
        t.__table__.drop(engine, checkfirst=True)
        t.__table__.create(engine, checkfirst=True)


if __name__ == "__main__":
    with SessionMaker() as sql_session:
        count: int = 0

        def missing_sessions_from_transactions() -> None:
            global count
            for _, row in get_transactions_from_disk().iterrows():
                add_generator(
                    generate_sessions_from_missing_transactions(
                        session_id_raw=row["session_id"],
                        transaction_created_at=datetime.datetime.strptime(
                            row["created_at"],
                            "%Y-%m-%d %H:%M:%S.%f",
                        ),
                        user_id=row["user_id"],
                    ),
                    sql_session,
                )
                print(
                    f"{count:06d}:  missing_sessions_from_transactions: {row['session_id']}",
                )
                count += 1

        def users() -> None:
            global count
            users = get_users_from_disk()["id"]
            mid_index = len(users) // 2  # Calculate halfway point
            for user_id in users[:mid_index]:  # Slice to iterate only the first half
                add_generator(generate_user_session_new(user_id=user_id), sql_session)
                print(f"{count:06d}:  user: {user_id}")
                count += 1

        def transaction_details() -> None:
            global count
            make_tables(TransactionDetail)
            product_ids = get_product_prices_from_disk()["id"]
            for transaction_id in reversed(get_transactions_from_disk()["id"]):
                add_generator(
                    generate_transaction_details(
                        transaction_id=transaction_id,
                        product_ids=product_ids,
                    ),
                    sql_session,
                )
                print(f"{count:06d}:  transaction_details: {transaction_id}")
                count += 1
            # if count == 10:

        def product_prices() -> None:
            global count
            product_prices_df = get_product_prices_from_disk()
            print(product_prices_df)
            # Print the type of each column
            for column in product_prices_df.columns:
                print(f"Column '{column}' has type: {product_prices_df[column].dtype}")

            for _index, row in product_prices_df.iterrows():
                product_id = row["product_id"]
                current_price = row["price"]
                created_at = row["created_at"]
                add_generator(
                    generate_product_price(
                        product_id=product_id,
                        current_price=current_price,
                        created_at=datetime.datetime.strptime(
                            created_at,
                            "%Y-%m-%d %H:%M:%S.%f",
                        ),
                    ),
                    sql_session,
                )
                print(f"{count:06d}:  product_prices: {product_id}")
                count += 1

        missing_sessions_from_transactions()
