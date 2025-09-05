from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import DeclarativeBase, sessionmaker

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session


class Base(DeclarativeBase):

    @staticmethod
    def create_database_connection() -> tuple[Engine, sessionmaker[Session]]:
        """Create database connection using Modal secrets."""
        import os  # trunk-ignore(ruff/PLC0415)

        from sqlalchemy import create_engine  # trunk-ignore(ruff/PLC0415)

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
    __tablename__ = "txns"

    id = Column(Integer, primary_key=True, index=True)
    amount = Column(Integer, nullable=False)  # Store as cents
    at = Column(DateTime(timezone=True), nullable=False)
    description = Column(String(500), nullable=False)
    user_id = Column(Integer, nullable=False, index=True)
    payer_id = Column(Integer, nullable=True, index=True)
    payee_id = Column(Integer, nullable=True, index=True)

    @classmethod
    def dict_to_transaction(cls, transaction_dict: dict[str, Any]) -> Transaction:
        """Convert a transaction dictionary to a Transaction model object."""
        return cls(
            id=transaction_dict["id"],
            amount=transaction_dict["amount"],
            at=transaction_dict["at"],
            description=transaction_dict["description"],
            user_id=transaction_dict["user_id"],
            payer_id=transaction_dict.get("payer_id"),
            payee_id=transaction_dict.get("payee_id"),
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
