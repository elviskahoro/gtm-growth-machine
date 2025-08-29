from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import DeclarativeMeta, Session

import anyio
import pandas as pd
from anyio import Semaphore
from dotenv import load_dotenv
from openai import AsyncOpenAI
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import declarative_base, sessionmaker

logger: logging.Logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
)

load_dotenv()
DATABASE_USER: str = os.environ.get("DB_USER", "postgres")
DATABASE_PASSWORD: str = os.environ.get("DB_PASSWORD", "password")
DATABASE_HOST: str = os.environ.get("DB_HOST", "localhost")
DATABASE_NAME: str = os.environ.get("DB_NAME", "testdb")
OPENAI_API_TOKEN: str = os.environ.get("OPENAI_API_TOKEN", "")

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

client: AsyncOpenAI = AsyncOpenAI(api_key=OPENAI_API_TOKEN)

Base: DeclarativeMeta = declarative_base()


class UserProfile(Base):
    __tablename__ = "usr_profiles"

    id = Column(Integer, primary_key=True, index=True)
    bio = Column(String, nullable=False)


def get_users_from_disk() -> pd.DataFrame:
    path: Path = Path.cwd() / "data/fraud/usrs.csv"
    return pd.read_csv(
        filepath_or_buffer=path,
        header=0,
        on_bad_lines="error",
    )


def calculate_age(birth_date: str) -> int:
    birth_date_parsed: datetime = datetime.strptime(birth_date, "%Y-%m-%d").replace(
        tzinfo=timezone.utc,
    )
    today: datetime = datetime.now(timezone.utc)
    age: int = today.year - birth_date_parsed.year
    if today < birth_date_parsed.replace(year=today.year):
        age -= 1
    return age


async def generate_bio_for_user(name: str, age: int) -> str:
    try:
        response: Any = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a profile bio generator. Given a person's name and age, create a realistic and engaging user bio that reflects their life stage and interests. Keep it concise (2-3 sentences) and authentic.",
                },
                {
                    "role": "user",
                    "content": f"Generate a user profile bio for {name}, who is {age} years old.",
                },
            ],
            max_tokens=200,
            temperature=0.8,
        )
        return response.choices[0].message.content.strip()

    except Exception:
        logger.exception("Failed to generate bio for user '%s'", name)
        return f"Hi, I'm {name}. I enjoy exploring new experiences and connecting with others."


async def create_user_profile(row: pd.Series) -> UserProfile:
    age: int = calculate_age(row["dob"])
    bio: str = await generate_bio_for_user(row["name"], age)

    return UserProfile(
        id=int(row["id"]),
        bio=bio,
    )


async def process_users_and_generate_profiles(sql_session: Session) -> None:
    users: pd.DataFrame = get_users_from_disk()
    logger.info("Processing %d users...", len(users))

    semaphore: Semaphore = Semaphore(10)  # Limit concurrent OpenAI API calls

    async def process_single_user(row: pd.Series) -> UserProfile | None:
        async with semaphore:
            logger.info("Processing user %s: %s", row["id"], row["name"])
            try:
                return await create_user_profile(row)
            except Exception:
                logger.exception("Failed to process user %s", row["id"])
                return None

    # Process all users concurrently
    tasks: list[Any] = []
    for _index, row in users.iterrows():
        tasks.append(process_single_user(row))

    results: list[Any] = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out None results and exceptions
    profiles: list[UserProfile] = [
        profile for profile in results if isinstance(profile, UserProfile)
    ]

    logger.info("Saving %d user profiles to database...", len(profiles))
    sql_session.add_all(profiles)
    sql_session.commit()
    logger.info("Successfully saved %d user profiles!", len(profiles))


def replace_database_tables(
    *ts: DeclarativeMeta,
) -> None:
    for t in ts:
        t.__table__.drop(engine, checkfirst=True)
        t.__table__.create(engine, checkfirst=True)


async def main() -> None:
    with SessionMaker() as sql_session:
        replace_database_tables(UserProfile)
        await process_users_and_generate_profiles(sql_session)


if __name__ == "__main__":
    anyio.run(main)
