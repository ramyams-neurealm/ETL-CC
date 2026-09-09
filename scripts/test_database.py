"""
Tests connectivity to the PostgreSQL metadata database.

This script executes a simple query and confirms the configured database
and schema without printing credentials.
"""

import asyncio

from sqlalchemy import text

from etl_cc.config import settings
from etl_cc.database import engine


async def test_database() -> None:
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT
                        current_database() AS database_name,
                        current_user AS database_user,
                        :schema_name AS configured_schema
                    """
                ),
                {
                    "schema_name": settings.pg_schema,
                },
            )

            row = result.mappings().one()

            schema_exists = await connection.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.schemata
                        WHERE schema_name = :schema_name
                    )
                    """
                ),
                {
                    "schema_name": settings.pg_schema,
                },
            )

            print("PostgreSQL connection successful")
            print(f"Database: {row['database_name']}")
            print(f"Database user: {row['database_user']}")
            print(f"Configured schema: {row['configured_schema']}")
            print(f"Schema exists: {schema_exists.scalar_one()}")

    except Exception as exc:
        print("PostgreSQL connection failed")
        print(f"Error type: {type(exc).__name__}")
        print(f"Reason: {exc}")
        raise

    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(test_database())