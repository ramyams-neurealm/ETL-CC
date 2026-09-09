"""
Creates the ETL CC metadata tables in the configured PostgreSQL schema.

This script creates only tables defined by the ETL CC SQLAlchemy metadata.
Existing tables are not dropped, replaced, or modified.
"""

import asyncio
import re

from sqlalchemy import text

from etl_cc import models  # noqa: F401
from etl_cc.config import settings
from etl_cc.database import Base, engine


def validate_schema_name(schema_name: str) -> str:
    """
    Validates the PostgreSQL schema name before using it in SQL.

    Schema names cannot be passed as ordinary SQL parameters, so this
    validation prevents unsafe or invalid identifiers.
    """

    schema_pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"

    if not re.fullmatch(schema_pattern, schema_name):
        raise ValueError(
            f"Invalid PostgreSQL schema name: {schema_name}"
        )

    return schema_name


async def create_tables() -> None:
    """
    Creates the configured schema and the seven ETL CC tables.

    SQLAlchemy create_all does not drop existing tables and does not
    replace unrelated objects already available in the schema.
    """

    schema_name = validate_schema_name(
        settings.pg_schema
    )

    try:
        async with engine.begin() as connection:
            schema_exists_result = await connection.execute(
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
                    "schema_name": schema_name,
                },
            )

            schema_exists = (
                schema_exists_result.scalar_one()
            )

            if not schema_exists:
                await connection.execute(
                    text(
                        f'CREATE SCHEMA "{schema_name}"'
                    )
                )

                print(
                    f"Created PostgreSQL schema: "
                    f"{schema_name}"
                )
            else:
                print(
                    f"PostgreSQL schema already exists: "
                    f"{schema_name}"
                )

            await connection.run_sync(
                Base.metadata.create_all
            )

        print()
        print("ETL CC table creation completed")
        print(f"Database: {settings.pg_db}")
        print(f"Schema: {schema_name}")
        print()
        print("Expected ETL CC tables:")
        print(f"  - {schema_name}.repository_etl")
        print(f"  - {schema_name}.etl_object_etl")
        print(f"  - {schema_name}.workflow_run_etl")
        print(f"  - {schema_name}.agent_response_etl")
        print(f"  - {schema_name}.workflow_event_etl")
        print(f"  - {schema_name}.generated_artifact_etl")
        print(f"  - {schema_name}.knowledge_base_etl")

    except Exception as exc:
        print()
        print("ETL CC table creation failed")
        print(f"Database: {settings.pg_db}")
        print(f"Schema: {schema_name}")
        print(f"Error type: {type(exc).__name__}")
        print(f"Reason: {exc}")

        raise

    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(create_tables())