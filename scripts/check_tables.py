"""
Checks the ETL metadata tables in the configured PostgreSQL schema.

This script lists the existing ETL tables and identifies which of the
seven required Release 1 tables are present or missing.
"""

import asyncio

from sqlalchemy import text

from etl_cc.config import settings
from etl_cc.database import engine


REQUIRED_TABLES = {
    "repository_etl",
    "etl_object_etl",
    "workflow_run_etl",
    "agent_response_etl",
    "workflow_event_etl",
    "generated_artifact_etl",
    "knowledge_base_etl",
}


async def check_tables() -> None:
    """
    Checks whether all seven required ETL tables exist.

    The function reads PostgreSQL information_schema and compares the
    tables in the configured schema with the ETL CC table list.
    """

    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = :schema_name
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """
                ),
                {
                    "schema_name": settings.pg_schema,
                },
            )

            all_schema_tables = {
                row["table_name"]
                for row in result.mappings()
            }

            existing_etl_tables = (
                all_schema_tables & REQUIRED_TABLES
            )

            missing_etl_tables = (
                REQUIRED_TABLES - all_schema_tables
            )

            unrelated_tables = (
                all_schema_tables - REQUIRED_TABLES
            )

            print("=" * 60)
            print("ETL CC Metadata Table Check")
            print("=" * 60)

            print(
                f"Configured database : {settings.pg_db}"
            )

            print(
                f"Configured schema   : {settings.pg_schema}"
            )

            print(
                f"Required ETL tables : {len(REQUIRED_TABLES)}"
            )

            print(
                f"Existing ETL tables : {len(existing_etl_tables)}"
            )

            print(
                f"Missing ETL tables  : {len(missing_etl_tables)}"
            )

            if existing_etl_tables:
                print("\nExisting ETL tables:")

                for table_name in sorted(
                    existing_etl_tables
                ):
                    print(
                        f"  - {settings.pg_schema}.{table_name}"
                    )

            if missing_etl_tables:
                print("\nMissing ETL tables:")

                for table_name in sorted(
                    missing_etl_tables
                ):
                    print(
                        f"  - {settings.pg_schema}.{table_name}"
                    )

            if unrelated_tables:
                print(
                    "\nOther tables available in the schema:"
                )

                for table_name in sorted(
                    unrelated_tables
                ):
                    print(
                        f"  - {settings.pg_schema}.{table_name}"
                    )

            if not missing_etl_tables:
                print(
                    "\nResult: All seven ETL metadata tables exist."
                )
            elif not existing_etl_tables:
                print(
                    "\nResult: None of the required ETL metadata "
                    "tables exist."
                )
            else:
                print(
                    "\nResult: Some ETL metadata tables are missing."
                )

            print("=" * 60)

    except Exception as exc:
        print("=" * 60)
        print("Unable to check ETL metadata tables")
        print("=" * 60)
        print(
            f"Database : {settings.pg_db}"
        )
        print(
            f"Schema   : {settings.pg_schema}"
        )
        print(
            f"Error type: {type(exc).__name__}"
        )
        print(
            f"Reason: {exc}"
        )
        print("=" * 60)

        raise

    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(check_tables())