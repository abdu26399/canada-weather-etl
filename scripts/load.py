import os
import pyodbc
from sqlalchemy import create_engine, inspect, text
import pandas as pd
from urllib.parse import quote_plus
from typing import Literal


def make_engine(server: str, database: str, username: str, password: str):
    import os, pyodbc
    from urllib.parse import quote_plus

    # Ensure ODBC driver is present
    available = pyodbc.drivers()
    driver = os.getenv("AZURE_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
    if driver not in available:
        raise RuntimeError(f"ODBC driver '{driver}' not found. Installed: {available}")

    server = server.strip()
    host = server if server.lower().endswith(".database.windows.net") else f"{server}.database.windows.net"

    params = quote_plus(
        f"DRIVER={{{driver}}};"
        f"SERVER={host},1433;"  # Removed tcp:
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=60;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def write_weather(
        df: pd.DataFrame,
        engine,
        table: str = "WeatherDataClean",
        schema: str = "dbo",
        if_exists: Literal["fail", "replace", "append"] = "append",
) -> None:
    insp = inspect(engine)

    # Cast a few columns, so they line up with your existing schema
    # (humidity, wind_deg are BIGINT; timestamp/source_time are datetime)
    if "humidity" in df.columns:
        df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce").astype("Int64")
    if "wind_deg" in df.columns:
        df["wind_deg"] = pd.to_numeric(df["wind_deg"], errors="coerce").astype("Int64")
    for col in ("timestamp", "source_time"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=False, errors="coerce")

    # Keep column order consistent with the existing table (optional but nice)
    if insp.has_table(table, schema=schema):
        with engine.connect() as conn:
            # noinspection SqlNoDataSourceInspection
            cols = conn.execute(text(f"""
                SELECT c.name
                FROM sys.columns c
                JOIN sys.objects o ON o.object_id = c.object_id
                JOIN sys.schemas s ON s.schema_id = o.schema_id
                WHERE s.name = :schema AND o.name = :table
                ORDER BY c.column_id
            """), {"schema": schema, "table": table}).scalars().all()
        df = df[[c for c in cols if c in df.columns]]

        # TRUNCATE + APPEND (no schema change)
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {schema}.{table}"))
        df.to_sql(table, con=engine, schema=schema, if_exists="append", index=False)
    else:
        # First-ever run: create the table
        df.to_sql(table, con=engine, schema=schema, if_exists="fail", index=False)
