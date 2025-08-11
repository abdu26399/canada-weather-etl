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

    if insp.has_table(table, schema=schema):
        # Get all existing city+timestamp combos from DB
        with engine.connect() as conn:
            existing_pairs = pd.DataFrame(conn.execute(
                text(f"SELECT city, [timestamp] FROM {schema}.{table}")
            ).fetchall(), columns=["city", "timestamp"])

        # Ensure timestamp types match
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False, errors="coerce")
        existing_pairs["timestamp"] = pd.to_datetime(existing_pairs["timestamp"], utc=False, errors="coerce")

        # Keep only rows not already present
        merged = df.merge(existing_pairs, on=["city", "timestamp"], how="left", indicator=True)
        new_data = merged[merged["_merge"] == "left_only"].drop(columns="_merge")

        if new_data.empty:
            print("✅ No new city/timestamp data to insert.")
            return

        print(f"Appending {len(new_data)} new rows...")
        new_data.to_sql(table, con=engine, schema=schema, if_exists="append", index=False)

    else:
        print("Table does not exist — creating it with all data.")
        df.to_sql(table, con=engine, schema=schema, if_exists="fail", index=False)