import os
import pyodbc
from sqlalchemy import create_engine, inspect, text
import pandas as pd
from urllib.parse import quote_plus
from typing import Literal


def make_engine(server: str, database: str, username: str, password: str):
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    driver = "ODBC Driver 18 for SQL Server" if any("18" in d for d in drivers) else "ODBC Driver 17 for SQL Server"

    # URL-escape user & pass
    user_esc = quote_plus(username)
    pass_esc = quote_plus(password)
    drv_esc = driver.replace(" ", "+")  # URL form

    # URL-style connection string
    url = (
        f"mssql+pyodbc://{user_esc}:{pass_esc}@{server}:1433/{database}"
        f"?driver={drv_esc}&Encrypt=yes&TrustServerCertificate=no&ConnectionTimeout=30"
    )

    eng = create_engine(url, pool_pre_ping=True, pool_recycle=1800)

    # sanity check
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))

    return eng


def write_weather(
    df: pd.DataFrame,
    engine,
    table: str = "WeatherDataClean",
    schema: str = "dbo",
) -> None:
    insp = inspect(engine)

    if insp.has_table(table, schema=schema):
        # Get all existing city and timestamp combos from database
        with engine.connect() as conn:
            existing_pairs = pd.DataFrame(conn.execute(
                text(f"SELECT city, [timestamp] FROM {schema}.{table}")
            ).fetchall(), columns=["city", "timestamp"])

        #  timestamp types matching
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False, errors="coerce")
        existing_pairs["timestamp"] = pd.to_datetime(existing_pairs["timestamp"], utc=False, errors="coerce")

        # Keeping only rows not already present
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