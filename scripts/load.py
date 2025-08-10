import os

import pyodbc
from sqlalchemy import create_engine
import pandas as pd
from urllib.parse import quote_plus
from typing import Literal


def make_engine(server: str, database: str, username: str, password: str):
    # Ensure the driver exists on the system
    available_drivers = pyodbc.drivers()
    driver_env = os.getenv("AZURE_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
    if driver_env not in available_drivers:
        raise RuntimeError(f"ODBC driver '{driver_env}' not found. Installed: {available_drivers}")

    params = quote_plus(
        f"DRIVER={{{driver_env}}};"
        f"SERVER={server}.database.windows.net,1433;"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def write_weather(
        df: pd.DataFrame,
        engine,
        table: str = "WeatherDataClean",
        if_exists: Literal["fail", "replace", "append"] = "replace"
) -> None:
    df.to_sql(table, con=engine, if_exists=if_exists, index=False)
