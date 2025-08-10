import os
import pyodbc
from sqlalchemy import create_engine
import pandas as pd
from urllib.parse import quote_plus
from typing import Literal


def make_engine(server: str, database: str, username: str, password: str):
    # Ensure the ODBC driver is actually installed
    available_drivers = pyodbc.drivers()
    driver = os.getenv("AZURE_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
    if driver not in available_drivers:
        raise RuntimeError(f"ODBC driver '{driver}' not found. Installed: {available_drivers}")

    # Normalize server: accept either logical name or full FQDN
    server = server.strip()
    if server.lower().endswith(".database.windows.net"):
        host = server
    else:
        host = f"{server}.database.windows.net"

    # Build DSN (note the tcp: prefix)
    params = quote_plus(
        f"DRIVER={{{driver}}};"
        f"SERVER=tcp:{host},1433;"
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
